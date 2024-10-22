"""
Module for classes that upload single files to the database.
"""

import os
from subprocess import STDOUT, check_output
from pathlib import Path
import pandas as pd
from geoalchemy2.elements import RasterElement, WKTElement
from os.path import basename, exists, join
from os import makedirs, remove
import boto3
import logging
from timezonefinder import TimezoneFinder
from snowexsql.db import get_table_attributes
from snowexsql.tables import LayerData
import datetime

from ..interpretation import add_date_time_keys, standardize_depth
from ..metadata import DataHeader
from ..string_management import parse_none, remap_data_names
from ..utilities import (assign_default_kwargs, get_file_creation_date,
                        get_logger)
from ..projection import reproject_point_in_dict
from snowexsql.tables import Instrument, Campaign, Observer, DOI, MeasurementType, Site
from .base import BaseUpload


LOG = logging.getLogger("snowex_db.upload.layers")


class DataValidationError(ValueError):
    pass


class UploadProfileData(BaseUpload):
    """
    Class for submitting a single profile. Since layers are uploaded layer by layer this allows for submitting them
    one file at a time.
    """
    expected_attributes = [c for c in dir(LayerData) if c[0] != '_']

    def __init__(self, profile_filename, **kwargs):
        self.log = get_logger(__name__)

        self.filename = profile_filename

        # Read in the file header
        self.hdr = DataHeader(profile_filename, **kwargs)

        # Transfer a couple attributes for brevity
        for att in ['data_names', 'multi_sample_profiles']:
            setattr(self, att, getattr(self.hdr, att))

        # Read in data
        self.df = self._read(profile_filename)

        # Use the files creation date as the date accessed for NSIDC citation
        self.date_accessed = get_file_creation_date(self.filename)

    def _handle_force(self, df, profile_filename):
        if 'force' in df.columns:
            # Convert depth from mm to cm
            df['depth'] = df['depth'].div(10)
            is_smp = True
            # Make the data negative from snow surface
            depth_fmt = 'surface_datum'

            # SMP serial number and original filename for provenance to the comment
            f = basename(profile_filename)
            serial_no = f.split('SMP_')[-1][1:3]

            df['comments'] = f"fname = {f}, " \
                             f"serial no. = {serial_no}"

        return df

    def _handle_flags(self, df):

        if "flags" in df.columns:
            # Max length of the flags column
            max_len = LayerData.flags.type.length
            df["flags"] = df["flags"].str.replace(" ", "")
            str_len = df["flags"].str.len()
            if any(str_len > max_len):
                raise DataValidationError(
                    f"Flag column is too long"
                )
        return df

    def _read(self, profile_filename):
        """
        Read in a profile file. Managing the number of lines to skip and
        adjusting column names

        Args:
            profile_filename: Filename containing the a manually measured
                             profile
        Returns:
            df: pd.dataframe contain csv data with standardized column names
        """
        # header=0 because docs say to if using skip rows and columns
        try:
            df = pd.read_csv(
                profile_filename, header=0, skiprows=self.hdr.header_pos,
                names=self.hdr.columns, encoding='latin'
            )
        except pd.errors.ParserError as e:
            LOG.error(e)
            raise RuntimeError(f"Failed reading {profile_filename}")

        # Special SMP specific tasks
        depth_fmt = 'snow_height'
        is_smp = False

        if 'force' in df.columns:
            df = self._handle_force(df, profile_filename)
            is_smp = True
            # Make the data negative from snow surface
            depth_fmt = 'surface_datum'

        if not df.empty:
            # Standardize all depth data
            new_depth = standardize_depth(df['depth'],
                                          desired_format=depth_fmt,
                                          is_smp=is_smp)

            if 'bottom_depth' in df.columns:
                delta = df['depth'] - new_depth
                df['bottom_depth'] = df['bottom_depth'] - delta

            df['depth'] = new_depth

            delta = abs(df['depth'].max() - df['depth'].min())
            self.log.info('File contains {} profiles each with {:,} layers across '
                          '{:0.2f} cm'.format(len(self.hdr.data_names), len(df), delta))
        return df

    def check(self, site_info):
        """
        Checks to be applied before submitting data
        Currently checks for:

        1. Header information integrity between site info and profile headers

        Args:
            site_info: Dictionary containing all site information

        Raises:
            ValueError: If any mismatches are found
        """

        # Ensure information matches between site details and profile headers
        mismatch = self.hdr.check_integrity(site_info)

        if len(mismatch.keys()) > 0:
            self.log.error('Header Error with {}'.format(self.filename))
            for k, v in mismatch.items():
                self.log.error('\t{}: {}'.format(k, v))
                raise ValueError('Site Information Header and Profile Header '
                                 'do not agree!\n Key: {} does yields {} from '
                                 'here and {} from site info.'.format(k,
                                                                      self.hdr.info[k],
                                                                      site_info[k]))

    def build_data(self, data_name):
        """
        Build out the original dataframe with the metadata to avoid doing it
        during the submission loop. Removes all other main profile columns and
        assigns data_name as the value column

        Args:
            data_name: Name of a the main profile

        Returns:
            df: Dataframe ready for submission
        """

        df = self.df.copy()

        # Assign all meta data to every entry to the data frame
        for k, v in self.hdr.info.items():
            if not pd.isna(v):
                df[k] = v

        df['type'] = data_name
        df['date_accessed'] = self.date_accessed

        # Manage nans and nones
        for c in df.columns:
            df[c] = df[c].apply(lambda x: parse_none(x))

        # Get the average if its multisample profile
        if data_name in self.multi_sample_profiles:
            kw = '{}_sample'.format(data_name)
            sample_cols = [c for c in df.columns if kw in c]
            df['value'] = df[sample_cols].mean(axis=1, skipna=True).astype(str)

            # Replace the data_name sample columns with just sample
            for s in sample_cols:
                n = s.replace(kw, 'sample')
                df[n] = df[s].copy()

        # Individual
        else:
            df['value'] = df[data_name].astype(str)

        # Drop all columns were not expecting
        # drop_cols = [
        #     c for c in df.columns if c not in self.expected_attributes]
        # df = df.drop(columns=drop_cols)

        # Clean up comments a bit
        if 'comments' in df.columns:
            df['comments'] = df['comments'].apply(
                lambda x: x.strip(' ') if isinstance(x, str) else x)

        self._handle_flags(df)

        return df

    def submit(self, session):
        """
        Submit values to the db from dictionary. Manage how some profiles have
        multiple values and get submitted individual

        Args:
            session: SQLAlchemy session
        """

        # Construct a dataframe with all metadata
        for pt in self.data_names:
            df = self.build_data(pt)

            # Grab each row, convert it to dict and join it with site info
            if not df.empty:
                objects = []
                for i, row in df.iterrows():
                    data = row.to_dict()

                    # self.log.debug('\tAdding {} for {} at {}cm'.format(value_type, data['site_id'], data['depth']))
                    d = self._add_entry(session, LayerData, **row)
                    # d = LayerData(**data)
                    objects.append(d)
                session.bulk_save_objects(objects)
                session.commit()
            else:
                self.log.warning('File contains header but no data which is sometimes expected. Skipping db submission.')

        if self.data_names:
            if not df.empty:
                self.log.debug('Profile Submitted!\n')

    def _add_entry(self, session, data_cls, **kwargs):
        # Add instrument
        instrument = self._check_or_add_object(
            session, Instrument, dict(name=kwargs.pop('instrument'))
        )
        # Add campaign
        campaign = self._check_or_add_object(
            session, Campaign, dict(name=kwargs.pop('site_name'))
        )

        # add list of observers
        observer_list = []
        observer_names = kwargs.pop('observers')
        for obs_name in observer_names.split(','):
            observer = self._check_or_add_object(
                session, Observer, dict(name=obs_name)
            )
            observer_list.append(observer)

        # Add site
        site_id = kwargs.pop('pit_id')
        date = kwargs.pop("date")
        meas_time = kwargs.pop("time")
        dt = datetime.datetime.combine(date, meas_time)

        site = self._check_or_add_object(
            session, Site, dict(name=site_id),
            object_kwargs=dict(
                name=site_id, campaign=campaign,
                datetime=dt,
                geom=kwargs.pop("geom"),
                elevation=kwargs.pop("elevation"),
                observers=observer_list,
            ))

        # Add doi
        doi_string = kwargs.pop("doi")
        if doi_string is not None:
            doi = self._check_or_add_object(
                session, DOI, dict(doi=doi_string)
            )
        else:
            doi = None

        # Add measurement type
        measurement_type = kwargs.pop("type")
        measurement_obj = self._check_or_add_object(
            session, MeasurementType, dict(name=measurement_type)
        )

        object_kwargs = dict(
            instrument=instrument,
            doi=doi,
            measurement_type=measurement_obj,
            **kwargs
        )
        # Add site if given
        if site_name is None:
            object_kwargs["campaign"] = campaign
            object_kwargs["observers"] = observer_list
        else:
            object_kwargs["site"] = site

        # Now that the instrument exists, create the entry, notice we only need the instrument object
        new_entry = data_cls(**object_kwargs)
        return new_entry
