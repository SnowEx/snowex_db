"""
Module for classes that upload single files to the database.
"""
from typing import List

import pandas as pd

import logging
from snowexsql.tables import LayerData
import datetime
from snowexsql.tables import Instrument, Campaign, Observer, DOI, MeasurementType, Site
from insitupy.campaigns.snowex import SnowExProfileData

from ..metadata import DataHeader
from ..string_management import parse_none
from ..utilities import get_file_creation_date, get_logger
from .base import BaseUpload


from ..profile_data import SnowExProfileDataCollection


LOG = logging.getLogger("snowex_db.upload.layers")


class DataValidationError(ValueError):
    pass


class UploadProfileData(BaseUpload):
    """
    Class for submitting a single profile. Since layers are uploaded layer by layer this allows for submitting them
    one file at a time.
    """
    expected_attributes = [c for c in dir(LayerData) if c[0] != '_']
    TABLE_CLASS = LayerData

    def __init__(self, profile_filename, **kwargs):
        self.log = get_logger(__name__)

        self.filename = profile_filename

        # Read in the file header
        self.hdr = DataHeader(profile_filename, **kwargs)

        # Transfer a couple attributes for brevity
        for att in ['data_names', 'multi_sample_profiles']:
            setattr(self, att, getattr(self.hdr, att))

        # Read in data
        self.data = self._read(profile_filename)

        # Use the files creation date as the date accessed for NSIDC citation
        self.date_accessed = get_file_creation_date(self.filename)

    def _read(self, profile_filename) -> List[SnowExProfileData]:
        """
        Read in a profile file. Managing the number of lines to skip and
        adjusting column names

        Args:
            profile_filename: Filename containing the a manually measured
                             profile
        Returns:
            list of ProfileData objects
        """
        try:
            data = SnowExProfileDataCollection.from_csv(profile_filename)
        except pd.errors.ParserError as e:
            LOG.error(e)
            raise RuntimeError(f"Failed reading {profile_filename}")

        return data

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

    def build_data(self, profile: SnowExProfileData):
        """
        Build out the original dataframe with the metadata to avoid doing it
        during the submission loop. Removes all other main profile columns and
        assigns data_name as the value column

        Args:
            profile: The object of a single profile

        Returns:
            df: Dataframe ready for submission
        """

        df = profile.df.copy()
        # TODO: what do we do with the metadata
        metadata = profile.metadata
        variable = profile.variable

        df['type'] = [variable.code] * len(df)

        # Manage nans and nones
        for c in df.columns:
            df[c] = df[c].apply(lambda x: parse_none(x))
        df['value'] = df[variable.code].astype(str)

        # Drop all columns were not expecting
        # drop_cols = [
        #     c for c in df.columns if c not in self.expected_attributes]
        # df = df.drop(columns=drop_cols)

        # Clean up comments a bit
        if 'comments' in df.columns:
            df['comments'] = df['comments'].apply(
                lambda x: x.strip(' ') if isinstance(x, str) else x)

        return df

    def submit(self, session):
        """
        Submit values to the db from dictionary. Manage how some profiles have
        multiple values and get submitted individual

        Args:
            session: SQLAlchemy session
        """

        # Construct a dataframe with all metadata
        for profile in self.data:
            df = self.build_data(profile)

            # Grab each row, convert it to dict and join it with site info
            if not df.empty:
                objects = []
                for i, row in df.iterrows():
                    data = row.to_dict()
                    d = self._add_entry(session, LayerData, **row)
                    objects.append(d)
                session.bulk_save_objects(objects)
                session.commit()
            else:
                self.log.warning('File contains header but no data which is sometimes expected. Skipping db submission.')

    def _add_entry(self, session, **kwargs):
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

        # Now that the other objects exist, create the entry,
        # notice we only need the instrument object
        new_entry = self.TABLE_CLASS(
            # Linked tables
            instrument=instrument,
            doi=doi,
            measurement_type=measurement_obj,
            site=site,
            # Arguments from kwargs
            depth=kwargs.get("depth"),
            bottom_depth=kwargs.get("bottom_depth"),
            comments=kwargs.get("comments"),
            value=kwargs.get("value"),
            flags=kwargs.get("flags"),
        )
        return new_entry
