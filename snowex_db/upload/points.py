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
from snowexsql.tables import ImageData, LayerData, PointData

from ..interpretation import add_date_time_keys, standardize_depth
from ..metadata import DataHeader
from ..string_management import parse_none, remap_data_names
from ..utilities import (assign_default_kwargs, get_file_creation_date,
                        get_logger)
from ..projection import reproject_point_in_dict
from .base import BaseUpload
from snowexsql.tables import Instrument, Campaign, Observer, DOI, MeasurementType, Site


LOG = logging.getLogger("snowex_db.upload.points")


class DataValidationError(ValueError):
    pass


class PointDataCSV(BaseUpload):
    """
    Class for submitting whole csv files of point data
    """

    # Remapping for special keywords for snowdepth measurements
    measurement_names = {'mp': 'magnaprobe', 'm2': 'mesa', 'pr': 'pit ruler'}

    # Units to apply
    units = {'depth': 'cm', 'two_way_travel': 'ns', 'swe': 'mm',
             'density': 'kg/m^3'}

    # Class attributes to apply defaults
    defaults = {'debug': True,
                'in_timezone': None}

    def __init__(self, filename, **kwargs):
        """
        Args:
            filename: Path to a csv of data to upload as point data
            debug: Boolean indicating whether to print out debug info
            in_timezone: Pytz valid timezone for the incoming data
        """

        self.log = get_logger(__name__)

        # Assign defaults for this class
        self.kwargs = assign_default_kwargs(self, kwargs, self.defaults)

        # Assign if details are row based (generally for the SWE files)
        self._row_based_crs = self.kwargs.get("row_based_crs", False)
        self._row_based_tz = self.kwargs.get("row_based_timezone", False)
        if self._row_based_tz:
            in_timezone = None
        else:
            in_timezone = kwargs['in_timezone']

        # Use the files creation date as the date accessed for NSIDC citation
        self.date_accessed = get_file_creation_date(filename)

        # NOTE: This will error if in_timezone is not provided
        self.hdr = DataHeader(
            filename, in_timezone=in_timezone,
            **self.kwargs
        )
        self.df = self._read(filename)

        # Performance tracking
        self.errors = []
        self.points_uploaded = 0

    def _read(self, filename):
        """
        Read in the csv
        """

        self.log.info('Reading in CSV data from {}'.format(filename))
        df = pd.read_csv(filename, header=self.hdr.header_pos,
                         names=self.hdr.columns,
                         dtype={'date': str, 'time': str})

        # Assign the measurement tool verbose name
        if 'instrument' in df.columns:
            self.log.info('Renaming instruments to more verbose names...')
            df['instrument'] = \
                df['instrument'].apply(
                    lambda x: remap_data_names(
                        x, self.measurement_names))

        # Add date and time keys
        self.log.info('Adding date and time to metadata...')
        # Date/time was only provided in the header
        if 'date' in self.hdr.info.keys() and 'date' not in df.columns:
            df['date'] = self.hdr.info['date']
            df['time'] = self.hdr.info['time']
        else:
            # date/time was provided in the
            if self._row_based_tz:
                # row based in timezone
                df = df.apply(
                    lambda data: add_date_time_keys(
                        data,
                        in_timezone=TimezoneFinder().timezone_at(
                            lng=data['longitude'], lat=data['latitude']
                        )
                    ), axis=1
                )
            else:
                # file based timezone
                df = df.apply(lambda data: add_date_time_keys(
                    data, in_timezone=self.in_timezone), axis=1)

        # 1. Only submit valid columns to the DB
        self.log.info('Adding valid keyword arguments to metadata...')
        valid = get_table_attributes(PointData)

        # 2. Add northing/Easting/latitude/longitude if necessary
        proj_columns = ['northing', 'easting', 'latitude', 'longitude']
        if any(k in df.columns for k in proj_columns):
            self.log.info('Adding UTM Northing/Easting to data...')
            df = df.apply(lambda row: reproject_point_in_dict(row), axis=1)

        # Use header projection info
        elif any(k in self.hdr.info.keys() for k in proj_columns):
            for k in proj_columns:
                df[k] = self.hdr.info[k]

        # Add geometry
        if self._row_based_crs:
            # EPSG at row level here (EPSG:269...)
            df['geom'] = df.apply(lambda row: WKTElement(
                'POINT({} {})'.format(
                    row['easting'],
                    row['northing']),
                srid=int(row['epsg'])), axis=1)
        else:
            # EPSG at the file level
            df['geom'] = df.apply(lambda row: WKTElement(
                'POINT({} {})'.format(
                    row['easting'],
                    row['northing']),
                srid=self.hdr.info['epsg']), axis=1)

        # 2. Add all kwargs that were valid
        for v in valid:
            if v in self.kwargs.keys():
                df[v] = self.kwargs[v]

        # Add a camera id to the description if camera is in the cols
        # (For camera derived snow depths)
        if 'camera' in df.columns:
            self.log.info('Adding camera id to equipment column...')
            df['equipment'] = df.apply(
                lambda row: f'camera id = {row["camera"]}', axis=1
            )

        # 3. Remove columns that are not valid
        drops = \
            [c for c in df.columns if c not in valid and c not in self.hdr.data_names]
        self.log.info(
            'Dropping {} as they are not valid columns in the database...'.format(
                ', '.join(drops)))
        df = df.drop(columns=drops)

        # replace all nans or string nones with None (none type)
        df = df.apply(lambda x: parse_none(x))

        # Assign the access date for citation
        df['date_accessed'] = self.date_accessed

        return df

    def build_data(self, data_name):
        """
        Pad the dataframe with metadata or make info more verbose
        """
        # Assign our main value to the value column
        df = self.df.copy()
        df['value'] = self.df[data_name].copy()
        df['type'] = data_name

        # Add units
        if data_name in self.units.keys():
            df['units'] = self.units[data_name]

        df = df.drop(columns=self.hdr.data_names)

        return df

    def submit(self, session):
        # Loop through all the entries and add them to the db
        for pt in self.hdr.data_names:
            objects = []
            df = self.build_data(pt)
            self.log.info('Submitting {:,} points of {} to the database...'.format(
                len(df.index), pt))
            for i, row in df.iterrows():
                d = self._add_entry(**row)
                # d = PointData(**row)
                objects.append(d)
            session.bulk_save_objects(objects)
            session.commit()
            self.points_uploaded += len(objects)
