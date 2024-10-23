"""
Module for classes that upload single files to the database.
"""


import pandas as pd
from geoalchemy2.elements import WKTElement
from os.path import basename, exists, join
import logging
from timezonefinder import TimezoneFinder
from snowexsql.db import get_table_attributes
from snowexsql.tables import LayerData, PointData

from .interpretation import add_date_time_keys, standardize_depth
from .metadata import DataHeader
from .string_management import parse_none, remap_data_names
from .utilities import (assign_default_kwargs, get_file_creation_date,
                        get_logger)
from .projection import reproject_point_in_dict


LOG = logging.getLogger("snowex_db.upload")


class DataValidationError(ValueError):
    pass


class UploadProfileData:
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
        drop_cols = [
            c for c in df.columns if c not in self.expected_attributes]
        df = df.drop(columns=drop_cols)

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
                    d = LayerData(**data)
                    objects.append(d)
                session.bulk_save_objects(objects)
                session.commit()
            else:
                self.log.warning('File contains header but no data which is sometimes expected. Skipping db submission.')

        if self.data_names:
            if not df.empty:
                self.log.debug('Profile Submitted!\n')


class PointDataCSV(object):
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
                d = PointData(**row)
                objects.append(d)
            session.bulk_save_objects(objects)
            session.commit()
            self.points_uploaded += len(objects)




