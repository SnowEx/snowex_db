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
from .metadata import ExtendedSnowExMetadataParser
from .string_management import parse_none, remap_data_names
from .utilities import (assign_default_kwargs, get_file_creation_date,
                        get_logger, metadata_to_dict)
from .projection import reproject_point_in_dict


LOG = logging.getLogger("snowex_db.upload")


class DataValidationError(ValueError):
    pass


def determine_data_names(raw_columns, depth_is_metadata=True):
    """
    Determine the names of the data to be uploaded from the raw column
    header. Also determine if this is the type of profile file that will
    submit more than one main value (e.g. hand_hardness, grain size all in
    the same file)

    Args:
        raw_columns: list of raw text split on commas of the column names
        depth_is_metadata: Whether or not to include depth as a main variable

    Returns:
        tuple: (data_names, multi_sample_profiles)
               data_names - list of column names to upload as main values
               multi_sample_profiles - list of profile types with multiple samples
    """
    # Known possible profile types
    available_data_names = [
        'density', 'permittivity', 'lwc_vol', 'temperature',
        'force', 'reflectance', 'sample_signal',
        'specific_surface_area', 'equivalent_diameter',
        'grain_size', 'hand_hardness', 'grain_type',
        'manual_wetness', 'two_way_travel', 'depth', 'swe',
        'relative_humidity_10ft', 'barometric_pressure',
        'air_temp_10ft', 'wind_speed_10ft', 'wind_direction_10ft',
        'incoming_shortwave', 'outgoing_shortwave', 'incoming_longwave',
        'outgoing_longwave', 'soil_moisture_20cm', 'soil_temp_20cm',
        'snow_void'
    ]
    
    # Names of columns we are going to submit as main values
    data_names = []
    multi_sample_profiles = []

    # String of the columns for counting
    str_cols = ' '.join(raw_columns).replace(' ', "_").lower()

    for dname in available_data_names:
        kw_count = str_cols.count(dname)

        # if we have keyword match in our columns then add the type
        if kw_count > 0:
            data_names.append(dname)

            # Avoid triggering on depth and bottom depth in profiles
            if kw_count > 1 and dname != 'depth':
                LOG.debug('{} is multisampled...'.format(dname))
                multi_sample_profiles.append(dname)

    # If depth is metadata (e.g. profiles) then remove it as a main variable
    if 'depth' in data_names and depth_is_metadata:
        data_names.pop(data_names.index('depth'))

    if data_names:
        LOG.info('Names to be uploaded as main data are: {}'
                 ''.format(', '.join(data_names)))
    else:
        raise ValueError('Unable to determine data names from'
                         ' header/columns columns: {}'.format(", ".join(raw_columns)))

    if multi_sample_profiles:
        LOG.info('{} contains multiple samples for each '
                 'layer. The main value will be the average of '
                 'these samples.'.format(', '.join(multi_sample_profiles)))

    return data_names, multi_sample_profiles


class UploadProfileData:
    """
    Class for submitting a single profile. Since layers are uploaded layer by layer this allows for submitting them
    one file at a time.
    """
    expected_attributes = [c for c in dir(LayerData) if c[0] != '_']

    def __init__(self, profile_filename, **kwargs):
        self.log = get_logger(__name__)

        self.filename = profile_filename

        # Read in the file header using insitupy
        # Map in_timezone to timezone parameter for insitupy compatibility
        parser_kwargs = kwargs.copy()
        if 'in_timezone' in parser_kwargs:
            parser_kwargs['timezone'] = parser_kwargs.pop('in_timezone')
        
        parser = ExtendedSnowExMetadataParser(**parser_kwargs)
        self.metadata, self.columns, self.columns_map, self.header_pos = parser.parse(profile_filename)
        
        # Determine data names and multi-sample profiles
        depth_is_metadata = kwargs.get('depth_is_metadata', True)
        self.data_names, self.multi_sample_profiles = determine_data_names(
            self.columns, depth_is_metadata=depth_is_metadata
        )
        
        # Apply data name remapping
        rename_map = {
            'location': 'site_name', 'top': 'depth', 'snow void': "snow_void",
            'height': 'depth', 'bottom': 'bottom_depth', 'site': 'site_id',
            'pitid': 'pit_id', 'slope': 'slope_angle', 'weather': 'weather_description',
            'sky': 'sky_cover', 'notes': 'site_notes', 'sample_top_height': 'depth',
            'deq': 'equivalent_diameter', 'operator': 'observers', 'surveyors': 'observers',
            'observer': 'observers', 'total_snow_depth': 'total_depth',
            'smp_serial_number': 'instrument', 'lat': 'latitude', 'long': 'longitude',
            'lon': 'longitude', 'twt': 'two_way_travel', 'twt_ns': 'two_way_travel',
            'utmzone': 'utm_zone', 'measurement_tool': 'instrument',
            'avgdensity': 'density', 'avg_density': 'density', 'density_mean': 'density',
            'dielectric_constant': 'permittivity', 'flag': 'flags', 'hs': 'depth',
            'swe_mm': 'swe', 'depth_m': 'depth', 'date_dd_mmm_yy': 'date',
            'time_gmt': 'time', 'elev_m': 'elevation', 'rh_10ft': 'relative_humidity_10ft',
            'bp_kpa_avg': 'barometric_pressure', 'airtc_10ft_avg': 'air_temp_10ft',
            'wsms_10ft_avg': 'wind_speed_10ft', 'winddir_10ft_d1_wvt': 'wind_direction_10ft',
            'sup_avg': 'incoming_shortwave', 'sdn_avg': 'outgoing_shortwave',
            'lupco_avg': 'incoming_longwave', 'ldnco_avg': 'outgoing_longwave',
            'sm_20cm_avg': 'soil_moisture_20cm', 'tc_20cm_avg': 'soil_temp_20cm',
            'snowdepthfilter(m)': 'depth'
        }
        self.data_names = remap_data_names(self.data_names, rename_map)
        
        # Create info dict from metadata object for compatibility
        self.info = metadata_to_dict(self.metadata)

        # Read in data
        self.df = self._read(profile_filename)

        # Use the files creation date as the date accessed for NSIDC citation
        self.date_accessed = get_file_creation_date(self.filename)


    def check_integrity(self, site_info):
        """
        Compare the attribute info to the site dictionary to insure integrity
        between datasets. Comparisons are only done as strings currently.

        Args:
            site_info: Dictionary containing the site details file header

        Returns:
            mismatch: Dictionary with a message about how a piece of info is
                      mismatched
        """
        mismatch = {}

        for k, v in self.info.items():
            if k not in site_info.keys():
                mismatch[k] = 'Key not found in site details'
            else:
                if v != site_info[k]:
                    mismatch[k] = 'Profile header != Site details header'

        return mismatch

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
                profile_filename, header=0, skiprows=self.header_pos,
                names=self.columns, encoding='latin'
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
                          '{:0.2f} cm'.format(len(self.data_names), len(df), delta))
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
        mismatch = self.check_integrity(site_info)

        if len(mismatch.keys()) > 0:
            self.log.error('Header Error with {}'.format(self.filename))
            for k, v in mismatch.items():
                self.log.error('\t{}: {}'.format(k, v))
                raise ValueError('Site Information Header and Profile Header '
                                 'do not agree!\n Key: {} does yields {} from '
                                 'here and {} from site info.'.format(k,
                                                                      self.info[k],
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
        for k, v in self.info.items():
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
        # Read in the file header using insitupy
        parser_kwargs = {'timezone': in_timezone}
        parser_kwargs.update(self.kwargs)
        # Map in_timezone to timezone if present in kwargs
        if 'in_timezone' in parser_kwargs:
            parser_kwargs['timezone'] = parser_kwargs.pop('in_timezone')
            
        parser = ExtendedSnowExMetadataParser(**parser_kwargs)
        self.metadata, self.columns, self.columns_map, self.header_pos = parser.parse(filename)
        
        # Create info dict from metadata object for compatibility
        self.info = metadata_to_dict(self.metadata)
        self.df = self._read(filename)

        # Performance tracking
        self.errors = []
        self.points_uploaded = 0


    def _read(self, filename):
        """
        Read in the csv
        """

        self.log.info('Reading in CSV data from {}'.format(filename))
        df = pd.read_csv(filename, header=self.header_pos,
                         names=self.columns,
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
        if 'date' in self.info.keys() and 'date' not in df.columns:
            df['date'] = self.info['date']
            df['time'] = self.info['time']
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
        elif any(k in self.info.keys() for k in proj_columns):
            for k in proj_columns:
                df[k] = self.info[k]

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
                srid=self.info['epsg']), axis=1)

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
            [c for c in df.columns if c not in valid and c not in self.data_names]
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

        df = df.drop(columns=self.data_names)

        return df

    def submit(self, session):
        # Loop through all the entries and add them to the db
        for pt in self.data_names:
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




