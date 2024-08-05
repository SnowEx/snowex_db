"""
Module for header classes and metadata interpreters. This includes interpreting data file headers or dedicated files
to describing data.
"""

from os.path import basename
import pandas as pd
from insitupy.campaigns.campaign import SnowExMetadataParser
from insitupy.campaigns.variables import SnowExProfileVariables, \
    MeasurementDescription
from snowexsql.db import get_table_attributes
from snowexsql.data import SiteData

from .interpretation import *
from .projection import add_geom, reproject_point_in_dict
from .string_management import *
from .utilities import assign_default_kwargs, get_logger, read_n_lines


def read_InSar_annotation(ann_file):
    """
    .ann files describe the INSAR data. Use this function to read all that
    information in and return it as a dictionary

    Expected format:

    `DEM Original Pixel spacing (arcsec) = 1`

    Where this is interpretted as:
    `key (units) = [value]`

    Then stored in the dictionary as:

    `data[key] = {'value':value, 'units':units}`

    values that are found to be numeric and have a decimal are converted to a
    float otherwise numeric data is cast as integers. Everything else is left
    as strings.

    Args:
        ann_file: path to UAVsAR description file
    Returns:
        data: Dictionary containing a dictionary for each entry with keys
              for value, units and comments
    """

    with open(ann_file) as fp:
        lines = fp.readlines()
        fp.close()

    data = {}

    # loop through the data and parse
    for line in lines:

        # Filter out all comments and remove any line returns
        info = line.strip().split(';')
        comment = info[-1].strip().lower()
        info = info[0]

        # ignore empty strings
        if info and "=" in info:
            d = info.split('=')
            name, value = d[0], d[1]

            # Clean up tabs, spaces and line returns
            key = name.split('(')[0].strip().lower()
            units = get_encapsulated(name, '()')
            if not units:
                units = None
            else:
                units = units[0]

            value = value.strip()

            # Cast the values that can be to numbers ###
            if value.strip('-').replace('.', '').isnumeric():
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)

            # Assign each entry as a dictionary with value and units
            data[key] = {'value': value, 'units': units, 'comment': comment}

    # Convert times to datetimes
    for pass_num in ['1', '2']:
        for timing in ['start', 'stop']:
            key = '{} time of acquisition for pass {}'.format(timing, pass_num)
            dt = pd.to_datetime(data[key]['value'])
            dt = dt.astimezone(pytz.timezone('UTC'))
            data[key]['value'] = dt

    return data


class SMPMeasurementLog(object):
    """
    Opens and processes the log that describes the SMP measurments. This file
    contains notes on all the measurements taken.

    This class build a dataframe from this file. It also reorganizes the
    file contents to be more standardized with our database.
    Some of this includes merging information in the comments.

    File should have the headers:
              Date,
              Pit ID
              SMP instrument #
              Fname sufix
              Orientation
              Snow depth
              Flag
              Observer
              Comments

    Attributes:
        observer_map: Dictionary mapping name initials to full verbose names
        orientation_map: Dictionary mapping the measurement locations relative
                         to the pit
        header: Dictionary containing other header information regarding the
                details of measurements
        df: Dataframe containing rows of details describing each measurement

    """

    def __init__(self, filename):
        self.log = get_logger(__name__)

        self.header, self.df = self._read(filename)

        # Cardinal map to interpet the orientation
        self.cardinal_map = {'N': 'North', 'NE': 'Northeast', 'E': 'East',
                             'SE': 'Southeast', 'S': 'South', 'SW': 'Southwest',
                             'W': 'West', 'NW': 'Northwest', 'C': 'Center'}

    def _read(self, filename):
        """
        Read the CSV file thet contains SMP log inforamtion. Also reads in the
        header and creates a few attributes from that information:
            1. observer_map
            2. orientation_map
        """
        self.log.info('Reading SMP file log header')

        header_pos = 9
        header = read_n_lines(filename, header_pos + 1)
        self.observer_map = self._build_observers(header)

        # parse/rename column names
        line = header[header_pos]
        str_cols = [standardize_key(col)
                    for col in line.lower().split(',') if col.strip()]

        # Assume columns are populated left to right so if we have empty ones
        # they are assumed at the end
        n_cols = len(str_cols)
        str_cols = remap_data_names(str_cols, DataHeader.rename)

        dtype = {k: str for k in str_cols}
        df = pd.read_csv(
            filename, header=header_pos, names=str_cols,
            usecols=range(n_cols), encoding='latin',
            # parse_dates=[0],
            dtype=dtype
        )
        # WHY IS THIS NEEDED?
        df["date"] = pd.to_datetime(df["date"])

        # Insure all values are 4 digits. Seems like some were not by accident
        df['fname_sufix'] = df['fname_sufix'].apply(lambda v: v.zfill(4))

        df = self.interpret_dataframe(df)

        return header, df

    def interpret_dataframe(self, df):
        """
        Using various info collected from the dataframe header modify the data
        frame entries to be more verbose and standardize the database

        Args:
            df: pandas.Dataframe

        Returns:
            new_df: pandas.Dataframe with modifications
        """
        # Apply observer map
        df = self.interpret_observers(df)

        # Apply orientation map

        # Pit ID is actually the Site ID here at least in comparison to the
        df['site_id'] = df['pit_id'].copy()

        return df

    def _build_observers(self, header):
        """
        Interprets the header of the smp file log which has a map to the
        names of the oberservers names. This creates a dictionary mapping those
        string names
        """
        # Map for observer names and their
        observer_map = {}

        for line in header:
            ll = line.lower()

            # Create a name map for the observers and there initials
            if 'observer' in ll:
                data = [d.strip() for d in line.split(':')[-1].split(',')]
                data = [d for d in data if d]

                for d in data:
                    info = [clean_str(s).strip(')') for s in d.split('(')]
                    name = info[0]
                    initials = info[1]
                    observer_map[initials] = name
                break

        return observer_map

    def interpret_observers(self, df):
        """
        Rename all the observers with initials in the observer_map which is
        interpeted from the header

        Args:
            df: dataframe containing a column observer
        Return:
            new_df: df with the observers column replaced with more verbose
                    names
        """
        new_df = df.copy()
        new_df['observers'] = \
            new_df['observers'].apply(lambda x: self.observer_map[x])
        return new_df

    def interpret_sample_strategy(self, df):
        """
        Look through all the measurements posted by site and attempt to
        determine the sample strategy

        Args:
            df: Dataframe containing all the data from the dataframe
        Returns:
            new_df: Same dataframe with a new column containing the sampling
                    strategy
        """

        pits = pd.unique(df['pit_id'])

        for p in pits:
            ind = df['pit_id'] == p
            temp = df.loc[ind]
            orientations = pd.unique(temp['orientation'])

    def get_metadata(self, smp_file):
        """
        Builds a dictionary of extra header information useful for SMP
        files which lack some info regarding submission to the db

        S06M0874_2N12_20200131.CSV, 0874 is the suffix

        """
        s = basename(smp_file).split('.')[0].split('_')
        suffix = s[0].split('M')[-1]
        ind = self.df['fname_sufix'] == suffix
        meta = self.df.loc[ind]
        return meta.iloc[0].to_dict()


class ExtendedSnowExProfileVariables(SnowExProfileVariables):
    """
    Extend variables to add a few relevant ones
    """
    DEPTH = MeasurementDescription(
        "depth", "top or center depth of measurement",
        [
            "depth", "top", "sample_top_height", "hs",
            "depth_m", 'snowdepthfilter(m)', 'snowdepthfilter',
            'height'
        ], True
    )
    PERMITTIVITY = MeasurementDescription(
        "permittivity", "Permittivity",
        ["permittivity_a", "permittivity_b", "permittivity",
         'dielectric_constant', 'dielectric_constant_a',
         'dielectric_constant_b']
    )
    IGNORE = MeasurementDescription(
        "ignore", "Ignore this",
        ["original_index", 'id', 'freq_mhz', 'camera', 'avgvelocity']
    )
    SAMPLE_SIGNAL = MeasurementDescription(
        'sample_signal', "Sample Signal",
        ['sample_signal']
    )
    FORCE = MeasurementDescription(
        'force', "Force", ["force"]
    )
    REFLECTANCE = MeasurementDescription(
        'reflectance', "Reflectance", ['reflectance']
    )
    SSA = MeasurementDescription(
        'specific_surface_area', "Specific Surface Area",
        ['specific_surface_area']
    )
    DATETIME = MeasurementDescription(
        'datetime', "Combined date and time",
        ["Date/Local Standard Time", "date/local_standard_time", "datetime",
         "date&time"],
        True
    )
    DATE = MeasurementDescription(
        'date', "Measurement Date (only date column)",
        ['date_dd_mmm_yy', 'date']
    )
    TIME = MeasurementDescription(
        'time', "Measurement time",
        ['time_gmt', 'time']
    )
    UTCYEAR = MeasurementDescription(
        'utcyear', "UTC Year", ['utcyear']
    )
    UTCDOY = MeasurementDescription(
        'utcdoy', "UTC day of year", ['utcdoy']
    )
    UTCTOD = MeasurementDescription(
        'utctod', 'UTC Time of Day', ['utctod']
    )
    ELEVATION = MeasurementDescription(
        'elevation', "Elevation",
        ['elev_m', 'elevation']
    )
    EQUIPMENT = MeasurementDescription(
        'equipment', "Equipment",
        ['equipment']
    )
    VERSION_NUMBER = MeasurementDescription(
        'version_number', "Version Number",
        ['version_number']
    )
    NORTHING = MeasurementDescription(
        'northing', "UTM Northing",
        ['northing', 'utm_wgs84_northing']
    )
    EASTING = MeasurementDescription(
        'easting', "UTM Easting",
        ['easting', 'utm_wgs84_easting']
    )


class ExtendedSnowExMetadataParser(SnowExMetadataParser):
    """
    Extend the parser to update the extended varaibles
    """
    VARIABLES_CLASS = ExtendedSnowExProfileVariables


class DataHeader(object):
    """
    Class for managing information stored in files headers about a snow pit
    site.

    Format of such file headers should be
    1. Each line of importance is preceded by a #
    2. Key values should be comma separated.

    e.g.
        `# PitID,COGM1C8_20200131`
        `# Date/Time,2020-01-31-15:10`

    If the file is not determined to be a site details file as indicated by the
    word site in the filename, then the all header lines except the last line
    is interpreted as header. In csv files the last line of the
    header should be the column header which is also interpreted and stored
    as a class attribute

    Attributes:
        info: Dictionary containing all header information, stripped of
              unnecessary chars, all lower case, and all spaces replaced with
              underscores
        columns: Column names of data stored in csv. None for site description
                 files which is basically all one header
        data_names: List of data names to be uploaded
        multi_sample_profiles: List containing profile types that
                              have multiple samples (e.g. density). This
                              triggers calculating the mean of the profiles
                              for the main value
        extra_header: Dictionary containing supplemental information to write
                      into the .info dictionary after its generated. Any
                      duplicate keys will be overwritten with this info.
    """

    # Typical names we run into that need renaming
    rename = {'location': 'site_name',
              'top': 'depth',
              'height': 'depth',
              'bottom': 'bottom_depth',
              'site': 'site_id',
              'pitid': 'pit_id',
              'slope': 'slope_angle',
              'weather': 'weather_description',
              'sky': 'sky_cover',
              'notes': 'site_notes',
              'sample_top_height': 'depth',
              'deq': 'equivalent_diameter',
              'operator': 'observers',
              'surveyors': 'observers',
              'observer': 'observers',
              'total_snow_depth': 'total_depth',
              'smp_serial_number': 'instrument',
              'lat': 'latitude',
              'long': 'longitude',
              'lon': 'longitude',
              'twt': 'two_way_travel',
              'twt_ns': 'two_way_travel',
              'utmzone': 'utm_zone',
              'measurement_tool': 'instrument',
              'avgdensity': 'density',
              'avg_density': 'density',
              'density_mean': 'density',
              'dielectric_constant': 'permittivity',
              'flag': 'flags',
              'hs': 'depth',
              'swe_mm': 'swe',
              'depth_m': 'depth',
              'date_dd_mmm_yy': 'date',
              'time_gmt': 'time',
              'elev_m': 'elevation'
              }

    # Known possible profile types anything not in here will throw an error
    available_data_names = ['density', 'permittivity', 'lwc_vol', 'temperature',
                            'force', 'reflectance', 'sample_signal',
                            'specific_surface_area', 'equivalent_diameter',
                            'grain_size', 'hand_hardness', 'grain_type',
                            'manual_wetness', 'two_way_travel', 'depth', 'swe']

    # Defaults to keywords arguments
    defaults = {
        'in_timezone': None,
        'out_timezone': 'UTC',
        'epsg': None,
        'header_sep': ',',
        'northern_hemisphere': True,
        'depth_is_metadata': True,
        'allow_split_lines': False
    }

    def __init__(self, filename, **kwargs):
        """
        Class for managing site details information

        Args:
            filename: File for a site details file containing
            header_sep: key value pairs in header information separator (: , etc)
            northern_hemisphere: Bool describing if the pit location is in the
                                 northern_hemisphere for converting utm coordinatess
            depth_is_metadata: Whether or not to include depth as a main
                              variable (useful for point data that contains
                              snow depth and other variables), profiles should
                              use depth as metadata
            kwargs: keyword values to pass to the database as metadata
        """
        self.log = get_logger(__name__)

        self.extra_header = assign_default_kwargs(
            self, kwargs, self.defaults, leave=['epsg'])

        # Use a row based timezone
        if kwargs.get("row_based_timezone", False):
            if kwargs.get('in_timezone'):
                raise ValueError(
                    "Cannot have row based and file based timezone"
                )
            self.in_timezone = None
        else:
            # Validate that an intentionally good in timezone was given
            in_timezone = kwargs.get('in_timezone')
            if in_timezone is None or "local" in in_timezone.lower():
                raise ValueError("A valid in_timezone was not provided")
            else:
                self.in_timezone = in_timezone

        self.log.info('Interpreting metadata in {}'.format(filename))

        # Site location files will have no data_name
        self.data_names = None

        # Does our profile type have multiple samples
        self.multi_sample_profiles = []

        # Read in the header into dictionary and list of columns names
        info, self.columns, self.header_pos = self._read(filename)

        # Interpret any data needing interpretation e.g. aspect
        self.info = self.interpret_data(info)

    def submit(self, session):
        """
        Submit meta data to the database as site info, Do not use on profile
        headers. Only use on site_details files.

        Args:
            session: SQLAlchemy session object
        """
        # only submit valid  keys to db
        kwargs = {}
        valid = get_table_attributes(SiteData)
        for k, v in self.info.items():
            if k in valid:
                kwargs[k] = v

        kwargs = add_geom(kwargs, self.info['epsg'])
        d = SiteData(**kwargs)
        session.add(d)
        session.commit()

    def rename_sample_profiles(self, columns, data_names):
        """
        Rename columns like density_a to density_sample_a
        """
        result = []
        for c in columns:
            for data_name in data_names:
                v = c
                if data_name in c and c[-2] == '_':
                    v = c.replace(data_name, '{}_sample'.format(data_name))
                    result.append(v)

                elif c not in result and c[-2] != '_':
                    result.append(c)
        return result

    def determine_data_names(self, raw_columns):
        """
        Determine the names of the data to be uploaded from the raw column
        header. Also determine if this is the type of profile file that will
        submit more than one main value (e.g. hand_hardness, grain size all in
        the same file)

        Args:
            raw_columns: list of raw text split on commas of the column names

        Returns:
            type: **data_names** - list of column names that will be uploaded
                   as a main value
                  **multi_sample_profiles** - boolean representing if we will
                    average the samples for a main value (e.g. density)
        """
        # Names of columns we are going to submit as main values
        data_names = []
        multi_sample_profiles = []

        # String of the columns for counting
        str_cols = ' '.join(raw_columns).replace(' ', "_").lower()

        for dname in self.available_data_names:

            kw_count = str_cols.count(dname)

            # if we have keyword match in our columns then add the type
            if kw_count > 0:
                data_names.append(dname)

                # Avoid triggering on depth and bottom depth in profiles
                if kw_count > 1 and dname != 'depth':
                    self.log.debug('{} is multisampled...'.format(dname))
                    multi_sample_profiles.append(dname)

        # If depth is metadata (e.g. profiles) then remove it as a main
        # variable
        if 'depth' in data_names and self.depth_is_metadata:
            data_names.pop(data_names.index('depth'))

        if data_names:
            self.log.info('Names to be uploaded as main data are: {}'
                          ''.format(', '.join(data_names)))
        else:
            raise ValueError('Unable to determine data names from'
                             ' header/columns columns: {}'.format(", ".join(raw_columns)))

        if multi_sample_profiles:
            self.log.info('{} contains multiple samples for each '
                          'layer. The main value will be the average of '
                          'these samples.'.format(', '.join(multi_sample_profiles)))

        return data_names, multi_sample_profiles

    def _read(self, filename):
        """
        Read in all site details file for a pit If the filename has the word site in it then we
        read everything in the file. Otherwise we use this to read all the site
        data up to the header of the profile.

        E.g. Read all commented data until we see a column descriptor.

        Args:
            filename: Path to a csv containing # leading lines with site details

        Returns:
            tuple: **data** - Dictionary containing site details
                   **columns** - List of clean column names
                   **header_pos** - Index of the columns header for skiprows in
                                    read_csv
       """

        parser = ExtendedSnowExMetadataParser(
            filename, timezone=self.in_timezone,
            header_sep=self.header_sep,
            allow_split_lines=self.allow_split_lines
        )
        str_data, standard_cols, header_pos = parser.find_header_info()

        if standard_cols is not None:
            # handle name remapping
            columns = remap_data_names(standard_cols, self.rename)
            # Determine the profile type
            (self.data_names, self.multi_sample_profiles) = \
                self.determine_data_names(columns)

            self.data_names = remap_data_names(self.data_names, self.rename)

            if self.multi_sample_profiles:
                columns = self.rename_sample_profiles(columns, self.data_names)
            self.log.debug('Column Data found to be {} columns based on Line '
                           '{}'.format(len(columns), header_pos))
        else:
            columns = standard_cols

        # Key value pairs are separate by some separator provided.
        data = {}

        # Collect key value pairs from the information above the column header
        for ln in str_data:
            d = ln.split(self.header_sep)

            # Key is always the first entry in comma sep list
            k = standardize_key(d[0])

            # Avoid splitting on times
            if 'time' in k or 'date' in k:
                value = ':'.join(d[1:]).strip()
            else:
                value = ', '.join(d[1:])
                value = clean_str(value)

            # Assign non empty strings to dictionary
            if k and value:
                data[k] = value.strip(' ').replace('"', '').replace('  ', ' ')

            elif k and not value:
                data[k] = None

        # If there is not header data then don't bother (useful for point data)
        if data:
            data = add_date_time_keys(
                data,
                in_timezone=self.in_timezone,
                out_timezone=self.out_timezone)

        # Rename the info dictionary keys to more standard ones
        data = remap_data_names(data, self.rename)
        self.log.debug('Discovered {} lines of valid header info.'
                       ''.format(len(data.keys())))

        return data, columns, header_pos

    def check_integrity(self, site_info):
        """
        Compare the attribute info to the site dictionary to insure integrity
        between datasets. Comparisons are only done as strings currently.

        In theory the site details header should contain identical info
        to the profile header, it should only have more info than the profile
        header.

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

    def interpret_data(self, raw_info):
        """
        Some data inside the headers is inconsistently noted. This function
        adjusts such data to the correct format.

        Adjustments include:

        A. Add in any extra info from the extra_header dictionary, defer to info
        provided by user

        B: Rename any keys that need to be renamed

        C. Aspect is recorded either cardinal directions or degrees from north,
        should be in degrees

        D. Cast UTM attributes to correct types. Convert UTM to lat long, store both


        Args:
            raw_info: Dictionary containing information to be parsed
        Returns:
            info: Dictionary of the raw_info containing interpetted info

        """
        info = {}

        # A. Parse out any nans, nones or other not-data type entries
        for k, v in raw_info.items():
            info[k] = parse_none(raw_info[k])

        keys = info.keys()

        # Merge information, warn user about overwriting
        overwrite_keys = [k for k in keys if k in self.extra_header.keys()]

        if overwrite_keys:
            self.log.warning('Extra header information passed will overwrite '
                             'the following information found in the file '
                             'header:\n{}'.format(', '.join(overwrite_keys)))

        info.update(self.extra_header)

        # Convert slope, aspect, and zone to numbers
        info = manage_degrees(info)
        info = manage_aspect(info)
        info = manage_utm_zone(info)

        # Convert lat/long to utm and vice versa if either exist
        original_zone = info['utm_zone']
        info = reproject_point_in_dict(
            info, is_northern=self.northern_hemisphere)

        if info['utm_zone'] != original_zone and original_zone is not None:
            self.log.warning(f'Overwriting UTM zone in the header from {original_zone} to {info["utm_zone"]}')

        self.epsg = info['epsg']

        # Check for point data which will contain this in the data not the
        # header
        if not is_point_data(self.columns):
            if self.epsg is None:
                raise RuntimeError("EPSG was not determined from the header nor was it "
                                   "passed as a kwarg to uploader. If there is no "
                                   "projection information in the file please "
                                   "prescribe an epsg value")

            info = add_geom(info, self.epsg)

        # If columns or info does not have coordinates raise an error
        important = ['northing', 'latitude']

        cols_have_coords = []
        if self.columns is not None:
            cols_have_coords = [c for c in self.columns if c in important]

        hdr_has_coords = [c for c in info if c in important]

        if not cols_have_coords and not hdr_has_coords:
            raise (ValueError('No geographic information was provided in the'
                              ' file header or via keyword arguments.'))
        return info
