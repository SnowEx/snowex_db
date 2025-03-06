"""
Module for header classes and metadata interpreters. This includes interpreting data file headers or dedicated files
to describing data.
"""
import logging
from dataclasses import dataclass
from os.path import basename
from typing import Union

import pandas as pd
from insitupy.campaigns.snowex.snowex_campaign import SnowExMetadataParser
from insitupy.profiles.metadata import ProfileMetaData
from insitupy.variables import MeasurementDescription
from insitupy.campaigns.snowex import (
    SnowExPrimaryVariables, SnowExMetadataVariables,
)
from snowexsql.db import get_table_attributes
from snowexsql.tables import Site

from .interpretation import *
from .projection import add_geom, reproject_point_in_dict
from .string_management import *
from .utilities import assign_default_kwargs, get_logger, read_n_lines


LOG = logging.getLogger(__name__)


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


class ExtendedSnowExMetadataVariables(SnowExMetadataVariables):
    IGNORE = MeasurementDescription(
        "ignore", "Ignore this",
        [
            "profile_id", "timing",  # SSA things
            "smp_serial_number", "original_total_samples",  # SMP things
            "data_subsampled_to",
            "wise_serial_no" # snow pit things
        ], auto_remap=False
    )
    FLAGS = MeasurementDescription(
        'flags', "Measurements flags",
        ['flag', 'flags'], auto_remap=True
    )
    UTM_ZONE = MeasurementDescription(
        'utm_zone', "UTM Zone",
        ['utmzone', 'utm_zone', 'zone'], auto_remap=True
    )
    COUNT = MeasurementDescription(
        "count", "Count for surrounding perimeter depths",
        ["count"], auto_remap=True
    )
    UTCYEAR = MeasurementDescription(
        'utcyear', "UTC Year", ['utcyear'], auto_remap=True
    )
    UTCDOY = MeasurementDescription(
        'utcdoy', "UTC day of year", ['utcdoy'], auto_remap=True
    )
    UTCTOD = MeasurementDescription(
        'utctod', 'UTC Time of Day', ['utctod'], auto_remap=True
    )
    COMMENTS = MeasurementDescription(
        "comments", "Comments in the header", [
            "comments", "pit comments"
        ], auto_remap=True
    )
    SLOPE = MeasurementDescription(
        "slope_angle", "Slope Angle", ["slope", "slope_angle"],
        auto_remap=True
    )
    ASPECT = MeasurementDescription(
        "aspect", "Site Aspect", ["aspect",],
        auto_remap=True
    )
    WEATHER = MeasurementDescription(
        "weather_description", "Weather Description", ["weather"],
        auto_remap=True
    )
    SKY_COVER = MeasurementDescription(
        "sky_cover", "Sky Cover Description", ["sky"], match_on_code=True,
        auto_remap=True
    )
    NOTES = MeasurementDescription(
        "site_notes", "Site Notes", ["notes"], auto_remap=True
    )
    INSTRUMENT = MeasurementDescription(
        "instrument", "Instrument", ["measurement_tool"], auto_remap=True
    )
    OBSERVERS = MeasurementDescription(
        'observers', "Observer(s) of the measurement",
        ['operator', 'surveyors', 'observer'], auto_remap=True
    )
    GROUND_ROUGHNESS = MeasurementDescription(
        "ground_roughness", "Roughness Description", [
            "ground roughness"
        ], auto_remap=True
    )
    GROUND_CONDITION = MeasurementDescription(
        "ground_condition", "The condition of the ground", [
            "ground condition"
        ], auto_remap=True
    )
    GROUND_VEGETATION = MeasurementDescription(
        "ground_vegetation", "Description of the vegetation", [
            "ground vegetation"
        ], auto_remap=True
    )
    VEGETATION_HEIGHT = MeasurementDescription(
        "vegetation_height", "The height of the vegetation", [
            "vegetation height"
        ], auto_remap=True
    )
    PRECIP = MeasurementDescription(
        "precip", "Site notes on precipitation", ["precip"], auto_remap=True
    )
    WIND = MeasurementDescription(
        "wind", "Site notes on wind", ["wind"], auto_remap=True
    )
    AIR_TEMP = MeasurementDescription(
        "air_temp", "Site notes on air temperature", ["air_temp"],
        auto_remap=True
    )
    TREE_CANOPY = MeasurementDescription(
        "tree_canopy", "Description of the tree canopy", ["tree canopy"],
        auto_remap=True
    )


class ExtendedSnowExPrimaryVariables(SnowExPrimaryVariables):
    """
    Extend variables to add a few relevant ones
    """
    COMMENTS = MeasurementDescription(
        "comments", "Comments",
        ["comments"]
    )
    PARAMETER_CODES = MeasurementDescription(
        "parameter_codes", "Parameter Codes",
        ["parameter_codes"]
    )
    FLAGS = MeasurementDescription(
        'flags', "Measurements flags",
        ['flag']
    )
    IGNORE = MeasurementDescription(
        "ignore", "Ignore this",
        [
            "original_index", 'id', 'freq_mhz', 'camera',
            'avgvelocity', 'equipment', 'version_number'
        ]
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


@dataclass()
class SnowExProfileMetadata(ProfileMetaData):
    """
    Extend the profile metadata to add more args
    """
    aspect: Union[float, None] = None
    slope: Union[float, None] = None
    air_temp: Union[float, None] = None
    total_depth: Union[float, None] = None
    weather_description: Union[str, None] = None
    precip: Union[str, None] = None
    sky_cover: Union[str, None] = None
    wind: Union[str, None] = None
    ground_condition: Union[str, None] = None
    ground_roughness: Union[str, None] = None
    ground_vegetation: Union[str, None] = None
    vegetation_height: Union[str, None] = None
    tree_canopy: Union[str, None] = None
    site_notes: Union[str, None] = None


class ExtendedSnowExMetadataParser(SnowExMetadataParser):
    """
    Extend the parser to update the extended varaibles
    """
    PRIMARY_VARIABLES_CLASS = ExtendedSnowExPrimaryVariables
    METADATA_VARIABLE_CLASS = ExtendedSnowExMetadataVariables

    def parse(self):
        """
        Parse the file and return a metadata object.
        We can override these methods as needed to parse the different
        metadata

        This populates self.rough_obj

        Returns:
            (metadata object, column list, position of header in file)
        """
        (
            meta_lines, columns, columns_map, header_position
        ) = self.find_header_info(self._fname)
        self._rough_obj = self._preparse_meta(meta_lines)
        # Create a standard metadata object
        metadata = SnowExProfileMetadata(
            site_name=self.parse_id(),
            date_time=self.parse_date_time(),
            latitude=self.parse_latitude(),
            longitude=self.parse_longitude(),
            utm_epsg=str(self.parse_utm_epsg()),
            campaign_name=self.parse_campaign_name(),
            flags=self.parse_flags(),
            observers=self.parse_observers(),
            aspect=self.parse_aspect(),
            slope=self.parse_slope(),
            air_temp=self.parse_air_temp(),
            total_depth=self.parse_total_depth(),
            weather_description=self.parse_weather_description(),
            precip=self.parse_precip(),
            sky_cover=self.parse_sky_cover(),
            wind=self.parse_wind(),
            ground_condition=self.parse_ground_condition(),
            ground_roughness=self.parse_ground_roughness(),
            ground_vegetation=self.parse_ground_vegetation(),
            vegetation_height=self.parse_vegetation_height(),
            tree_canopy=self.parse_tree_canopy(),
            site_notes=self.parse_site_notes(),
        )

        return metadata, columns, columns_map, header_position

    def parse_aspect(self):
        aspect = None
        for k, v in self.rough_obj.items():
            if k in ["aspect"]:
                aspect = v
                # Handle parsing string
                aspect = manage_degree_values(aspect)
                # Handle conversion to degrees
                if aspect is not None and isinstance(aspect, str):
                    # Check for number of numeric values.
                    numeric = len([True for c in aspect if c.isnumeric()])

                    if numeric != len(aspect) and aspect is not None:
                        LOG.warning(
                            'Aspect recorded as cardinal '
                            'directions, converting to degrees...'
                        )
                        aspect = convert_cardinal_to_degree(aspect)
                break

        return aspect

    def parse_slope(self):
        result = None
        for k, v in self.rough_obj.items():
            if k in ["slope_angle", "slope"]:
                result = v
                # Handle parsing string
                result = manage_degree_values(result)
                break
        return result

    def parse_air_temp(self):
        result = None
        for k, v in self.rough_obj.items():
            if k in ["air_temp"]:
                result = manage_degree_values(v)
                break
        return result

    def parse_total_depth(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.TOTAL_DEPTH.code
        )

    def parse_weather_description(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.WEATHER.code
        )

    def parse_precip(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.PRECIP.code
        )

    def parse_sky_cover(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.SKY_COVER.code
        )

    def parse_wind(self):
        return self.rough_obj.get(ExtendedSnowExMetadataVariables.WIND.code)

    def parse_ground_condition(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.GROUND_CONDITION.code
        )

    def parse_ground_roughness(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.GROUND_ROUGHNESS.code
        )

    def parse_ground_vegetation(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.GROUND_VEGETATION.code
        )

    def parse_vegetation_height(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.VEGETATION_HEIGHT.code
        )

    def parse_tree_canopy(self):
        return self.rough_obj.get(
            ExtendedSnowExMetadataVariables.TREE_CANOPY.code
        )

    def parse_site_notes(self):
        return None



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

    # Known possible profile types anything not in here will throw an error
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

    # Defaults to keywords arguments
    defaults = {
        'in_timezone': None,
        'out_timezone': 'UTC',
        'epsg': None,
        'header_sep': ',',
        'northern_hemisphere': True,
        'depth_is_metadata': True,
        'allow_split_lines': False,
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
        self._fname = filename

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
        Submit metadata to the database as site info, Do not use on profile
        headers. Only use on site_details files.

        Args:
            session: SQLAlchemy session object
        """
        # only submit valid  keys to db
        kwargs = {}
        valid = get_table_attributes(Site)
        for k, v in self.info.items():
            if k in valid:
                kwargs[k] = v

        kwargs = add_geom(kwargs, self.info['epsg'])
        d = Site(**kwargs)
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
        str_data, columns, header_pos = parser.find_header_info()

        if columns is not None:

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
        info = manage_degrees_keys(info)
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
