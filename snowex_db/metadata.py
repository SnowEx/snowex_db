"""
Module for header classes and metadata interpreters. This includes interpreting data file headers or dedicated files
to describing data.
"""
import logging
import pandas as pd
import pytz

from dataclasses import dataclass
from typing import Tuple, Union

from insitupy.profiles.metadata import ProfileMetaData
from insitupy.campaigns.snowex.snowex_metadata import SnowExMetaDataParser

from .interpretation import (
    manage_degree_values, convert_cardinal_to_degree
)
from .string_management import *

LOG = logging.getLogger(__name__)


def read_InSar_annotation(ann_file):
    """
    .ann files describe the INSAR data. Use this function to read all that
    information in and return it as a dictionary

    Expected format:

    `DEM Original Pixel spacing (arcsec) = 1`

    Where this is interpreted as:
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


@dataclass()
class SnowExProfileMetadata(ProfileMetaData):
    """
    Extend the profile metadata to add more args
    """
    air_temp: Union[float, None] = None
    aspect: Union[float, None] = None
    comments: Union[str, None] = None
    ground_condition: Union[str, None] = None
    ground_roughness: Union[str, None] = None
    ground_vegetation: Union[str, None] = None
    instrument: Union[str, None] = None
    instrument_model: Union[str, None] = None
    precip: Union[str, None] = None
    sky_cover: Union[str, None] = None
    slope: Union[float, None] = None
    total_depth: Union[float, None] = None
    tree_canopy: Union[str, None] = None
    vegetation_height: Union[str, None] = None
    weather_description: Union[str, None] = None
    wind: Union[str, None] = None


class ExtendedSnowExMetadataParser(SnowExMetaDataParser):
    """
    Extend the parser to update the parsing function
    """

    def parse(self, filename: str) \
            -> Tuple[SnowExProfileMetadata, list, dict, int]:
        """
        Parse the file and return a metadata object.
        We can override these methods as needed to parse the different
        metadata

        This populates self.rough_obj

        Args:
            filename: Path to the file from which to parse metadata

        Returns:
            (metadata object, column list, position of header in file)
        """
        (
            meta_lines, columns, columns_map, header_position
        ) = self.find_header_info(filename)
        self._rough_obj = self._preparse_meta(meta_lines)
        # Create a standard metadata object
        metadata = SnowExProfileMetadata(
            air_temp=self.parse_air_temp(),
            aspect=self.parse_aspect(),
            campaign_name=self.parse_campaign_name(),
            comments=self.parse_header('COMMENTS'),
            date_time=self.parse_date_time(),
            flags=self.parse_flags(),
            ground_condition=self.parse_header('GROUND_CONDITION'),
            ground_roughness=self.parse_header('GROUND_ROUGHNESS'),
            ground_vegetation=self.parse_header('GROUND_VEGETATION'),
            instrument=self.parse_header('INSTRUMENT'),
            instrument_model=self.parse_header('INSTRUMENT_MODEL'),
            latitude=self.parse_latitude(),
            longitude=self.parse_longitude(),
            observers=self.parse_observers(),
            precip=self.parse_header('PRECIP'),
            site_name=self.parse_id(),
            sky_cover=self.parse_header('SKY_COVER'),
            slope=self.parse_slope(),
            total_depth=self.parse_header('TOTAL_DEPTH'),
            tree_canopy=self.parse_header('TREE_CANOPY'),
            utm_epsg=str(self.parse_utm_epsg()),
            vegetation_height=self.parse_header('VEGETATION_HEIGHT'),
            weather_description=self.parse_header('WEATHER'),
            wind=self.parse_header('WIND'),
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

    def parse_header(self, name):
        return self.rough_obj.get(
            self.metadata_variables.entries[name].code
        )


