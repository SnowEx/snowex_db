import logging

from insitupy.campaigns.snowex import SnowExMetadataParser, \
    SnowExPrimaryVariables
from insitupy.variables import MeasurementDescription

from snowex_db.metadata import ExtendedSnowExPrimaryVariables, \
    ExtendedSnowExMetadataVariables, SnowExProfileMetadata

LOG = logging.getLogger()


class PointPrimaryVariables(SnowExPrimaryVariables):
    DENSITY = MeasurementDescription(
        "density", "measured snow density",
        [
            "density", 'density_mean', 'avgdensity'
        ], auto_remap=True
    )
    TWO_WAY_TRAVEL = MeasurementDescription(
        'two_way_travel', "Two way travel",
        ['twt', 'twt_ns']
    )
    RH_10FT = MeasurementDescription(
        "relative_humidity_10ft",
        "Relative humidity measured at 10 ft tower level",
        ['rh_10ft']
    )
    BP = MeasurementDescription(
        'barometric_pressure', "Barometric pressure",
        ['bp_kpa_avg']
    )
    AIR_TEMP_10FT = MeasurementDescription(
        'air_temperature_10ft',
        "Air temperature measured at 10 ft tower level",
        ['airtc_10ft_avg']
    )
    WIND_SPEED_10FT = MeasurementDescription(
        'wind_speed_10ft',
        "Vector mean wind speed measured at 10 ft tower level",
        ['wsms_10ft_avg']
    )
    WIND_DIR_10ft = MeasurementDescription(
        'wind_direction_10ft',
        "Vector mean wind direction measured at 10 ft tower level",
        ['winddir_10ft_d1_wvt']
    )
    SW_IN = MeasurementDescription(
        'incoming_shortwave',
        "Shortwave radiation measured with upward-facing sensor",
        ['sup_avg']
    )
    SW_OUT = MeasurementDescription(
        'outgoing_shortwave',
        "Shortwave radiation measured with downward-facing sensor",
        ['sdn_avg']
    )
    LW_IN = MeasurementDescription(
        'incoming_longwave',
        "Longwave radiation measured with upward-facing sensor",
        ['lupco_avg']
    )
    LW_OUT = MeasurementDescription(
        'outgoing_longwave',
        "Longwave radiation measured with downward-facing sensor",
        ['ldnco_avg']
    )
    SM_20CM = MeasurementDescription(
        'soil_moisture_20cm', "Soil moisture measured at 10 cm below the soil",
        ['sm_20cm_avg']
    )
    ST_20CM = MeasurementDescription(
        'soil_temperature_20cm',
        "Soil temperature measured at 10 cm below the soil",
        ['tc_20cm_avg']
    )
    SNOW_VOID = MeasurementDescription(
        "snow_void", "Void depth in the snow measurement",
        ["snow void", "snow_void"], True
    )
    DATETIME = MeasurementDescription(
        'datetime', "Combined date and time",
        ["Date/Local Standard Time", "date/local_standard_time", "datetime",
         "date&time", "date/time", "date/local_time"],
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
        'utcyear', "UTC Year", ['utcyear'], auto_remap=True
    )
    UTCDOY = MeasurementDescription(
        'utcdoy', "UTC day of year", ['utcdoy'], auto_remap=True
    )
    UTCTOD = MeasurementDescription(
        'utctod', 'UTC Time of Day', ['utctod'], auto_remap=True
    )
    LATITUDE = MeasurementDescription(
        'latitude', "Latitude",
        ['lat', 'latitude'], auto_remap=True
    )
    LONGITUDE = MeasurementDescription(
        'longitude', "Longitude",
        ['long', 'lon', 'longitude'], auto_remap=True
    )
    NORTHING = MeasurementDescription(
        'northing', "UTM Northing",
        ['northing', 'utm_wgs84_northing'], auto_remap=True
    )
    EASTING = MeasurementDescription(
        'easting', "UTM Easting",
        ['easting', 'utm_wgs84_easting'], auto_remap=True
    )
    UTM_ZONE = MeasurementDescription(
        'utm_zone', "UTM Zone",
        ['utmzone', 'utm_zone'], auto_remap=True
    )
    ELEVATION = MeasurementDescription(
        'elevation', "Elevation",
        ['elev_m', 'elevation', 'elevationwgs84'], auto_remap=True
    )
    INSTRUMENT = MeasurementDescription(
        "instrument", "Instrument", ["measurement_tool"], auto_remap=True
    )
    INSTRUMENT_MODEL = MeasurementDescription(
        "instrument_model", "Instrument Model", ["equipment"], auto_remap=True
    )
    IGNORE = MeasurementDescription(
        "ignore", "Ignore this",
        [
            'id', 'version_number', 'avgvelocity'
        ]
    )


class PointSnowExMetadataParser(SnowExMetadataParser):
    """
    Extend the parser to update the extended varaibles
    """
    PRIMARY_VARIABLES_CLASS = PointPrimaryVariables
    METADATA_VARIABLE_CLASS = ExtendedSnowExMetadataVariables

    def find_header_info(self, filename=None):
        """
        Read in all site details file for a pit If the filename has the word
        site in it then we read everything in the file. Otherwise, we use this
        to read all the site data up to the header of the profile.

        E.g. Read all commented data until we see a column descriptor.

        Args:
            filename: Path to a csv containing # leading lines with site details

        Returns:
            tuple: **data** - Dictionary containing site details
                   **columns** - List of clean column names
                   **header_pos** - Index of the columns header for skiprows in
                                    read_csv
       """
        filename = filename or self._fname
        filename = str(filename)
        with open(filename, encoding='latin') as fp:
            lines = fp.readlines()
            fp.close()

        header_pos, header_indicator = self._find_header_position(lines)
        strip_out = ["(hh:mm, local, MST)"]
        header_line = lines[header_pos]
        # Strip out unhelpful string segments that break parsing
        for so in strip_out:
            header_line = header_line.replace(so, "")
        # identify columns, map columns, and units map
        columns, columns_map, units_map = self._parse_columns(header_line)
        # Combine with user defined units map
        self._units_map = {**self._units_map, **units_map}
        LOG.debug(
            f'Column Data found to be {len(columns)} columns based on'
            f' Line {header_pos}'
        )
        # Only parse what we know if the header
        lines = lines[0:header_pos]

        final_lines = lines

        # Clean up the lines from line returns to grab header info
        final_lines = [ln.strip() for ln in final_lines]
        # Join all data and split on header separator
        # This handles combining split lines
        str_data = " ".join(final_lines).split('#')
        str_data = [ln.strip() for ln in str_data if ln]

        return str_data, columns, columns_map, header_pos

    def parse(self):
        """
        Parse the file and return a metadata object.
        We can override these methods as needed to parse the different
        metadata

        This populates self.rough_obj

        Returns:
            (None, column list, position of header in file)
        """
        (
            meta_lines, columns, columns_map, header_position
        ) = self.find_header_info(self._fname)
        self._rough_obj = self._preparse_meta(meta_lines)
        # We do not have header metadata for point files
        return None, columns, columns_map, header_position