import logging
from pathlib import Path
from typing import List

from insitupy.io.metadata import MetaDataParser
from timezonefinder import TimezoneFinder
import numpy as np
import pandas as pd
import geopandas as gpd
from insitupy.campaigns.snowex import SnowExProfileData
from insitupy.io.dates import DateTimeManager
from insitupy.io.locations import LocationManager
from insitupy.io.yaml_codes import YamlCodes

from insitupy.profiles.base import MeasurementData
from insitupy.profiles.metadata import ProfileMetaData
from insitupy.variables import MeasurementDescription, ExtendableVariables

from .point_metadata import PointSnowExMetadataParser

LOG = logging.getLogger(__name__)


class SnowExPointData(MeasurementData):
    OUT_TIMEZONE = "UTC"

    def __init__(
        self, variable: MeasurementDescription = None,
        meta_parser: MetaDataParser = None,
        row_based_timezone=False,
        timezone=None
    ):
        """
        Args:
            See MeasurementData.__init__
            row_based_timezone: does each row have a unique timezone implied
            timezone: input timezone for the whole file

        """
        self._row_based_timezone = row_based_timezone
        self._in_timezone = timezone
        super().__init__(variable, meta_parser)

    @staticmethod
    def read_csv_dataframe(profile_filename, columns, header_position):
        """
        Read in a profile file. Managing the number of lines to skip and
        adjusting column names

        Args:
            profile_filename: Filename containing a manually measured
                             profile
            columns: list of columns to use in dataframe
            header_position: skiprows for pd.read_csv
        Returns:
            df: pd.dataframe contain csv data with desired column names
        """
        # header=0 because docs say to if using skip rows and columns
        df = pd.read_csv(
            profile_filename, header=0,
            skiprows=header_position,
            names=columns,
            encoding='latin',
            dtype=str  # treat all columns as strings to get weird date format
        )
        if "flags" in df.columns:
            # Max length of the flags column
            df["flags"] = df["flags"].str.replace(" ", "")

        return df

    def _get_location(self, row):
        """
        fill in the location info for a row
        Args:
            row: pandas row
        """
        try:
            lat, lon, *_ = LocationManager.parse(row)
        except ValueError as e:
            if self.metadata is not None:
                LOG.warning(
                    f"Row {row.name} does not have a valid location. "
                    "Attempting to use header metadata."
                )
                lat, lon = self.metadata.latitude, self.metadata.longitude
            else:
                raise RuntimeError("No valid location found in row or metadata.")

        return lat, lon

    def _get_datetime(self, row):
        """
        fill in the datetime info for a row
        Args:
            row: pandas row
        """
        tz = self._in_timezone
        if self._row_based_timezone:
            # Look up the timezone for the location and apply that
            timezone_str = TimezoneFinder().timezone_at(
                lat=row["latitude"], lng=row["longitude"]
            )
            tz = timezone_str  # e.g., 'America/Denver'
        try:
            datetime = None
            # In case we found a date entry that has date and time
            if row.get(YamlCodes.DATE_TIME) is not None:
                str_date = str(
                    row[YamlCodes.DATE_TIME].replace('T', '-')
                )
                datetime = pd.to_datetime(str_date)

            if datetime is None:
                datetime = DateTimeManager.handle_separate_datetime(row)

            result = DateTimeManager.adjust_timezone(
                datetime,
                in_timezone=tz,
                out_timezone=self.OUT_TIMEZONE
            )
        except ValueError as e:
            if self.metadata is not None:
                result = self.metadata.date_time
            else:
                raise e
        return result

    def _format_df(self, input_df):
        """
        Format the incoming df with the column headers and other info we want
        This will filter to a single measurement as well as the expected
        shared columns like depth
        """
        self._set_column_mappings(input_df)

        # Verify the sample column exists and rename to variable
        df = self._check_sample_columns(input_df)

        # Get the campaign name
        if "campaign" not in df.columns:
            df["campaign"] = df.get(YamlCodes.SITE_NAME)
        # TODO: How do we speed this up?
        #   campaign should be very quick with a df level logic
        #   but the other ones will take morelogic
        # parse the location
        df[["latitude", "longitude"]] = df.apply(
            self._get_location, axis=1, result_type="expand"
        )
        # Parse the datetime
        df["datetime"] = df.apply(self._get_datetime, axis=1, result_type="expand")

        location = gpd.points_from_xy(
            df["longitude"], df["latitude"]
        )
        df = df.drop(columns=["longitude", "latitude"])

        df = gpd.GeoDataFrame(
            df, geometry=location
        ).set_crs("EPSG:4326")
        df = df.replace(-9999, np.NaN)

        return df


class PointDataCollection:
    """
    This could be a collection of profiles
    """
    DATA_CLASS = SnowExPointData

    def __init__(self, series: List[SnowExPointData], metadata: ProfileMetaData):
        self._series = series
        self._metadata = metadata

    @property
    def metadata(self) -> ProfileMetaData:
        return self._metadata

    @property
    def series(self) -> List[SnowExPointData]:
        return self._series

    @classmethod
    def _read_csv(
        cls, fname, meta_parser: PointSnowExMetadataParser,
        timezone=None, row_based_timezone=False
    ) -> List[SnowExPointData]:
        """
        Args:
            fname: path to csv
            meta_parser: parser for the metadata
            timezone: input timezone
            row_based_timezone: is the timezone row based?

        Returns:
            a list of ProfileData objects

        """
        # parse the file for metadata before parsing the individual
        # variables
        all_file = cls.DATA_CLASS(
            variable=None,  # we do not have a variable yet
            meta_parser=meta_parser,
            timezone=timezone, row_based_timezone=row_based_timezone
        )
        all_file.from_csv(fname)

        result = []

        shared_column_options = [
            # TODO: could we make this a 'shared' option in the definition
            meta_parser.primary_variables.entries["INSTRUMENT"],
            meta_parser.primary_variables.entries["DATE"],
            meta_parser.primary_variables.entries["TIME"],
            meta_parser.primary_variables.entries["DATETIME"],
            meta_parser.primary_variables.entries["UTCDOY"],
            meta_parser.primary_variables.entries["UTCTOD"],
            meta_parser.primary_variables.entries["UTCYEAR"],
            meta_parser.primary_variables.entries["LATITUDE"],
            meta_parser.primary_variables.entries["LONGITUDE"],
            meta_parser.primary_variables.entries["EASTING"],
            meta_parser.primary_variables.entries["NORTHING"],
            meta_parser.primary_variables.entries["ELEVATION"],
            meta_parser.primary_variables.entries["INSTRUMENT_MODEL"],
            meta_parser.primary_variables.entries["UTM_ZONE"],
            meta_parser.primary_variables.entries["NAME"],
            meta_parser.primary_variables.entries["CAMPAIGN"],
            meta_parser.primary_variables.entries["COMMENTS"],
        ]

        shared_columns = [
            c for c, v in all_file.meta_columns_map.items()
            if v in shared_column_options
        ]
        variable_columns = [
            c for c in all_file.meta_columns_map.keys() if c not in shared_columns
        ]

        # Create an object for each measurement
        for column in variable_columns:
            points = cls.DATA_CLASS(
                variable=all_file.meta_columns_map[column],
                meta_parser=meta_parser
            )
            # IMPORTANT - Metadata needs to be set before assigning the
            # dataframe as information from the metadata is used to format_df
            # the information
            points.metadata = all_file.metadata
            points.df = all_file.df.loc[:, shared_columns + [column]].copy()
            # --------
            result.append(points)

        return result

    @classmethod
    def from_csv(
        cls, fname, timezone="US/Mountain", header_sep=",", site_id=None,
        campaign_name=None, allow_map_failure=False, units_map=None,
        row_based_timezone=False,
        metadata_variable_files=None,
        primary_variable_files=None,
    ):
        """
        Find all variables in a single csv file
        Args:
            fname: path to file
            timezone: expected timezone in file
            header_sep: header sep in the file
            site_id: Site id override for the metadata
            campaign_name: Campaign.name override for the metadata
            allow_map_failure: allow metadata and column unknowns
            units_map: units map for the metadata
            row_based_timezone: is the timezone row based
            metadata_variable_files: list of files to override the metadata
                variables
            primary_variable_files: list of files to override the
                primary variables

        Returns:
            This class with a collection of profiles and metadata
        """
        # parse multiple files and create an iterable of ProfileData
        meta_parser = PointSnowExMetadataParser(
            timezone, primary_variable_files, metadata_variable_files,
            header_sep=header_sep, _id=site_id,
            campaign_name=campaign_name, allow_map_failures=allow_map_failure,
            units_map=units_map,
        )

        # read in the actual data
        profiles, metadata = cls._read_csv(
            fname, meta_parser,
            timezone=timezone, row_based_timezone=row_based_timezone
        )
        # ignore profiles with the name 'ignore'
        profiles = [
            p for p in profiles if
            # Keep the profile if it is None because we need the metadata
            (p.variable is None or p.variable.code != "ignore")
        ]

        return cls(profiles, metadata)
