import logging
from pathlib import Path
from typing import List
from timezonefinder import TimezoneFinder
import numpy as np
import pandas as pd
import geopandas as gpd
from insitupy.campaigns.snowex import SnowExProfileData
from insitupy.io.dates import DateManager
from insitupy.io.locations import LocationManager
from insitupy.io.yaml_codes import YamlCodes

from insitupy.profiles.base import MeasurementData
from insitupy.profiles.metadata import ProfileMetaData
from insitupy.variables import MeasurementDescription, ExtendableVariables

from .point_metadata import PointSnowExMetadataParser

LOG = logging.getLogger(__name__)


class SnowExPointData(MeasurementData):
    OUT_TIMEZONE = "UTC"
    DEFAULT_METADATA_VARIABLE_FILES = SnowExProfileData.DEFAULT_METADATA_VARIABLE_FILES
    DEFAULT_PRIMARY_VARIABLE_FILES = MeasurementData.DEFAULT_PRIMARY_VARIABLE_FILES + [
        Path(__file__).parent.joinpath(
            "./point_primary_variable_overrides.yaml"
        )
    ]

    def __init__(
        self, input_df: pd.DataFrame, metadata: ProfileMetaData,
        variable: MeasurementDescription,
        original_file=None, meta_parser=None, allow_map_failure=False,
        row_based_timezone=False,
        timezone=None
    ):
        """
        Take df of layered data (SMP, pit, etc)
        Args:
            input_df: dataframe of data
                Should include depth and optional bottom depth
                Should include sample or sample_a, sample_b, etc
            metadata: ProfileMetaData object
            variable: description of variable
            original_file: optional track original file
            meta_parser: MetaDataParser object. This will hold our variables
                map and units map
            allow_map_failures: if a mapping fails, warn us and use the
                original string (default False)
            row_based_timezone: does each row have a unique timezone implied
            timezone: input timezone for the whole file

        """
        self._row_based_timezone = row_based_timezone
        self._in_timezone = timezone
        super().__init__(
            input_df, metadata, variable,
            original_file=original_file,
            meta_parser=meta_parser,
            allow_map_failure=allow_map_failure
        )

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
            # TODO: do we have to look it up?
            # TODO: Look up the timezone for the location and apply that
            tz = None

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
                datetime = DateManager.handle_separate_datetime(row)

            result = DateManager.adjust_timezone(
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
        cls, fname, columns, column_mapping, header_pos,
        metadata: ProfileMetaData, meta_parser: PointSnowExMetadataParser,
        timezone=None, row_based_timezone=False
    ) -> List[SnowExPointData]:
        """
        Args:
            fname: path to csv
            columns: columns for dataframe
            column_mapping: mapping of column name to variable description
            header_pos: skiprows for pd.read_csv
            metadata: metadata for each object
            meta_parser: parser for the metadata
            timezone: input timezone
            row_based_timezone: is the timezone row based?

        Returns:
            a list of ProfileData objects

        """
        result = []
        if columns is None and header_pos is None:
            LOG.warning(f"File {fname} is empty of rows")
            df = pd.DataFrame()
        else:
            df = cls.DATA_CLASS.read_csv_dataframe(
                fname, columns, header_pos,
            )

        shared_column_options = [
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
            meta_parser.primary_variables.entries["UTM_ZONE"]
        ]

        shared_columns = [
            c for c, v in column_mapping.items()
            if v in shared_column_options
        ]
        variable_columns = [
            c for c in column_mapping.keys() if c not in shared_columns
        ]

        # Create an object for each measurement
        for column in variable_columns:
            target_df = df.loc[:, shared_columns + [column]]
            result.append(cls.DATA_CLASS(
                target_df, metadata,
                column_mapping[column],  # variable is a MeasurementDescription
                original_file=fname,
                meta_parser=meta_parser,
                timezone=timezone, row_based_timezone=row_based_timezone
            ))

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
        primary_variables = ExtendableVariables(
            primary_variable_files or cls.DATA_CLASS.DEFAULT_PRIMARY_VARIABLE_FILES
        )
        metadata_variables = ExtendableVariables(
            metadata_variable_files or cls.DATA_CLASS.DEFAULT_METADATA_VARIABLE_FILES,
        )
        # parse multiple files and create an iterable of ProfileData
        meta_parser = PointSnowExMetadataParser(
            fname, timezone, primary_variables, metadata_variables,
            header_sep=header_sep, _id=site_id,
            campaign_name=campaign_name, allow_map_failures=allow_map_failure,
            units_map=units_map
        )
        # Parse the metadata and column info
        metadata, columns, columns_map, header_pos = meta_parser.parse()
        # read in the actual data
        profiles = cls._read_csv(
            fname, columns, columns_map, header_pos, metadata,
            meta_parser,
            timezone=timezone, row_based_timezone=row_based_timezone
        )

        # ignore profiles with the name 'ignore'
        profiles = [
            p for p in profiles if
            # Keep the profile if it is None because we need the metadata
            (p.variable is None or p.variable.code != "ignore")
        ]

        return cls(profiles, metadata)
