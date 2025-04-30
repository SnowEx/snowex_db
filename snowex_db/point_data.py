import logging
from typing import List

import numpy as np
import pandas as pd
import geopandas as gpd

from insitupy.profiles.base import MeasurementData
from insitupy.profiles.metadata import ProfileMetaData
from insitupy.variables import MeasurementDescription

from .point_metadata import PointSnowExMetadataParser

LOG = logging.getLogger(__name__)


class SnowExPointData(MeasurementData):
    META_PARSER = PointSnowExMetadataParser

    def __init__(
        self, input_df: pd.DataFrame, metadata: ProfileMetaData,
        variable: MeasurementDescription,
        original_file=None, units_map=None, allow_map_failure=False,
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
            units_map: optional dictionary of column name to unit
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
            units_map=units_map,
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
            encoding='latin'
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
        lat, lon, *_ = self.META_PARSER.parse_location_from_row(row)
        row["latitude"] = lat
        row["longitude"] = lon
        return row

    def _get_datetime(self, row):
        """
        fill in the datetime info for a row
        Args:
            row: pandas row
        """
        tz = self._in_timezone
        if self._row_based_timezone:
            # TODO: do we have to look it up?
            raise NotImplementedError("?")
        result = self.META_PARSER.datetime_from_row(row, tz)
        row["datetime"] = result
        return row

    @classmethod
    def _get_campaign(cls, row):
        """
        fill in the campaign info for a row
        Args:
            row: pandas row
        """
        result = cls.META_PARSER.parse_campaign_from_row(row)
        row["campaign"] = result
        return row

    def _format_df(self, input_df):
        """
        Format the incoming df with the column headers and other info we want
        This will filter to a single measurement as well as the expected
        shared columns like depth
        """
        self._set_column_mappings(input_df)

        # Verify the sample column exists and rename to variable
        df = self._check_sample_columns(input_df)

        df = df.apply(self._get_campaign, axis=1)
        # parse the location
        df = df.apply(self._get_location, axis=1)
        # Parse the datetime
        df = df.apply(self._get_datetime, axis=1)

        location = gpd.points_from_xy(
            df["longitude"], df["latitude"]
        )
        df = df.drop(columns=["longitude", "latitude"])

        df = gpd.GeoDataFrame(
            df, geometry=location
        ).set_crs("EPSG:4326")
        df = df.replace(-9999, np.NaN)

        return df

    @classmethod
    def shared_column_options(cls):
        """
        These are columns of interest, but not the primary variable
        of interest
        Returns:

        """
        variables_class = cls.META_PARSER.PRIMARY_VARIABLES_CLASS
        return [
            variables_class.INSTRUMENT, variables_class.DATE,
            variables_class.TIME, variables_class.DATETIME,
            variables_class.UTCDOY, variables_class.UTCTOD,
            variables_class.UTCYEAR,
            variables_class.LATITUDE, variables_class.LONGITUDE,
            variables_class.EASTING, variables_class.NORTHING,
            variables_class.ELEVATION,
            variables_class.INSTRUMENT_MODEL,
            variables_class.UTM_ZONE
        ]


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
        metadata: ProfileMetaData, units_map,
        timezone=None, row_based_timezone=False
    ) -> List[SnowExPointData]:
        """
        Args:
            fname: path to csv
            columns: columns for dataframe
            column_mapping: mapping of column name to variable description
            header_pos: skiprows for pd.read_csv
            metadata: metadata for each object
            units_map: map of column name to unit
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

        shared_column_options = cls.DATA_CLASS.shared_column_options()
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
                units_map=units_map,
                timezone=timezone, row_based_timezone=row_based_timezone
            ))

        return result

    @classmethod
    def from_csv(
        cls, fname, timezone="US/Mountain", header_sep=",", site_id=None,
        campaign_name=None, allow_map_failure=False, units_map=None,
        row_based_timezone=False
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
            row_based_timezone: is the timezone row based

        Returns:
            This class with a collection of profiles and metadata
        """
        # TODO: DRY up?
        # parse mlutiple files and create an iterable of ProfileData
        meta_parser = PointSnowExMetadataParser(
            fname, timezone, header_sep=header_sep, _id=site_id,
            campaign_name=campaign_name, allow_map_failures=allow_map_failure,
            units_map=units_map
        )
        # Parse the metadata and column info
        metadata, columns, columns_map, header_pos = meta_parser.parse()
        # read in the actual data
        profiles = cls._read_csv(
            fname, columns, columns_map, header_pos, metadata,
            meta_parser.units_map,
            timezone=timezone, row_based_timezone=row_based_timezone
        )

        # ignore profiles with the name 'ignore'
        profiles = [
            p for p in profiles if
            # Keep the profile if it is None because we need the metadata
            (p.variable is None or p.variable.code != "ignore")
        ]

        return cls(profiles, metadata)
