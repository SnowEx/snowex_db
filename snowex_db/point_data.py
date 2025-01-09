import logging
from typing import List

import numpy as np
import pandas as pd
import geopandas as gpd

from .metadata import ExtendedSnowExMetadataParser
from insitupy.profiles.base import MeasurementData
from insitupy.profiles.metadata import ProfileMetaData

from .point_metadata import PointSnowExMetadataParser

LOG = logging.getLogger(__name__)


class SnowExPointData(MeasurementData):
    META_PARSER = PointSnowExMetadataParser

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

    def _format_df(self, input_df):
        """
        Format the incoming df with the column headers and other info we want
        This will filter to a single measurement as well as the expected
        shared columns like depth
        """
        self._set_column_mappings(input_df)

        # Verify the sample column exists and rename to variable
        df = self._check_sample_columns(input_df)

        n_entries = len(df)
        df["datetime"] = [self._dt] * n_entries

        # parse the location
        lat, lon = self.latlon
        location = gpd.points_from_xy(
            [lon] * n_entries, [lat] * n_entries
        )

        df = gpd.GeoDataFrame(
            df, geometry=location
        ).set_crs("EPSG:4326")
        df = df.replace(-9999, np.NaN)

        return df

    @classmethod
    def shared_column_options(cls):
        variables_class = cls.META_PARSER.PRIMARY_VARIABLES_CLASS
        return [
            variables_class.INSTRUMENT, variables_class.DATE,
            variables_class.TIME, variables_class.DATETIME,
            variables_class.LATITUDE, variables_class.LONGITUDE,
            variables_class.EASTING, variables_class.NORTHING,
            variables_class.ELEVATION,
            variables_class.INSTRUMENT_MODEL,
        ]


class PointDataCollection:
    """
    This could be a collection of profiles
    """
    META_PARSER = PointSnowExMetadataParser
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
        metadata: ProfileMetaData, units_map
    ) -> List[SnowExPointData]:
        """
        Args:
            fname: path to csv
            columns: columns for dataframe
            column_mapping: mapping of column name to variable description
            header_pos: skiprows for pd.read_csv
            metadata: metadata for each object
            units_map: map of column name to unit

        Returns:
            a list of ProfileData objects

        """
        result = []
        if columns is None and header_pos is None:
            LOG.warning(f"File {fname} is empty of rows")
            df = pd.DataFrame()
        else:
            df = cls.DATA_CLASS.read_csv_dataframe(
                fname, columns, header_pos
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
            ))
        if not result and df.empty:
            # Add one profile if this is empty, so we can
            # keep the metadata
            result = [
                cls.DATA_CLASS(
                    df, metadata,
                    None,
                    # variable is a MeasurementDescription
                    original_file=fname,
                    units_map=units_map
                )
            ]

        return result

    @classmethod
    def from_csv(
        cls, fname, timezone="US/Mountain", header_sep=",", site_id=None,
        campaign_name=None, allow_map_failure=False, units_map=None
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

        Returns:
            This class with a collection of profiles and metadata
        """
        # TODO: DRY up?
        # parse mlutiple files and create an iterable of ProfileData
        meta_parser = cls.META_PARSER(
            fname, timezone, header_sep=header_sep, _id=site_id,
            campaign_name=campaign_name, allow_map_failures=allow_map_failure,
            units_map=units_map
        )
        # Parse the metadata and column info
        metadata, columns, columns_map, header_pos = meta_parser.parse()
        # read in the actual data
        profiles = cls._read_csv(
            fname, columns, columns_map, header_pos, metadata,
            meta_parser.units_map
        )

        # ignore profiles with the name 'ignore'
        profiles = [
            p for p in profiles if
            # Keep the profile if it is None because we need the metadata
            (p.variable is None or p.variable.code != "ignore")
        ]

        return cls(profiles, metadata)
