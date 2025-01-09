import logging
from typing import List

import pandas as pd
from pathlib import Path

from .metadata import ExtendedSnowExMetadataParser
from insitupy.variables import MeasurementDescription
from insitupy.profiles.base import MeasurementData
from insitupy.profiles.metadata import ProfileMetaData

LOG = logging.getLogger(__name__)


class SnowExPointData(MeasurementData):
    META_PARSER = ExtendedSnowExMetadataParser

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


class PointDataCollection:
    """
    This could be a collection of profiles
    """
    META_PARSER = ExtendedSnowExMetadataParser
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

        variable_columns = list(column_mapping.keys())

        # Create an object for each measurement
        for column in variable_columns:
            target_df = df.loc[:, [column]]
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
        campaign_name=None, allow_map_failure=False
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
            campaign_name=campaign_name, allow_map_failures=allow_map_failure
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
