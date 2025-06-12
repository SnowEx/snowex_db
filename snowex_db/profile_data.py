from pathlib import Path

import pandas as pd
from insitupy.campaigns.snowex import (
    SnowExProfileData, SnowExProfileDataCollection
)
from insitupy.io.metadata import MetaDataParser
from insitupy.profiles.metadata import ProfileMetaData
from insitupy.variables import MeasurementDescription

from .metadata import ExtendedSnowExMetadataParser


class ExtendedSnowexProfileData(SnowExProfileData):
    META_PARSER = ExtendedSnowExMetadataParser
    DEFAULT_METADATA_VARIABLE_FILES = (
        SnowExProfileData.DEFAULT_METADATA_VARIABLE_FILES
    ) + [
      Path(__file__).parent.joinpath(
          "./metadata_variable_overrides.yaml"
      )
    ]
    DEFAULT_PRIMARY_VARIABLE_FILES = (
        SnowExProfileData.DEFAULT_PRIMARY_VARIABLE_FILES) + [
        Path(__file__).parent.joinpath(
            "./profile_primary_variable_overrides.yaml"
        )
    ]

    def __init__(
        self, input_df: pd.DataFrame,
        metadata: ProfileMetaData,
        variable: MeasurementDescription,
        meta_parser: MetaDataParser, **kwargs
    ):
        # Tricky, this needs to happen before super init
        self._comments_column = meta_parser.primary_variables.entries[
            "COMMENTS"]
        super().__init__(input_df, metadata, variable, meta_parser, **kwargs)

    def shared_column_options(self):
        return self._depth_columns + [self._comments_column]


class ExtendedSnowExProfileDataCollection(SnowExProfileDataCollection):
    META_PARSER = ExtendedSnowExMetadataParser
    PROFILE_DATA_CLASS = ExtendedSnowexProfileData
