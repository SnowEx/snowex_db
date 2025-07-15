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

    def __init__(
        self,
        variable: MeasurementDescription,
        meta_parser: MetaDataParser
    ):
        # Tricky, this needs to happen before super init
        self._comments_column = meta_parser.primary_variables.entries[
            "COMMENTS"]
        super().__init__(variable, meta_parser)

    def shared_column_options(self):
        return self._depth_columns + [self._comments_column]


class ExtendedSnowExProfileDataCollection(SnowExProfileDataCollection):
    PROFILE_DATA_CLASS = ExtendedSnowexProfileData
