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
        super().__init__(variable, meta_parser)


class ExtendedSnowExProfileDataCollection(SnowExProfileDataCollection):
    PROFILE_DATA_CLASS = ExtendedSnowexProfileData
