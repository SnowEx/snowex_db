from insitupy.campaigns.snowex import SnowExProfileData, SnowExProfileDataCollection

from .metadata import ExtendedSnowExMetadataParser


class ExtendedSnowexProfileData(SnowExProfileData):
    META_PARSER = ExtendedSnowExMetadataParser


class ExtendedSnowExProfileDataCollection(SnowExProfileDataCollection):
    PROFILE_DATA_CLASS = ExtendedSnowexProfileData
