from insitupy.campaigns.campaigns import ProfileDataCollection
from insitupy.campaigns.snowex import SnowExProfileData
from .metadata import ExtendedSnowExMetadataParser


class SnowExProfileDataCollection(ProfileDataCollection):
    META_PARSER = ExtendedSnowExMetadataParser
    PROFILE_DATA_CLASS = SnowExProfileData
