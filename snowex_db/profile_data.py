from insitupy.campaigns.campaign import ProfileDataCollection
from insitupy.campaigns.snowex import SnowExProfileData
from .metadata import ExtendedSnowExMetadataParser


class ExtendedSnowexProfileData(SnowExProfileData):
    META_PARSER = ExtendedSnowExMetadataParser

    @classmethod
    def shared_column_options(cls):
        return cls.depth_columns() + [
            cls.META_PARSER.PRIMARY_VARIABLES_CLASS.COMMENTS
        ]


class SnowExProfileDataCollection(ProfileDataCollection):
    META_PARSER = ExtendedSnowExMetadataParser
    PROFILE_DATA_CLASS = ExtendedSnowexProfileData
