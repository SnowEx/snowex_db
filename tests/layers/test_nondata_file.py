from datetime import datetime, timezone

import numpy as np
import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site, Campaign, Observer

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestEmptyProfile(TableTestBase, WithUploadedFile):
    """
    Test that a file with header info that doesn't have data (which
    happens on purpose) doesn't upload anything
    """

    args = []
    kwargs = {
        'timezone': 'MST',
        'doi': 'no_doi',
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath('empty_data.csv')), session=session
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("hand_hardness", 0),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected


class TestMetadata(TableTestBase, WithUploadedFile):
    """
    Test the large amount of metadata we get from the
    site details file
    """
    kwargs = {
        'timezone': 'MST',
        'doi': 'no_doi',
    }
    UploaderClass = UploadProfileData

    @pytest.fixture
    def uploaded_site_details_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath("site_details.csv")), session=session
        )

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (
                Site, "datetime",
                datetime(2020, 2, 5, 20, 30, tzinfo=timezone.utc),
            ),
            (
                Site, "geom",
                WKTElement(
                    'POINT (-108.1894813320662 39.031261970372725)', srid=4326
                ),
            ),
            (Campaign, "name", "Grand Mesa"),
            (Site, "aspect", 180.0),
            (Site, "slope_angle", 5.0),
            (Site, "air_temp", np.nan),
            (Site, "total_depth", 35.0),
            (Site, "weather_description", "Sunny, cold, gusts"),
            (Site, "precip", "None"),
            (Site, "sky_cover", "Few (< 1/4 of sky)"),
            (Site, "wind", "Moderate"),
            (Site, "ground_condition", "Frozen"),
            (Site, "ground_roughness", "rough, rocks in places"),
            (Site, "ground_vegetation", "[Grass]"),
            (Site, "vegetation_height", "5, nan"),
            (Site, "tree_canopy", "No Trees"),
            (Site, "site_notes", None),
            (Observer, "name", ["Chris Hiemstra", "Hans Lievens"]),
        ]
    )
    def test_metadata(
        self, uploaded_site_details_file, table, attribute,
        expected_value
    ):
        self._check_metadata(table, attribute, expected_value)
