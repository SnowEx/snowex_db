from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site, Campaign, Instrument

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestStratigraphyProfile(TableTestBase, WithUploadedFile):
    """
    Test that all the profiles from the Stratigraphy file were uploaded and
    have integrity
    """

    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("stratigraphy.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 30, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", None),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check,"
        " filter_attribute, filter_value, expected",
        [
            ('hand_hardness', 'value', 'depth', 30, ["4F"]),
            ('grain_size', 'value', 'depth', 35, ["< 1 mm"]),
            ('grain_type', 'value', 'depth', 17, ["FC"]),
            ('manual_wetness', 'value', 'depth', 17, ["D"]),
            ('hand_hardness', 'comments', 'depth', 17, ["Cups"]),
        ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("hand_hardness", 5)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("hand_hardness", "site_id", 1),
            ("manual_wetness", "value", 1),
            ("hand_hardness", "value", 3),
            ("grain_type", "value", 2),
            ("grain_size", "value", 2),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
