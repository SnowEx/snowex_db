from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site, Campaign, Instrument, \
    MeasurementType

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestTemperatureProfile(TableTestBase, WithUploadedFile):
    """
    Test that a temperature profile is uploaded to the DB correctly
    """

    kwargs = {'in_timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("temperature.csv")))

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
            (MeasurementType, "name", ['snow_temperature']),
            (MeasurementType, "units", ['deg c']),
            (MeasurementType, "derived", [False])
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected",
        [
            ('snow_temperature', 'value', 'depth', 10, [-5.9]),
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
            ("snow_temperature", 5),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("snow_temperature", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
