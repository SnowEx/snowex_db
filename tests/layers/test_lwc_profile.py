from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site, Campaign, Instrument, \
    MeasurementType

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestLWCProfile(TableTestBase, WithUploadedFile):
    """
    Test the permittivity file is uploaded correctly
    """

    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("LWC.csv")))

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
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('permittivity', 'value', 'depth', 27, [1.372, 1.35]),
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
            ("permittivity", 8)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("permittivity", "site_id", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestLWCProfileB(TableTestBase, WithUploadedFile):
    """
    Test the permittivity file is uploaded correctly
    """

    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("LWC2.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGMST_20200312"),
            (Site, "datetime", datetime(
                2020, 3, 12, 21, 45, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.06310597266031 39.04495658046074)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", None),
            (MeasurementType, "name", [
                'density', 'permittivity', 'liquid_water_content'
            ]),
            (MeasurementType, "units", ['kg/m3', None, '%']),
            (MeasurementType, "derived", [False] * 3)
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected",
        [
            ('permittivity', 'value', 'depth', 73, [1.507, 1.521]),
            ('liquid_water_content', 'value', 'depth', 15, [0.1, 0.0]),
            ('density', 'value', 'depth', 83, [164.5])
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
            ("permittivity", 16),
            ('liquid_water_content', 16),
            ('density', 8)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("permittivity", "site_id", 1),
            ("liquid_water_content", "site_id", 1),
            ("density", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
