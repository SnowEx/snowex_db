from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site, Campaign, Instrument, \
    MeasurementType

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestSSAProfile(TableTestBase, WithUploadedFile):
    """
    Test that all profiles from an SSA file are uploaded correctly
    """

    kwargs = {'in_timezone': 'MST', 'doi': 'SSA DOI', 'instrument': 'ice cube'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("SSA.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 40, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "ice cube"),
            (
                MeasurementType, "name",
                ['sample_signal', 'reflectance', 'specific_surface_area',
                 'equivalent_diameter']
            ),
            (MeasurementType, "units", ['mv', '%', 'm^2/kg', 'mm']),
            (MeasurementType, "derived", [False] * 4)
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, "
        "filter_value, expected",
        [
            ('reflectance', 'value', 'depth', 10, [22.12]),
            ('specific_surface_area', 'value', 'depth', 35, [11.2]),
            ('equivalent_diameter', 'value', 'depth', 80, [0.1054]),
            ('sample_signal', 'value', 'depth', 10, [186.9]),
            ('sample_signal', 'comments', 'depth', 5, ["brush"])
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
            ("reflectance", 16),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("reflectance", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
