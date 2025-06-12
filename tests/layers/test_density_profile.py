from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site, Campaign, Instrument, \
    MeasurementType

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestDensityProfile(TableTestBase, WithUploadedFile):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """

    kwargs = {
        'timezone': 'MST',
        'instrument': 'kelly cutter',
        'doi': "somedoi",
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("density.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (
                    Site, "datetime", datetime(
                    2020, 2, 5, 20, 30, tzinfo=timezone.utc
                ),
            ),
            (
                    Site, "geom", WKTElement(
                    'POINT (-108.1894813320662 39.031261970372725)', srid=4326
                ),
            ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "kelly cutter"),
            (MeasurementType, "name", ['density']),
            (MeasurementType, "units", ['kg/m3']),
            (MeasurementType, "derived", [False]),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected",
        [
            ('density', 'value', 'depth', 35, [190, 245, 'None']),
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
            ("density", 12),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("density", "site_id", 1),
        ]
    )
    def test_unique_count(
        self, data_name, attribute_to_count, expected, uploaded_file
    ):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestDensityAlaska(TableTestBase, WithUploadedFile):
    """
    Test that the logic carries over to Alaska data
    """
    kwargs = {
        'timezone': 'US/Alaska',
        'header_sep': ':',
        'doi': 'DOI-1234321',
        'instrument': 'density cutter'
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(
            str(
                data_dir.joinpath(
                    "SnowEx23_SnowPits_AKIOP_454SB_20230316_density_v01.csv"
                )
            )
        )

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "SB454"),
            (
                    Site, "datetime", datetime(
                    2023, 3, 16, 18, 25, tzinfo=timezone.utc
                ),
            ),
            (
                    Site, "geom", WKTElement(
                    'POINT (-148.31829 64.70955)', srid=4326
                ),
            ),
            (Campaign, "name", "Fairbanks"),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("density", 15),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected
