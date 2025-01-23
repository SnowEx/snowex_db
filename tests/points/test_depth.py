from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType

from snowex_db.upload.points import PointDataCSV
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestDepth(TableTestBase, WithUploadedFile):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """

    kwargs = {
        'timezone': 'MST',
        'doi': "some_point_doi",
        "campaign_name": "Grand Mesa"
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("depths.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "magnaprobe"),
            (MeasurementType, "name", ['depth']),
            (MeasurementType, "units", ['cm']),
            (MeasurementType, "derived", [False]),
            (DOI, "doi", "some_point_doi")
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'id', 1, [94]),
            (
                'depth', "datetime", 'id', 1, [datetime(
                    2020, 1, 28, 11, 48, tzinfo=timezone.utc)
                ]
             ),
            (
                'depth', "geom", 'id', 1, [WKTElement(
                    'POINT (-108.13516, 39.03045)', srid=4326)
                ]
            ),
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
            ("depth", 10)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("depth", "date", 5)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
