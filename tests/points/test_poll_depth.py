from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV

from _base import PointBaseTesting


class TestPollDepth(PointBaseTesting):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """

    kwargs = {
        'timezone': 'MST',
        'doi': "some_point_doi_poles",
        "campaign_name": "Grand Mesa",
        "name": "example_pole_point_name",
        "instrument": "camera",
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("pole_depths.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "camera"),
            (Instrument, "model", "E6A"),
            (MeasurementType, "name", ['depth']),
            (MeasurementType, "units", ['cm']),
            (MeasurementType, "derived", [False]),
            (DOI, "doi", "some_point_doi_poles"),
            (CampaignObservation, "name", "example_pole_point_name_cameraE6A_depth"),
            (PointData, "geom",
                WKTElement('POINT (-108.184794 39.008078)', srid=4326)
             ),
            (PointObservation, "date", date(2019, 11, 27)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'date', date(2020, 2, 1), [101.2728]),
            ('depth', 'units', 'date', date(2020, 2, 1), ['cm']),
            ('depth', 'datetime', 'date', date(2020, 2, 1), [datetime(2020, 2, 1, 20, 0, tzinfo=timezone.utc)]),
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
            ("depth", 14)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("depth", "value", 14),
            ("depth", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
