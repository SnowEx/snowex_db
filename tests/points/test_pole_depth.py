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
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath("pole_depths.csv")), session=session
        )

    @pytest.mark.usefixtures("uploaded_file")
    def test_measurement_type(self, session):
        record = self.get_records(session, MeasurementType, "name", "depth")
        assert len(record) == 1
        record = record[0]
        assert record.units == 'cm'
        assert record.derived is False

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize("model", ["W1B", "E9B", "E8A", "E6A"])
    def test_instrument(self, model, session):
        record = self.get_records(session, Instrument, "model", model)
        assert len(record) == 1
        record = record[0]
        assert record.name == "camera"

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "date",
        [
            date(2019, 11, 27),
            date(2019, 12, 7),
            date(2019, 12, 31),
            date(2020, 2, 1),
            date(2019, 10, 28),
            date(2019, 11, 28),
            date(2019, 12, 14),
            date(2019, 11, 29),
            date(2020, 2, 27),
            date(2020, 4, 7),
            date(2020, 5, 22),
            date(2020, 1, 27),
            date(2020, 3, 14),
            date(2020, 5, 3),
        ],
    )
    def test_point_observation(self, date, session):
        record = self.get_records(session, PointObservation, "date", date)
        assert len(record) == 1

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, count",
        [
            ("example_pole_point_name_camera_E6A_depth", 4),
            ("example_pole_point_name_camera_E8A_depth", 3),
            ("example_pole_point_name_camera_E9B_depth", 4),
            ("example_pole_point_name_camera_W1B_depth", 3),
        ],
    )
    def test_campaign_observation(self, name, count, session):
        names = self.get_records(session, CampaignObservation, "name", name)
        assert len(names) == count

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "some_point_doi_poles"),
            (PointData, "geom",
                WKTElement('POINT (-108.184794 39.008078)', srid=4326)
             ),
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
