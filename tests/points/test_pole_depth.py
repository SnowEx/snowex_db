from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV

from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestPollDepth(TableTestBase, WithUploadedFile):
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
    def test_measurement_type(self):
        record = self.get_records(MeasurementType, "name", "depth")
        assert len(record) == 1
        record = record[0]
        assert record.units == 'cm'
        assert record.derived is False

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize("model", ["W1B", "E9B", "E8A", "E6A"])
    def test_instrument(self, model):
        record = self.get_records(Instrument, "model", model)
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
    def test_point_observation(self, date):
        record = self.get_records(PointObservation, "date", date)
        assert len(record) == 1

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, count",
        [
            ("example_pole_point_name camera E6A", 4),
            ("example_pole_point_name camera E8A", 3),
            ("example_pole_point_name camera E9B", 4),
            ("example_pole_point_name camera W1B", 3),
        ],
    )
    def test_campaign_observation(self, name, count):
        names = self.get_records(CampaignObservation, "name", name)
        assert len(names) == count

        # Check relationships
        for record in names:
            assert record.campaign.name == "Grand Mesa"
            assert record.doi.doi == "some_point_doi_poles"
            assert record.observer.name == 'unknown'
            assert record.instrument.name == "camera"

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
        "measurement_type, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'date', date(2020, 2, 1), [101.2728]),
            ('depth', 'datetime', 'date', date(2020, 2, 1), [datetime(2020, 2, 1, 20, 0, tzinfo=timezone.utc)]),
        ]
    )
    def test_value(
            self, measurement_type, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        records = self.check_value(
            measurement_type, attribute_to_check,
            filter_attribute, filter_value, expected,
        )
        assert records[0].measurement_type.name == measurement_type

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("depth", 14)
        ]
    )
    def test_record_count(self, data_name, expected, uploaded_file):
        """
        Check that all entries in the CSV made it into the database
        """
        assert self.check_count(data_name) == expected
