from datetime import date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

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
        "campaign_name": "Grand Mesa",
        "name": "example_point_name"
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            session, str(data_dir.joinpath("depths.csv")),
        )

    @pytest.mark.usefixtures("uploaded_file")
    def test_measurement_type(self):
        record = self.get_records(MeasurementType, "name", "depth")
        assert len(record) == 1
        record = record[0]
        assert record.units == 'cm'
        assert record.derived is False

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, model", [
            ("mesa", "Mesa2_1"),
            ("magnaprobe", "CRREL_B"),
            ("pit ruler", None),
        ]
    )
    def test_instrument(self, name, model):
        record = self.get_records(Instrument, "name", name)
        assert len(record) == 1
        record = record[0]
        assert record.model == model

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, count, instrument_name",
        [
            ("example_point_name magnaprobe CRREL_B", 1, 'magnaprobe'),
            ("example_point_name mesa Mesa2_1", 1, 'mesa'),
            # We have three different dates
            ("example_point_name pit ruler", 3, 'pit ruler')
        ],
    )
    def test_campaign_observation_record(self, name, count, instrument_name):
        names = self.get_records(CampaignObservation, "name", name)
        assert len(names) == count

        # Check relationship mapping
        for record in names:
            assert record.campaign.name == "Grand Mesa"
            assert record.doi.doi == "some_point_doi"
            assert record.observer.name == 'unknown'
            assert record.instrument.name == instrument_name

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "date, count",
        [
            (date(2020, 1, 28), 1),
            (date(2020, 2, 4), 1),
            (date(2020, 2, 11), 1),
            (date(2020, 1, 30), 1),
            (date(2020, 2, 5), 1),
        ],
    )
    def test_point_observation(self, date, count):
        record = self.get_records(PointObservation, "date", date)
        assert len(record) == count

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "some_point_doi"),
            (PointData, "geom",
                WKTElement('POINT (-108.13515 39.03045)', srid=4326)
            ),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "value", [94, 74, 90, 117, 110, 68, 72, 89]
    )
    def test_value(self, value):
        records = self.check_value(
            'depth', 'value', 'value', value, [value]
        )
        assert records[0].measurement_type.name == "depth"

    @pytest.mark.usefixtures("uploaded_file")
    def test_record_count(self):
        """
        Check that all entries in the CSV made it into the database
        """
        assert self.check_count("depth") == 10
