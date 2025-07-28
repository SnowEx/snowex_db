from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV
from tests.points._base import PointBaseTesting


class TestDepth(PointBaseTesting):
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

    def filter_measurement_type(self, session, measurement_type, query=None):
        if query is None:
            query = session.query(self.TableClass)

        query = query.join(
            self.TableClass.observation
        ).join(
            PointObservation.measurement_type
        ).filter(MeasurementType.name == measurement_type)
        return query

    @pytest.mark.usefixtures("uploaded_file")
    def test_measurement_type(self, session):
        record = self.get_records(session, MeasurementType, "name", "depth")
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
    def test_instrument(self, name, model, session):
        record = self.get_records(session, Instrument, "name", name)
        assert len(record) == 1
        record = record[0]
        assert record.model == model

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, count",
        [
            ("example_point_name_magnaprobe_CRREL_B_depth", 1),
            ("example_point_name_mesa_Mesa2_1_depth", 1),
            # We have three different dates
            ("example_point_name_pit ruler_depth", 3)
        ],
    )
    def test_campaign_observation(self, name, count, session):
        names = self.get_records(session, CampaignObservation, "name", name)
        assert len(names) == count

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
    def test_point_observation(self, date, count, session):
        record = self.get_records(session, PointObservation, "date", date)
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
    
    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'value', 94.0, [94]),
            ('depth', 'units', 'value', 94.0, ['cm']),
            ('depth', 'datetime', 'value', 94.0, [datetime(2020, 1, 28, 18, 48, tzinfo=timezone.utc)]),
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
            ("depth", "value", 9),
            ("depth", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
