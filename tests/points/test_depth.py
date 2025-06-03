from datetime import datetime, timezone, date

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
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("depths.csv")))

    def filter_measurement_type(self, session, measurement_type, query=None):
        if query is None:
            query = session.query(self.TableClass)

        query = query.join(
            self.TableClass.observation
        ).join(
            PointObservation.measurement_type
        ).filter(MeasurementType.name == measurement_type)
        return query

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "mesa"),
            (Instrument, "model", "Mesa2_1"),
            (MeasurementType, "name", ['depth']),
            (MeasurementType, "units", ['cm']),
            (MeasurementType, "derived", [False]),
            (DOI, "doi", "some_point_doi"),
            (CampaignObservation, "name", "example_point_name_M2Mesa2_1_depth"),
            (PointData, "geom",
                WKTElement('POINT (-108.13515 39.03045)', srid=4326)
             ),
            (PointObservation, "date", date(2020, 2, 4)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'id', 1, [94]),
            ('depth', 'units', 'id', 1, ['cm']),
            ('depth', 'datetime', 'id', 1, [datetime(2020, 1, 28, 18, 48, tzinfo=timezone.utc)]),
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
