from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.db import db_session_with_credentials
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV
from tests.points._base import PointBaseTesting


class TestSummaryPits(PointBaseTesting):
    """
    Test the summary csvs for a collection of pits
    """

    kwargs = {
        'timezone': 'MST',
        'doi': "some_point_pit_doi",
        "row_based_timezone": True,  # row based timezone
        "derived": True,
        "instrument_map": {
            "depth": "manual",
            "swe": "manual",
            "density": "cutter",
        }
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        """
        NOTE - this is part of the _modified file that we create
        in the upload script, NOT the original file
        """
        self.upload_file(str(data_dir.joinpath("pit_summary_points.csv")))

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
            (MeasurementType, "derived", [True]),
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

    def test_unique_types(self, uploaded_file):
        with db_session_with_credentials() as (engine, session):
            records = session.query(PointObservation.measurement_type).unique()
            assert len(records) == 3
