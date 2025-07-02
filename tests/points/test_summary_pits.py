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
            "comments": "unknown"
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
            (Campaign, "name", "American River Basin"),
            (Instrument, "name", "cutter"),
            (Instrument, "model", None),
            (MeasurementType, "name", ['density', 'swe', 'depth']),
            (MeasurementType, "units", ['kg/m^3', 'mm', 'cm']),
            (MeasurementType, "derived", [True, True, True]),
            (DOI, "doi", "some_point_pit_doi"),
            (CampaignObservation, "name", "CAAMCL_20191220_1300_cutter_density"),
            (PointData, "geom",
                WKTElement('POINT (-120.04186927254749 38.71033054555811)', srid=4326)
             ),
            (PointObservation, "date", date(2019, 12, 20)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'value', 117.0, [117.0]),
            ('depth', 'units', 'value', 117.0, ['cm']),
            ('depth', 'datetime', 'value', 117.0, [datetime(2020, 2, 21, 20, 00, tzinfo=timezone.utc)]),
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
            ("depth", 12)
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
        """
        Test number of unique measurement types
        """
        with db_session_with_credentials() as (engine, session):
            records = session.query(
                MeasurementType.name
            ).distinct().all()
            assert len(records) == 3
