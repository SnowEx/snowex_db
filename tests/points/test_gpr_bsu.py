from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV

from _base import PointBaseTesting


class TestGPR(PointBaseTesting):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """
    kwargs = {
        'timezone': "UTC",
        'doi': "some_gpr_point_doi",
        "campaign_name": "Grand Mesa",
        "name": "BSU GPR DATA",
        "instrument": "gpr"
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("bsu_gpr.csv")))

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
            (Instrument, "name", "gpr"),
            (Instrument, "model", None),
            (MeasurementType, "name", ['two_way_travel', 'depth', "swe"]),
            (MeasurementType, "units", ['ns', 'cm', 'mm']),
            (MeasurementType, "derived", [False, False, False]),
            (DOI, "doi", "some_gpr_point_doi"),
            (CampaignObservation, "name", "BSU GPR DATA_gpr_two_way_travel"),
            (PointData, "geom",
                WKTElement('POINT (-108.190889311605 39.0343743775669)', srid=4326)
             ),
            (PointObservation, "date", date(2020, 1, 28)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('two_way_travel', 'value', 'date', date(2020, 1, 28), [8.3] * 8),
            ('depth', 'value', 'date', date(2020, 1, 28),
             [101.096735522092, 101.096735522092, 101.096735522092, 101.096735522092, 101.096735522092, 101.096735522092, 101.096735522092, 101.096735522092]),
            ('swe', 'value', 'date', date(2020, 1, 28),
             [275.994087975311, 275.994087975311, 275.994087975311, 275.994087975311, 275.994087975311, 275.994087975311, 275.994087975311, 275.994087975311]),
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
            ("depth", 12),
            ("swe", 12),
            ("two_way_travel", 12),
            ("density", 0),  # no measurements
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("depth", "value", 2),
            ("swe", "value", 2),
            ("swe", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
