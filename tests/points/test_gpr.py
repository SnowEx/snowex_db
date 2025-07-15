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
    gpr_dt = date(2019, 1, 28)
    kwargs = {
        'timezone': "UTC",
        'doi': "some_gpr_point_doi",
        "campaign_name": "Grand Mesa",
        "name": "GPR DATA",
        "instrument": "gpr"
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath("gpr.csv")), session=session
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

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "gpr"),
            (Instrument, "model", None),
            (MeasurementType, "name", ['two_way_travel', 'density', 'depth', "swe"]),
            (MeasurementType, "units", ['ns', 'kg/m^3', 'cm', 'mm']),
            (MeasurementType, "derived", [False, False, False, False]),
            (DOI, "doi", "some_gpr_point_doi"),
            (CampaignObservation, "name", "GPR DATA_gpr_two_way_travel"),
            (PointData, "geom",
                WKTElement('POINT (-108.19088935297108 39.03437443810879)', srid=4326)
             ),
            (PointObservation, "date", date(2019, 1, 28)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('two_way_travel', 'value', 'date', date(2019, 1, 28), [8.3, 10.0058518216919]),
            ('density', 'value', 'date', date(2019, 1, 28), [250.786035454008, 280.938399439763]),
            ('depth', 'value', 'date', date(2019, 1, 28),
             [102.662509421414, 121.213915188656]),
            ('swe', 'value', 'date', date(2019, 1, 28),
             [257.463237275561, 340.536433229281]),
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
            ("depth", 10),
            ("swe", 10),
            ("two_way_travel", 10),
            ("density", 10),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("depth", "value", 10),
            ("swe", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
