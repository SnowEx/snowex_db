from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV

from _base import PointBaseTesting


class TestUNMGPR(PointBaseTesting):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """
    kwargs = {
        # Constant Metadata for the GPR data
        'observer': 'Ryan Webb',
        'doi': 'https://doi.org/10.5067/WE9GI1GVMQF6',
        'campaign_name': 'Grand Mesa',
        'instrument': 'gpr',
        'instrument_model': f'Mala 800 MHz GPR',
        'timezone': 'UTC',
        'name': 'UNM GPR Data',
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(session, str(data_dir.joinpath("unm_gpr.csv")))

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
            (Instrument, "model", "Mala 800 MHz GPR"),
            (MeasurementType, "name", ['two_way_travel', 'depth', "swe"]),
            (MeasurementType, "units", ['ns', 'cm', 'mm']),
            (MeasurementType, "derived", [False, False, False]),
            (DOI, "doi", "https://doi.org/10.5067/WE9GI1GVMQF6"),
            (CampaignObservation, "name", "UNM GPR Data_gpr_Mala 800 MHz GPR_two_way_travel"),
            (PointData, "geom",
                WKTElement('POINT (-108.1340183 39.0296597)', srid=4326)
             ),
            (PointObservation, "date", date(2020, 1, 28)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('two_way_travel', 'value', 'date', date(2020, 1, 28), [8.97]),
            ('depth', 'value', 'date', date(2020, 1, 29), [0.9],),
            ('swe', 'value', 'date', date(2020, 1, 31), [291]),
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
            ("depth", 4),
            ("swe", 4),
            ("two_way_travel", 4),
            ("density", 0),  # no measurements
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("depth", "value", 4),
            ("swe", "value", 4),
            ("swe", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
