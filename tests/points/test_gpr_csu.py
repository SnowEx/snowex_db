from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV

from _base import PointBaseTesting


class TestCSUGPR(PointBaseTesting):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """
    kwargs = {
        # Constant Metadata for the GPR data
        'campaign_name': 'Grand Mesa',
        'observer': 'Randall Bonnell',
        'instrument': 'gpr',
        'instrument_model': 'pulse EKKO Pro multi-polarization 1 GHz GPR',
        'timezone': 'UTC',
        'doi': 'https://doi.org/10.5067/S5EGFLCIAB18',
        'name': 'CSU GPR Data',
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(session, str(data_dir.joinpath("csu_gpr.csv")))

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
            (Instrument, "model", "pulse EKKO Pro multi-polarization 1 GHz GPR"),
            (MeasurementType, "name", ['two_way_travel', 'depth', "swe"]),
            (MeasurementType, "units", ['ns', 'cm', 'mm']),
            (MeasurementType, "derived", [False, False, False]),
            (DOI, "doi", "https://doi.org/10.5067/S5EGFLCIAB18"),
            (CampaignObservation, "name", "CSU GPR Data_gpr_pulse EKKO Pro multi-polarization 1 GHz GPR_two_way_travel"),
            (PointData, "geom",
                WKTElement('POINT (-108.176474337253 39.0192013494439)', srid=4326)
             ),
            (PointObservation, "date", date(2020, 2, 6)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('two_way_travel', 'value', 'date', date(2020, 2, 6), [
                7.348628, 7.225466, 7.102305, 6.979144, 6.814929
            ]),
            ('depth', 'value', 'date', date(2020, 2, 6), [
                89.5675335280758, 88.0663939188338, 86.5652664979259,
                85.0641390770181, 83.0626317863629
            ],),
            ('swe', 'value', 'date', date(2020, 2, 6), [
                244.519366531647, 240.421255398416, 236.323177539338,
                232.225099680259, 226.760984776771
            ]),
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
            ("depth", 5),
            ("swe", 5),
            ("two_way_travel", 5),
            ("density", 0),  # no measurements
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("depth", "value", 5),
            ("swe", "value", 5),
            ("swe", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
