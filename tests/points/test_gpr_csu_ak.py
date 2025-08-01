from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV

from _base import PointBaseTesting


class TestCSUAKGPR(PointBaseTesting):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """
    kwargs = {
        # Constant Metadata for the GPR data
        'campaign_name': 'farmers-creamers',  # TODO: should this be AK-something?
        'observer': 'Randall Bonnell',
        'instrument': 'gpr',
        'instrument_model': 'pulseEkko pro 1 GHz GPR',
        'timezone': 'UTC',
        'doi': "preliminary_gpr_ak_farmers-creamers",  # TODO: presumably this exists now?
        'name': 'CSU GPR Data',
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(session, str(data_dir.joinpath("csu_ak_gpr.csv")))

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
            (Campaign, "name", "farmers-creamers"),
            (Instrument, "name", "gpr"),
            (Instrument, "model", "pulseEkko pro 1 GHz GPR"),
            (MeasurementType, "name", ['two_way_travel', 'depth', "swe", "density"]),
            (MeasurementType, "units", ['ns', 'cm', 'mm', "kg/m^3"]),
            (MeasurementType, "derived", [False, False, False, False]),
            (DOI, "doi", "preliminary_gpr_ak_farmers-creamers"),
            (CampaignObservation, "name", "CSU GPR Data_gpr_pulseEkko pro 1 GHz GPR_two_way_travel"),
            (PointData, "geom",
                WKTElement('POINT (-147.737230798003 64.8638112503524)', srid=4326)
             ),
            (PointObservation, "date", date(2023, 3, 7)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('two_way_travel', 'value', 'date', date(2023, 3, 7), [
                1.45, 1.487265, 1.524917, 1.55, 1.562569
            ]),
            ('depth', 'value', 'date', date(2023, 3, 7), [
                18.6201608827132, 19.098699017399, 19.5822068088169,
                19.9043099091073, 20.0657146002347
            ],),
            ('swe', 'value', 'date', date(2023, 3, 7), [
                36.8679185477722, 37.81542405445, 38.7727694814574,
                39.4105336200324, 39.7301149084648
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
            ("density", 5)
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
