from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestGPR(TableTestBase, WithUploadedFile):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """

    kwargs = {
        'timezone': "UTC",
        'doi': "some_gpr_point_doi",
        "campaign_name": "Grand Mesa",
        "name": "GPR DATA"
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("gpr.csv")))

    def filter_measurement_type(self, session, measurement_type, query=None):
        if query is None:
            query = session.query(self.TableClass)

        query = query.join(
            self.TableClass.observation
        ).join(
            PointObservation.measurement_type
        ).filter(MeasurementType.name == measurement_type)
        return query

    #             # Test the actual value of the dataset
    #             dict(data_name='two_way_travel', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
    #                  expected=8.3),
    #             dict(data_name='density', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
    #                  expected=250.786035454008),
    #             dict(data_name='depth', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
    #                  expected=102.662509421414),
    #             dict(data_name='swe', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
    #                  expected=257.463237275561),
    #             # Test our unit assignment
    #             dict(data_name='two_way_travel', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
    #                  expected='ns'),
    #             dict(data_name='density', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
    #                  expected='kg/m^3'),
    #             dict(data_name='depth', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
    #                  expected='cm'),
    #             dict(data_name='swe', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
    #                  expected='mm'),
    #         ],
    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "magnaprobe"),
            (Instrument, "model", "CRREL_B"),
            (MeasurementType, "name", ['depth']),
            (MeasurementType, "units", ['cm', 'ns', 'otherstuff']),
            (MeasurementType, "derived", [False]),
            (DOI, "doi", "some_point_doi"),
            (CampaignObservation, "name", "example_point_name"),
            (PointData, "geom",
                WKTElement('POINT (-108.13515 39.03045)', srid=4326)
             ),
            (PointObservation, "datetime", datetime(
                    2020, 1, 28, 18, 48, tzinfo=timezone.utc
            )),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'id', 1, [94]),
            ('depth', 'units', 'id', 1, ['cm']),
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
            ("depth", "value", 10),
            ("swe", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
