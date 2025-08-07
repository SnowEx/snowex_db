from datetime import date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation

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
        "name": "GPR DATA",
        "instrument": "gpr",
        "instrument_model": "BSU",
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(session, str(data_dir.joinpath("bsu_gpr.csv")))

    @pytest.mark.usefixtures("uploaded_file")
    def test_instrument(self, session):
        record = self.get_records(session, Instrument, "name", "gpr")
        assert len(record) == 1
        assert record[0].model == "BSU"

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, units, derived", [
            ("two_way_travel", "ns", False),
            ("depth", "cm", False),
            ("swe", "mm", False),
        ]
    )
    def test_measurement_type(self, name, units, derived, session):
        record = self.get_records(session, MeasurementType, "name", name)
        assert len(record) == 1
        assert record[0].units == units
        assert record[0].derived == derived

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name", [
            "GPR DATA_gpr_BSU_two_way_travel",
            "GPR DATA_gpr_BSU_depth",
            "GPR DATA_gpr_BSU_swe",
        ]
    )
    def test_point_observation(self, name, session):
        records = self.get_records(session, PointObservation, "name", name)
        assert len(records) == 2

        for record in records:
            # Attributes
            assert record.date in [ date(2020, 1, 28), date(2020, 2, 4)]
            # Relationships
            assert record.doi.doi == "some_gpr_point_doi"
            assert record.observer.name == 'unknown'
            assert record.instrument.name == "gpr"
            assert record.measurement_type.name in ["two_way_travel", "depth", "swe"]
            assert record.campaign.name == "Grand Mesa"

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "some_gpr_point_doi"),
            (PointData, "geom",
                WKTElement('POINT (-108.190889311605 39.0343743775669)', srid=4326)
             ),
        ]
    )
    def test_metadata(self, table, attribute, expected_value):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected",
        [
            ("two_way_travel", "value", "date", date(2020, 1, 28), [8.3] * 8),
            (
                "depth",
                "value",
                "date",
                date(2020, 1, 28),
                [
                    101.096735522092,
                    101.096735522092,
                    101.096735522092,
                    101.096735522092,
                    101.096735522092,
                    101.096735522092,
                    101.096735522092,
                    101.096735522092,
                ],
            ),
            (
                "swe",
                "value",
                "date",
                date(2020, 1, 28),
                [
                    275.994087975311,
                    275.994087975311,
                    275.994087975311,
                    275.994087975311,
                    275.994087975311,
                    275.994087975311,
                    275.994087975311,
                    275.994087975311,
                ],
            ),
        ],
    )
    def test_value(
        self, data_name, attribute_to_check, filter_attribute, filter_value, expected
    ):
        self.check_value(
            data_name, attribute_to_check, filter_attribute, filter_value, expected,
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
        assert self.check_count(data_name) == expected
