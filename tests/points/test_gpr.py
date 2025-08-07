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
    gpr_dt = date(2019, 1, 28)
    kwargs = {
        'timezone': "UTC",
        'doi': "some_gpr_point_doi",
        "campaign_name": "Grand Mesa",
        "name": "GPR DATA",
        "instrument": "gpr",
        "instrument_model": "GPR 1",
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath("gpr.csv")), session=session
        )

    @pytest.mark.usefixtures("uploaded_file")
    def test_instrument(self, session):
        record = self.get_records(session, Instrument, "name", "gpr")
        assert len(record) == 1
        assert record[0].model == "GPR 1"

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, units, derived", [
            ("two_way_travel", "ns", False),
            ("density", "kg/m^3", False),
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
            "GPR DATA_gpr_GPR 1_two_way_travel",
            "GPR DATA_gpr_GPR 1_density",
            "GPR DATA_gpr_GPR 1_depth",
            "GPR DATA_gpr_GPR 1_swe",
        ]
    )
    def test_point_observation(self, name, session):
        records = self.get_records(session, PointObservation, "name", name)
        assert len(records) == 3

        for record in records:
            # Attributes
            assert record.date in [
                date(2019, 1, 28), date(2019, 1, 29), date(2019, 2, 4),
            ]
            # Relationships
            assert record.doi.doi == "some_gpr_point_doi"
            assert record.observer.name == "unknown"
            assert record.instrument.name == "gpr"
            assert record.measurement_type.name in [
                "two_way_travel", "density", "depth", "swe"
            ]
            assert record.campaign.name == "Grand Mesa"

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "some_gpr_point_doi"),
            (PointData, "geom",
                WKTElement('POINT (-108.19088935297108 39.03437443810879)', srid=4326)
             ),
        ]
    )
    def test_metadata(self, table, attribute, expected_value):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected",
        [
            (
                "two_way_travel",
                "value",
                "date",
                date(2019, 1, 28),
                [8.3, 10.0058518216919],
            ),
            (
                "density",
                "value",
                "date",
                date(2019, 1, 28),
                [250.786035454008, 280.938399439763],
            ),
            (
                "depth",
                "value",
                "date",
                date(2019, 1, 28),
                [102.662509421414, 121.213915188656],
            ),
            (
                "swe",
                "value",
                "date",
                date(2019, 1, 28),
                [257.463237275561, 340.536433229281],
            ),
        ],
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
        assert self.check_count(data_name) == expected
