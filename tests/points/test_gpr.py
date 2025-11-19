from datetime import date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation

from snowex_db.upload.points import PointDataCSV
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestGPR(TableTestBase, WithUploadedFile):
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
    def test_instrument(self):
        record = self.get_records(Instrument, "name", self.kwargs['instrument'])
        assert len(record) == 1
        assert record[0].model == self.kwargs['instrument_model']

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, units, derived", [
            ("two_way_travel", "ns", False),
            ("density", "kg/m^3", False),
            ("depth", "cm", False),
            ("swe", "mm", False),
        ]
    )
    def test_measurement_type(self, name, units, derived):
        record = self.get_records(MeasurementType, "name", name)

        assert len(record) == 1
        assert record[0].units == units
        assert record[0].derived == derived


    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name", ["GPR DATA gpr GPR 1"]
    )
    def test_point_observation(self, name):
        records = self.get_records(PointObservation, "name", name)
        dates = [date(2019, 1, 28), date(2019, 1, 29), date(2019, 2, 4)]

        assert len(records) == len(dates)

        for record in records:
            # Attributes
            assert record.date in dates
            # Relationships
            assert record.doi.doi == self.kwargs['doi']
            assert record.observer.name == "unknown"
            assert record.instrument.name == self.kwargs['instrument']
            assert record.campaign.name == self.kwargs['campaign_name']

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "some_gpr_point_doi"),
        ]
    )
    def test_metadata(self, table, attribute, expected_value):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "table, attribute, lon, lat", [
            (PointData, "geom", -108.19088935297108, 39.03437443810879),
        ]
    )
    def test_point_location(
            self, table, attribute, lon, lat, uploaded_file
    ):
        self._check_location(table, lon, lat, attribute=attribute)

    @pytest.mark.parametrize(
        "measurement_type, attribute_to_check, filter_attribute, filter_value, expected",
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
            self, measurement_type, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        records = self.check_value(
            measurement_type, attribute_to_check,
            filter_attribute, filter_value, expected
        )
        assert records[0].measurement_type.name == measurement_type

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
