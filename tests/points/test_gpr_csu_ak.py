from datetime import date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation

from snowex_db.upload.points import PointDataCSV

from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestCSUAKGPR(TableTestBase, WithUploadedFile):
    kwargs = {
        # Constant Metadata for the GPR data
        'campaign_name': 'Alaska 2023',
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

    @pytest.mark.usefixtures("uploaded_file")
    def test_instrument(self):
        record = self.get_records(Instrument, "name", self.kwargs['instrument'])
        assert len(record) == 1
        assert record[0].model == self.kwargs['instrument_model']

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, units, derived",
        [
            ("density", "kg/m^3", False),
            ("depth", "cm", False),
            ("swe", "mm", False),
            ("two_way_travel", "ns", False),
        ],
    )
    def test_measurement_type(self, name, units, derived):
        record = self.get_records(MeasurementType, "name", name)
        assert len(record) == 1
        assert record[0].units == units
        assert record[0].derived == derived

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name", [ "CSU GPR Data gpr pulseEkko pro 1 GHz GPR"],
    )
    def test_point_observation(self, name):
        records = self.get_records(PointObservation, "name", name)
        dates = [date(2023, 3, 7)]

        assert len(records) == len(dates)

        for record in records:
            # Attributes
            assert record.date in dates
            assert record.doi.doi == self.kwargs['doi']
            assert record.observer.name == self.kwargs['observer']
            assert record.instrument.name == self.kwargs['instrument']
            assert record.campaign.name == self.kwargs['campaign_name']

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Alaska 2023"),
            (DOI, "doi", "preliminary_gpr_ak_farmers-creamers"),
        ]
    )
    def test_metadata(self, table, attribute, expected_value):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "table, attribute, lon, lat", [
            (PointData, "geom", -147.737230798003, 64.8638112503524),
        ]
    )
    def test_point_location(
            self, table, attribute, lon, lat, uploaded_file
    ):
        self._check_location(table, lon, lat, attribute=attribute)

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "measurement_type, attribute_to_check, filter_attribute, filter_value, expected",
        [
            (
                "two_way_travel",
                "value",
                "date",
                date(2023, 3, 7),
                [1.45, 1.487265, 1.524917, 1.55, 1.562569],
            ),
            (
                "depth",
                "value",
                "date",
                date(2023, 3, 7),
                [
                    18.6201608827132,
                    19.098699017399,
                    19.5822068088169,
                    19.9043099091073,
                    20.0657146002347,
                ],
            ),
            (
                "swe",
                "value",
                "date",
                date(2023, 3, 7),
                [
                    36.8679185477722,
                    37.81542405445,
                    38.7727694814574,
                    39.4105336200324,
                    39.7301149084648,
                ],
            ),
        ],
    )
    def test_value(
            self, measurement_type, attribute_to_check,
            filter_attribute, filter_value, expected
    ):
        records = self.check_value(
            measurement_type, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

        assert records[0].measurement_type.name == measurement_type

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "data_name, expected", [
            ("depth", 5),
            ("swe", 5),
            ("two_way_travel", 5),
            ("density", 5)
        ]
    )
    def test_count(self, data_name, expected):
        n = self.check_count(data_name)
        assert n == expected
