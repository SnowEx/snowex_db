from datetime import date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation

from snowex_db.upload.points import PointDataCSV

from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestCSUGPR(TableTestBase, WithUploadedFile):
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

    @pytest.mark.usefixtures("uploaded_file")
    def test_instrument(self):
        record = self.get_records(Instrument, "name", self.kwargs['instrument'])
        assert len(record) == 1
        assert record[0].model == self.kwargs['instrument_model']

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, units, derived",
        [
            ("two_way_travel", "ns", False),
            ("depth", "cm", False),
            ("swe", "mm", False),
        ],
    )
    def test_measurement_type(self, name, units, derived):
        record = self.get_records(MeasurementType, "name", name)
        assert len(record) == 1
        assert record[0].units == units
        assert record[0].derived == derived

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name", [ "CSU GPR Data gpr pulse EKKO Pro multi-polarization 1 GHz GPR" ]
    )
    def test_point_observation(self, name):
        records = self.get_records(PointObservation, "name", name)
        dates = [date(2020, 2, 6)]

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
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "https://doi.org/10.5067/S5EGFLCIAB18"),
            (PointData, "geom",
                WKTElement('POINT (-108.176474337253 39.0192013494439)', srid=4326)
             ),
        ]
    )
    def test_metadata(self, table, attribute, expected_value):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "measurement_type, attribute_to_check, filter_attribute, filter_value, expected",
        [
            (
                "two_way_travel",
                "value",
                "date",
                date(2020, 2, 6),
                [7.348628, 7.225466, 7.102305, 6.979144, 6.814929],
            ),
            (
                "depth",
                "value",
                "date",
                date(2020, 2, 6),
                [
                    89.5675335280758,
                    88.0663939188338,
                    86.5652664979259,
                    85.0641390770181,
                    83.0626317863629,
                ],
            ),
            (
                "swe",
                "value",
                "date",
                date(2020, 2, 6),
                [
                    244.519366531647,
                    240.421255398416,
                    236.323177539338,
                    232.225099680259,
                    226.760984776771,
                ],
            ),
        ],
    )
    def test_value(
            self, measurement_type, attribute_to_check,
            filter_attribute, filter_value, expected,
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
            ("density", 0),  # no measurements
        ]
    )
    def test_count(self, data_name, expected):
        n = self.check_count(data_name)
        assert n == expected
