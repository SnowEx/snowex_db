from datetime import date

import numpy as np
import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType

from snowex_db.upload.points import PointDataCSV

from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase
from tables import PointObservation


class TestPerimeterDepth(TableTestBase, WithUploadedFile):
    kwargs = {
        'timezone': 'MST',
        'doi': "some_point_doi_perimeter",
        "campaign_name": "Grand Mesa",
        "name": "example_pole_point_name",
        "instrument": "probe",
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            session, str(data_dir.joinpath("perimeters.csv"))
        )

    @pytest.mark.usefixtures("uploaded_file")
    def test_instrument(self):
        record = self.get_records(Instrument, "name", "probe")
        assert len(record) == 1
        record = record[0]
        assert record.model is None

    @pytest.mark.usefixtures("uploaded_file")
    def test_measurement_type(self):
        record = self.get_records(MeasurementType, "name", "depth")
        assert len(record) == 1
        assert record[0].units == 'cm'
        assert record[0].derived is False

    @pytest.mark.usefixtures("uploaded_file")
    def test_point_observation(self):
        record = self.get_records(
            PointObservation, "name", "example_pole_point_name probe"
        )
        assert len(record) == 1
        record = record[0]
        # Attributes
        assert record.date == date(2019, 12, 20)
        # Relationships
        assert record.campaign.name == "Grand Mesa"
        assert record.instrument.name == "probe"

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "some_point_doi_perimeter"),
            (PointData, "geom",
                WKTElement('POINT (-120.04187 38.71033)', srid=4326)
             ),
        ]
    )
    def test_metadata(self, table, attribute, expected_value):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.usefixtures("uploaded_file")
    def test_values(self):
        date(2019, 12, 20)
        records = self.check_value(
            'depth', 'value', 'date', date(2019, 12, 20),
            [121.0, 120.0, 120.0, 120.0, 119.0, 119.0, 120.0, 120.0, np.nan],
        )
        # Data was at 13:00 MST and DB stores UTC
        assert records[0].datetime.hour == 20
        assert records[0].measurement_type.name == "depth"

    @pytest.mark.usefixtures("uploaded_file")
    def test_count(self):
        assert 9 == self.check_count("depth")
