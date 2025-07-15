from datetime import datetime, timezone, date

import numpy as np
import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV

from _base import PointBaseTesting


class TestPerimeterDepth(PointBaseTesting):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """

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
            filename=str(data_dir.joinpath("perimeters.csv")), session=session
        )

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "probe"),
            (Instrument, "model", None),
            (MeasurementType, "name", ['depth']),
            (MeasurementType, "units", ['cm']),
            (MeasurementType, "derived", [False]),
            (DOI, "doi", "some_point_doi_perimeter"),
            (CampaignObservation, "name", "example_pole_point_name_probe_depth"),
            (PointData, "geom",
                WKTElement('POINT (-120.04187 38.71033)', srid=4326)
             ),
            (PointObservation, "date", date(2019, 12, 20)),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('depth', 'value', 'date', date(2019, 12, 20), [121.0, 120.0, 120.0, 120.0, 119.0, 119.0, 120.0, 120.0, np.nan]),
            # ('depth', 'units', 'date', date(2019, 12, 20), ['cm'] * 9),
            # ('depth', 'datetime', 'date', date(2019, 12, 20), [datetime(2019, 12, 20, 20, 0, tzinfo=timezone.utc)]*9),
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
            ("depth", 9)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("depth", "value", 4),  # lots of repeat depths
            ("depth", "units", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
