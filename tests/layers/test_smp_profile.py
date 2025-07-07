import os
from datetime import datetime, timezone

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site, Campaign, Instrument, \
    MeasurementType

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestSMPProfile(TableTestBase, WithUploadedFile):
    """
    Test SMP profile is uploaded with all its attributes and valid data
    """

    kwargs = {
        'timezone': 'UTC',
        'header_sep': ':',
        'instrument': 'snowmicropen',
        'id': "COGM_Fakepitid123",
        'campaign_name': "Grand Mesa",
        "derived": True,
        'comments': 'Filename: S06M0874_2N12_20200131.CSV',
        "doi": "SMP DOI",
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath("S06M0874_2N12_20200131.CSV")),
            session=session,
        )

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM_Fakepitid123"),
            (
                Site, "datetime",
                datetime(
                    2020, 1, 31, 22, 42, 14, 0, tzinfo=timezone.utc
                ),
            ),
            (
                Site, "geom",
                WKTElement(
                    'POINT (-108.16268920898438 39.03013229370117)', srid=4326
                ),
            ),
            (Site, "comments", "Filename: S06M0874_2N12_20200131.CSV"),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "snowmicropen"),
            (Instrument, "model", "6"),
            (MeasurementType, "name", ["force"]),
            (MeasurementType, "units", ["n"]),
            (MeasurementType, "derived", [True]),
        ]
    )
    def test_metadata(self, table, attribute, expected_value):
        # need:
        #  * comments = "Filename: filename"
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, "
        "filter_value, expected",
        [
            ('force', 'value', 'depth', -53.17, [0.331]),
        ]
    )
    def test_value(
        self, data_name, attribute_to_check,
        filter_attribute, filter_value, expected
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "data_name, expected", [
            ("force", 154),
        ]
    )
    def test_count(self, data_name, expected):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("force", "site_id", 1),
        ]
    )
    def test_unique_count(
        self, data_name, attribute_to_count, expected
    ):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
