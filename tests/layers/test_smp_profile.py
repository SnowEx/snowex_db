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
        "derived": True
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("S06M0874_2N12_20200131.CSV")))

    # def test_instrument_id_comment(self):
    #     """
    #     Test that the SMP serial ID is added to the comment column of a smp profile inspit of an instrument being passed
    #     """
    #     result = self.session.query(LayerData.comments).limit(1).one()
    #     assert 'serial no. = 06' in result[0]
    #
    # def test_original_fname_comment(self):
    #     """
    #     Test that the original SMP file name is added to the comment column of a smp profile. This is done for
    #     provenance so users can determine the original dataset location
    #     """
    #     result = self.session.query(LayerData.comments).limit(1).one()
    #     assert f'fname = {os.path.basename(self.args[0])}' in result[0]

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM_Fakepitid123"),
            (Site, "datetime", datetime(
                2020, 1, 31, 22, 42, 14, 0, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.16268920898438 39.03013229370117)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "snowmicropen"),
            (MeasurementType, "name", ["force"]),
            (MeasurementType, "units", ["n"]),
            (MeasurementType, "derived", [True])
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, "
        "filter_value, expected",
        [
            ('force', 'value', 'depth', -53.17, [0.331]),
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
            ("force", 154),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("force", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )
