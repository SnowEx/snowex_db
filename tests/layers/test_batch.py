import pytest
from snowexsql.tables import LayerData, Site

from snowex_db.upload.layers import UploadProfileBatch
from tests.helpers import WithUploadBatchFiles
from tests.sql_test_base import TableTestBase


class TestUploadProfileBatch(TableTestBase, WithUploadBatchFiles):
    """
    Test uploading multiple vertical profiles
    """

    kwargs = {
        'in_timezone': 'MST',
        'doi': 'DOI-1234321',
        'campaign_name': 'Campaign 1',
        'instrument': 'unknown',
    }
    UploaderClass = UploadProfileBatch
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        fnames = ['site_details.csv', 'stratigraphy.csv', 'temperature.csv']
        fpaths = [str(data_dir.joinpath(f)) for f in fnames]
        self.upload_file(fpaths)

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "data_name, expected", [
            ("snow_temperature", 5),
            ("hand_hardness", 5),
        ]
    )
    def test_count(self, data_name, expected):
        n = self.check_count(data_name)
        assert n == expected

    def test_only_one_site(self):
        """
        The three CSVs are for the same site. Verify we only create one
        Site record and properly associate the layer information with it.
        """
        records = self.get_records(Site, 'name', 'COGM1N20_20200205')
        assert len(records) == 1

        site = records[0]
        # The sratigraphy has 5 layers with 5 data points,
        # plus 5 LWC measurements
        assert len(site.layer_data) == 30


class TestUploadProfileBatchErrors(TableTestBase):
    """
    Test uploading multiple vertical profiles
    """
    files = ['doesnt_exist.csv']
    UploaderClass = UploadProfileBatch

    def test_without_debug(self, db, data_dir):
        fpaths = [str(data_dir.joinpath(f)) for f in self.files]
        u = self.UploaderClass(fpaths, debug=False)
        u.push()
        assert len(u.errors) == 1

    def test_with_debug(self, db, data_dir):
        """
        Test batch uploading with debug and errors
        """
        fpaths = [str(data_dir.joinpath(f)) for f in self.files]
        with pytest.raises(Exception):
            u = self.UploaderClass(fpaths, debug=True)
            u.push()

    def test_without_files(self, db, data_dir):
        """
        Test that batch correctly runs with no files
        """
        u = self.UploaderClass([], debug=True)
        u.push()
        assert u.uploaded == 0
