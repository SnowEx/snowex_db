import datetime
import os
from os.path import dirname

import pytest
from sqlalchemy import func

from snowex_db.upload.batch import UploadRasterBatch, UploadUAVSARBatch
from snowexsql.tables import ImageData
from .sql_test_base import TableTestBase


class TestUploadRasterBatch(TableTestBase):
    """
    Class testing the batch uploading of rasters
    """
    args = [['be_gm1_0287/w001001x.adf', 'be_gm1_0328/w001001x.adf']]
    kwargs = {
        'type': 'dem', 'observers': 'QSI',
        'units': 'meters',
        'epsg': 26912,
        'use_s3': False
    }
    UploaderClass = UploadRasterBatch
    TableClass = ImageData

    params = {
        'test_count': [dict(data_name='dem', expected_count=32)],
        'test_value': [dict(data_name='dem', attribute_to_check='observers', filter_attribute='id', filter_value=1,
                            expected='QSI'),
                       dict(data_name='dem', attribute_to_check='units', filter_attribute='id', filter_value=1,
                            expected='meters'),
                       ],
        # Dummy input
        'test_unique_count': [dict(data_name='dem', attribute_to_count='date', expected_count=1), ]
    }


class TestUploadUAVSARBatch(TableTestBase):
    """
    Test the UAVSAR uploader by providing one ann file which should upload
    all of the uavsar images.
    """
    observers = 'UAVSAR team, JPL'
    # Upload all uav
    d = os.path.join(dirname(__file__), 'data', 'uavsar')
    args = [['uavsar.ann']]
    kwargs = {
        'observers': observers,
        'epsg': 26912,
        'geotiff_dir': d,
        'instrument': 'UAVSAR, L-band InSAR',
        'use_s3': False
    }

    UploaderClass = UploadUAVSARBatch
    TableClass = ImageData

    params = {
        'test_count': [dict(data_name='insar amplitude', expected_count=18),
                       dict(data_name='insar correlation', expected_count=9),
                       dict(data_name='insar interferogram real', expected_count=9),
                       dict(data_name='insar interferogram imaginary', expected_count=9)],

        'test_value': [
            dict(data_name='insar interferogram imaginary', attribute_to_check='observers', filter_attribute='units',
                 filter_value='Linear Power and Phase in Radians', expected='UAVSAR team, JPL'),
            dict(data_name='insar interferogram real', attribute_to_check='units', filter_attribute='observers',
                 filter_value=observers, expected='Linear Power and Phase in Radians'),
            dict(data_name='insar correlation', attribute_to_check='instrument', filter_attribute='observers',
                 filter_value=observers, expected='UAVSAR, L-band InSAR'),
            ],
        # Test we have two dates for the insar amplitude overapasses
        'test_unique_count': [dict(data_name='insar amplitude', attribute_to_count='date', expected_count=2), ]
    }
    def test_uavsar_date(self):
        """
        Github actions is failing on a test pulling 1 of 2 uavsar dates. This is likely because the dates are not in
        the same order as our tests are expecting. This test accomplishes the same test but adds assurance around which
        date is being checked.
        """
        results = self.session.query(func.min(ImageData.date)).filter(ImageData.type == 'insar amplitude').all()
        assert results[0][0] == datetime.date(2020, 2, 1)

    @pytest.mark.parametrize("data_name, kw", [
        # Check the single pass products have a few key words
        ('amplitude', ['duration', 'overpass', 'polarization', 'dem']),
        # Check the derived products all have a ref to 1st and 2nd overpass in addition to the others
        ('correlation', ['duration', 'overpass', '1st', '2nd', 'polarization', 'dem']),
        ('interferogram real', ['duration', 'overpass', '1st', '2nd', 'polarization', 'dem']),
        ('interferogram imaginary', ['duration', 'overpass', '1st', '2nd', 'polarization', 'dem']),
    ])
    def test_description_generation(self, data_name, kw):
        """
        Asserts each kw is found in the description of the data
        """
        name = 'insar {}'.format(data_name)
        records = self.session.query(ImageData.description).filter(ImageData.type == name).all()

        for k in kw:
            assert k in records[0][0].lower()
