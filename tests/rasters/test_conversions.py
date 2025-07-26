import os
import shutil
from os.path import isdir, join
from pathlib import Path

import rasterio

import pytest
from numpy.testing import assert_almost_equal

from snowex_db.conversions import INSAR_to_rasterio
from snowex_db.metadata import read_InSar_annotation


# Does not require a database
class InSarToRasterioBase:
    """
    Convert the UAVSAR grd files to tif.
    This conversion is complicated and requires multiple tests to ensure
    fidelity.
    """
    this_location = Path(__file__).parent.parent

    # Temporary output folder
    temp = join(this_location, 'temp')

    # Data dir
    d = join(this_location, 'data')

    # Input file
    input_f = ''

    # Value comparison
    stats = {'mean': None, 'min': None, 'max': None, 'std': None}
    component = 'real'

    @classmethod
    def setup_class(cls):
        """
        Attempt to convert all the files
        """
        if not isdir(cls.temp):
            os.mkdir(cls.temp)

        cls.desc = read_InSar_annotation(join(cls.d, 'uavsar_latlon.ann'))

        # Output file
        f_pieces = cls.input_f.split('.')[0:-1] + [cls.component, 'tif']
        output_f = join(cls.temp, '.'.join(f_pieces))

        if isdir(cls.temp):
            shutil.rmtree(cls.temp)

        os.mkdir(cls.temp)

        INSAR_to_rasterio(join(cls.d, cls.input_f), cls.desc, join(cls.temp, cls.input_f.replace('grd', 'tif')))
        cls.dataset = rasterio.open(output_f)
        cls.band = cls.dataset.read(1)

    @classmethod
    def teardown_class(self):
        """
        On tear down clean up the files
        """
        self.dataset.close()
        # Delete the files
        shutil.rmtree(self.temp)

    def test_coords(self):
        """
        Test by Opening tiff and confirm coords are as expected in ann
        """
        nrows = self.desc['ground range data latitude lines']['value']
        ncols = self.desc['ground range data longitude samples']['value']
        assert self.band.shape == (nrows, ncols)

    @pytest.mark.parametrize("stat", [('mean'), ('min'), ('max'), ('std')])
    def test_stat(self, stat):
        """
        Test Values statistics are as expected
        """
        fn = getattr(self.band, stat)
        assert_almost_equal(self.stats[stat], fn(), 7)

    @pytest.mark.parametrize("dim, desc_key", [
        # Test the height
        ('width', 'ground range data longitude samples'),
        # Test the width
        ('height', 'ground range data latitude lines')])
    def test_dimensions(self, dim, desc_key):
        """
        Test the height of the array is correct
        """
        assert getattr(self.dataset, dim) == self.desc[desc_key]['value']


class TestInSarToRasterioCorrelation(InSarToRasterioBase):
    """
    Test converting an amplitude file to tif, test its integrity
    """
    input_f = 'uavsar_latlon.cor.grd'

    stats = {'mean': 0.6100003123283386,
             'min': 0.0016001829644665122,
             'max': 0.9895432591438293,
             'std': 0.18918956816196442}


class TestInSarToRasterioAmplitude(InSarToRasterioBase):
    """
    Test converting an amplitude file to tif, test its integrity
    """
    input_f = 'uavsar_latlon.amp1.grd'

    stats = {'mean': 0.303824245929718,
             'min': 0.046944327652454376,
             'max': 4.238321304321289,
             'std': 0.12033049762248993, }


class TestInSarToRasterioInterferogramImaginary(InSarToRasterioBase):
    """
    Test converting an interferogram file to tif, test its integrity
    imaginary component test only
    """
    input_f = 'uavsar_latlon.int.grd'
    component = 'imaginary'

    # Values taken from before conversion back to bytes
    stats = {'mean': -0.00034686949220485985,
             'min': -8.490335464477539,
             'max': 3.2697348594665527,
             'std': 0.03587743639945984}


class TestInSarToRasterioInterferogramReal(InSarToRasterioBase):
    """
    Test converting an interferogram file to tif, test its integrity
    imaginary component test only
    """

    input_f = 'uavsar_latlon.int.grd'
    component = 'real'

    # Values taken from before conversion back to bytes
    stats = {'mean': 0.046042509377002716,
             'min': -0.32751649618148804,
             'max': 15.531420707702637,
             'std': 0.06184517219662666}
