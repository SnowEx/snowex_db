import shutil
from os import mkdir, remove
from os.path import dirname, isdir, isfile, join

import pytest
from geoalchemy2.shape import to_shape
from geoalchemy2.types import WKTElement
from numpy.testing import assert_almost_equal
from rasterio.crs import CRS

from snowex_db.projection import *


@pytest.mark.parametrize('info, utm_zone, expected', [
    # Test we add UTM info when its not provided
    ({'latitude': 39.039, 'longitude': -108.003}, None,
     {'easting': 759397.644, 'northing': 4325379.675, 'utm_zone': 12}),
    # Test we add lat long when its not provided
    ({'easting': 759397.644, 'northing': 4325379.675, 'utm_zone': 12}, None,
     {'latitude': 39.039, 'longitude': -108.003}),
    # Test ignoring easting in another projection
    ({'latitude': 39.008078, 'longitude': -108.184794, 'utm_wgs84_easting': 743766.4795,
      'utm_wgs84_northing': 4321444.155}, None,
     {'easting': 743766.480, 'northing': 4321444.155}),
    # Confirm we force the zone to zone 12
    ({'latitude': 39.097464, 'longitude': -107.862476}, 12,
     {'northing': 4332280.1658, 'easting': 771338.607, "utm_zone": 12}),
    # Test Missing missing longitude
    ({"utm_zone": "10N", "easting": "757215", "northing": "4288778", "latitude": "38.71025", "longitude": ""}, None,
     {"utm_zone": 10, "easting": 757215.0, "northing": 4288778.0, "latitude": 38.71025, "longitude": -120.041884,
      'epsg': 26910}, ),
    # Test wrong incoming utm zone
    ({"utm_zone": "12N", "latitude": "38.71033", "longitude": "-120.04187"}, None,
     {"utm_zone": 10, "latitude": 38.71025, "longitude": -120.041884, 'epsg': 26910},),
    # Test moving past when nothing is provided
    ({}, None, {"utm_zone": None, 'epsg': None},),
])
def test_reproject_point_in_dict(info, utm_zone, expected):
    """
    Test adding point projection information
    """
    result = reproject_point_in_dict(info, zone_number=utm_zone)

    for k, v in expected.items():
        assert k in result
        if type(v) == float:
            assert_almost_equal(v, result[k], 3)
        else:
            assert v == result[k]


def test_add_geom():
    """
    Test add_geom adds a WKB element to a dictionary containing easting/northing info
    """
    info = {'easting': 759397.644, 'northing': 4325379.675, 'utm_zone': 12}
    result = add_geom(info, 26912)

    # Ensure we added a geom key and value that is WKTE
    assert 'geom' in result.keys()
    assert type(result['geom']) == WKTElement

    # Convert it to pyshapely for testing/ data integrity
    p = to_shape(result['geom'])
    assert p.x == info['easting']
    assert p.y == info['northing']
    assert result['geom'].srid == 26912


class TestReprojectRasterByEPSG():
    output_f = join(dirname(__file__), 'test.tif')

    # def teardown_method(self):
    #     '''
    #     Remove our output file
    #     '''
    #     if isfile(self.output_f):
    #         remove(self.output_f)
    @classmethod
    def teardown_method(self):
        remove(self.output_f)

    @pytest.mark.parametrize("input_f, epsg, bounds", [
        ('uavsar_latlon.amp1.real.tif', 26912,
         (748446.1945536422, 4325651.650770078, 751909.2857505103, 4328702.971977075)),
    ])
    def test_reproject(self, input_f, epsg, bounds):
        """
        test reprojecting a raster from EPSG to another
        """
        d = dirname(__file__)
        f = join(d, 'data', input_f)

        reproject_raster_by_epsg(f, self.output_f, epsg)

        with rasterio.open(self.output_f) as dataset:
            dbounds = dataset.bounds
            dcrs = dataset.crs

        # Test our epsg was assigned
        assert CRS.from_epsg(epsg) == dataset.crs

        # Assert bounds
        for i, v in enumerate(bounds):
            assert_almost_equal(v, dataset.bounds[i], 3)
