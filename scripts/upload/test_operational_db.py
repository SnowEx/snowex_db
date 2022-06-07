"""
Script is used to confirm uploads to the db were successful
"""

from snowexsql.db import get_db
from snowex_db.data import SiteData, PointData, LayerData, ImageData
import pytest

@pytest.fixture(scope='session')
def session():
    db = 'db.snowexdata.org/snowex'
    engine, session = get_db(db, credentials='credentials.json')
    yield session
    session.close()



@pytest.mark.parametrize('surveyor, datatype, expected_tiles', [
    # 50m swe product for grand mesa
    ('ASO Inc.', 'swe', 2),
    # 3m depth product for a 2 day flight
    ('ASO Inc.', 'depth', 256),
    # USGS Snow off DEM
    ('USGS', 'DEM', 1713),
])
def test_imagedata_uploads(session, surveyor, datatype, expected_tiles):
    """
    Check the number of tiles in the DB is what is expected
    """
    qry = session.query(ImageData.raster).filter(ImageData.type == datatype)
    ntiles = qry.filter(ImageData.surveyors == surveyor).count()
    assert ntiles == expected_tiles

@pytest.mark.parametrize('data_type, expected', [
    # Number of GPR points
    ('two_way_travel', 1264905),
])
def test_gpr_upload(session, data_type, expected):
    """
    Confirm the USGS snow off dem was uploaded correctly
    """
    n_pnts = session.query(PointData).filter(PointData.type == data_type).count()
    assert n_pnts == expected


@pytest.mark.parametrize('data_type, expected', [
    # Number of unique pits with hand hardness
    ('hand_hardness', 155),
])
def test_profiles_upload(session, data_type, expected):
    """
    Confirm the snow pits were uploaded to layer data
    """
    n_pnts = session.query(LayerData.geom).filter(LayerData.type == data_type).distinct().count()
    assert n_pnts == expected


def test_site_details_upload(session):
    """
    Confirm the site details wereu uploaded
    """
    n_pnts = session.query(SiteData.site_id).count()
    assert n_pnts == 155

@pytest.mark.parametrize
def test_add_downsampled_smp(session):
    """
    Test the downsampled smp profiles are uploaded
    """
