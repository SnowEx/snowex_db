"""
1. To download the data, run sh download_snow_off.sh
2. Run this script.

Usage:
    # To run with all the scripts
    python run.py

    # To run individually
    python add_gm_snow_off.py


Spatial Reference Original:
 * EPSG:26912 (No reprojection needed)
 * Vertical Datum is NAVD 88 (No reprojection needed)
 * URL https://www.sciencebase.gov/catalog/file/get/5a54a313e4b01e7be23c09a6?f=__disk__32%2F31%2Fd0%2F3231d0ab78c88fd13cc46066cd03a0a2055276aa&transform=1&allowOpen=true

Citation:
U.S. Geological Survey, 20171101, USGS NED Original Product Resolution CO MesaCo-QL2 2015 12SYJ515455 IMG 2017: U.S. Geological Survey.
"""

import glob
from os.path import abspath, expanduser, join
from pathlib import Path

from snowexsql.db import db_session_with_credentials

from snowex_db.upload.raster_mapping import metadata_from_single_file, \
    RasterType
from snowex_db.upload.rasters import UploadRaster


def main():

    # Location of the downloaded data
    downloads = '~/Downloads/GM_DEM'

    # Spatial Reference
    epsg = 26912

    # Metadata
    kwargs = dict(
        observer='USGS',
        instrument='lidar',
        campaign_name='Grand Mesa',
        units='meters',  # Add from the Annotation file
        comments='US Geological Survey 1m snow off DEM from the 3DEP',
        tiled=True,
        doi='https://doi.org/10.3133/fs20203062',
        timezone='MST'
    )

    # Expand the paths
    downloads = abspath(expanduser(downloads))

    # Grab all the geotiff,
    files = glob.glob(join(downloads, '*.tif'))
    with db_session_with_credentials() as (_engine, session):
        for fpath in files:
            # TODO: what do we do for date?
            raster_metadata = metadata_from_single_file(
                Path(fpath), date=dt, type=RasterType.DEM,
                **kwargs
            )
            rs = UploadRaster(
                session, fpath, epsg, **raster_metadata
            )
            rs.submit()


if __name__ == '__main__':
    main()
