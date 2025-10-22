"""
Usage:
    # To run with all the scripts
    python run.py

    # To run individually
    python add_QSI.py

Spatial Reference Original:
    * EPSG: 16012 (Needs reprojection)
    * Vertical Datum is NAVD88, Geoid 12B

Note: because rasters are so different in structure and metadata the metadata
needs to be provided as key word arguments to the UploadRasterBatch which
will pass them through to the final uploader

"""

import os
import shutil
from os.path import abspath, basename, expanduser, isdir, isfile, join, split
from pathlib import Path
from subprocess import check_output

import pandas as pd
from snowexsql.db import db_session_with_credentials

from snowex_db.upload.raster_mapping import metadata_from_single_file, \
    RasterType
from snowex_db.upload.rasters import UploadRaster
from snowex_db.utilities import get_logger


def reproject(filenames, out_epsg, out_dir, adjust_vdatum=False):
    """
    Reproject the data and then adjust the vertical datum
    """
    log = get_logger('reprojection')
    final = []

    if isdir(out_dir):
        shutil.rmtree(out_dir)

    os.mkdir(out_dir)
    n = len(filenames)
    log.info('Reprojecting {} files...'.format(n))

    for i, r in enumerate(filenames):
        bname = basename(r)
        log.info('Working on {} ({}/{})...'.format(bname, i, n))

        # Construct a new filename
        out = join(out_dir, bname.replace('.adf', '.tif'))
        in_ras = r
        # Some files share repeating naming convention
        if isfile(out):
            out = join(out_dir, '_'.join(
                split(r)[-2:]).replace('.adf', '.tif'))

        if adjust_vdatum:
            # Adjust the vertical datum in bash from python
            log.info('Reprojecting the vertical datum...')
            check_output('dem_geoid -o test {}'.format(in_ras), shell=True)

            # # Move the file back
            # log.info('Moving resulting files and cleaning up...')
            # check_output('mv test-adj.tif {}'.format(''), shell=True)
            in_ras = 'test-adj.tif'

        # Reproject the raster and output to the new location in bash from
        # python
        log.info('Reprojecting data to EPSG:{}'.format(out_epsg))
        check_output(
            'gdalwarp -r bilinear -t_srs "EPSG:{}" {} {}'.format(out_epsg, in_ras, out), shell=True)

        # Keep the new file name
        final.append(out)

    return final


def main():
    EPSG = 26912
    downloads = "../download/data/nsidc-cumulus-prod-protected"
    downloads = abspath(expanduser(downloads))

    # build our common metadata
    kwargs = {
        # Add these attributes to the db entry
        'observer': 'QSI',
        'instrument': 'lidar',
        'campaign_name': 'Grand Mesa',
        'timezone': 'MST',
        'doi': 'https://doi.org/10.5067/M9TPF6NWL53K',
        'name': "QSI Lidar data",
        'tiled': True,
        'no_data': -9999,
        "comments": "QSI Lidar derived snow depth for SnowEx 2020 Grand Mesa campaign",
    }

    # Form the directory structure and grab all the important files
    d = Path(downloads).joinpath(
        "SNOWEX/SNEX20_GM_Lidar/1/2020/02/01/SNEX20_GM_Lidar_SD_20200201_20200202_v01.0.tif"
    )
    output_dir = d.parent.joinpath('reprojected')
    output_dir.mkdir(exist_ok=True, parents=True)
    final = reproject([d], EPSG, str(output_dir))[0]

    raster_metadata = metadata_from_single_file(
        Path(final),
        RasterType.SWE,
        date=pd.to_datetime('02/01/2020').date(),
        **kwargs
    )

    # Instantiate the uploader
    with db_session_with_credentials() as (_engine, session):
        u = UploadRaster(
            session, final, EPSG, **raster_metadata)
        u.submit()


if __name__ == '__main__':
    main()
