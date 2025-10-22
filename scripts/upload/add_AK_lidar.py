"""
Upload the SnowEx AK lidar products from Farmers and Creamers field
for now.

# To run with all the scripts
python run.py

# To run individually
python add_AK_lidar.py
"""
import os
from datetime import date
from pathlib import Path
from subprocess import Popen
from earthaccess_data import get_files

from snowexsql.db import db_session_with_credentials

from snowex_db.upload.raster_mapping import RasterType, \
    metadata_from_single_file
from snowex_db.upload.rasters import UploadRaster


LIDAR_DOI = {
    "SNEX23_Lidar": "10.5067/BV4D8RRU1H7U",
}


def main():
    """
    Uploader script partial SnowEx Lidar
    """
    EPSG = 26906  # EPSG code for Alaska Zone 6

    # Typical kwargs for the dataset
    kwargs = {
        'instrument': 'lidar',
        'observer': 'chris larsen',
        'comments': '0.5m products', # Datasheet says 0.25 but data is actually 0.5
        'tiled': True,
        'no_data': -9999,
        'timezone': 'AKST',
        'doi': 'https://doi.org/10.5067/BV4D8RRU1H7U',
        # TODO: what is the campaign name?
        "campaign_name": "Alaska 2023"
        }

    # Directory of SNOWEX products
    lidar_dir = Path('../download/data/SNOWEX/SNEX23_Lidar.001/')
    reprojected = lidar_dir.joinpath('reprojected')

    if not reprojected.is_dir():
        os.mkdir(reprojected)

    # Reproject using GDAL
    print('Reprojecting files...')
    raw_files = lidar_dir.glob('*/*.tif')
    for f in raw_files:
        # Watch out for files already in the reprojection
        if f.parent != reprojected:
            output = reprojected.joinpath(f.name)
            cmd = f'gdalwarp -overwrite -t_srs EPSG:{kwargs["epsg"]} {f} {output}'
            print(cmd)
            p = Popen(cmd,shell=True)
            p.wait()

    ######################################## Farmers/Creamers Field (FLCF) ###################################################

    with db_session_with_credentials() as (_engine, session):
        # SWE flights
        for f, dt, raster_type in [
            ("SNEX23_Lidar_FLCF_CH_0.25M_20221024_V01.0.tif",
             date(2022, 10, 24), RasterType.CANOPY_HEIGHT),
            ("SNEX23_Lidar_FLCF_SD_0.25M_20230311_V01.0.tif",
             date(2023, 3, 11), RasterType.DEPTH),
        ]:
            fpath = reprojected.joinpath(f)
            raster_metadata = metadata_from_single_file(
                Path(fpath), date=dt, type=RasterType.SWE,
                **kwargs
            )
            u = UploadRaster(
                session, fpath, EPSG, **raster_metadata)
            u.submit()


# Add this so you can run your script directly without running run.py
if __name__ == '__main__':
    for data_set_id, doi in SMP_DOI.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
