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

from snowex_db.batch import UploadRasterBatch


def main():
    """
    Uploader script partial SnowEx Lidar
    """

    # Typical kwargs for the dataset
    kwargs = {'instrument': 'lidar',
              'observers': 'chris larsen',
              'description': '0.5m products', # Datasheet says 0.25 but data is actually 0.5
              'tiled': True,
              'epsg': 26906, # Alaska is Zone 6
              'no_data': -9999,
              'in_timezone': 'AKST',
              'doi':'https://doi.org/10.5067/BV4D8RRU1H7U',
              "site_name": "farmers-creamers"
              }
    # Build a list of uploaders and then execute them
    uploaders = []

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
    # Snow off - canopy height
    f = reprojected.joinpath("SNEX23_Lidar_FLCF_CH_0.25M_20221024_V01.0.tif")
    uploaders.append(UploadRasterBatch([f], date=date(2022, 10, 24),
                                       type="canopy_height", units="meters", **kwargs))

    # Snow Depth
    f =  reprojected.joinpath(reprojected, "SNEX23_Lidar_FLCF_SD_0.25M_20230311_V01.0.tif")
    uploaders.append(UploadRasterBatch([f], date=date(2023, 3, 11),
                                       type="depth", units="meters", **kwargs))

    errors = 0
    for u in uploaders:
        u.push()
        errors += len(u.errors)


# Add this so you can run your script directly without running run.py
if __name__ == '__main__':
    main()
