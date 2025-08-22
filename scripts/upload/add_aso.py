"""
Upload the SnowEx ASO product Grand Mesa, East River, and Reynolds Creek from 2020

# To run with all the scripts
python run.py

# To run individually
python add_aso.py
"""
from datetime import date
from os.path import join
from pathlib import Path

from snowexsql.db import db_session_with_credentials

from snowex_db.upload.raster_mapping import RasterType, \
    metadata_from_single_file
from snowex_db.upload.rasters import UploadRaster


def main():
    """
    Uploader script for ASO Snow off data
    """
    EPSG = 26912  # EPSG code for the Grand Mesa area

    # Typical kwargs
    kwargs = {
        'instrument': 'lidar',
        'observer': 'ASO Inc.',
        'comments': '50m product',
        'tiled': True,
        'no_data': -9999,
        'campaign_name': 'Grand Mesa',
        'name': 'ASO Data',
        'doi': "no_doi_aso_data_2020_grand_mesa",
        'timezone': 'MST',
    }

    # Directory of ASO products reprojected
    reprojected = '../download/data/aso/reprojected'
    geotif_loc = join("../download/", "geotiffs")

    ########################################### Grand MESA #############################################################
    with db_session_with_credentials() as (_engine, session):
        # SWE flights
        for f, dt, raster_type in [
            ("ASO_GrandMesa_Mosaic_2020Feb1-2_swe_50m.tif",
             date(2020, 2, 2), RasterType.SWE),
            ("ASO_GrandMesa_Mosaic_2020Feb13_swe_50m.tif",
             date(2020, 2, 13), RasterType.SWE),
            ("ASO_GrandMesa_Mosaic_2020Feb1-2_snowdepth_50m.tif",
             date(2020, 2, 2), RasterType.DEPTH),
            ("ASO_GrandMesa_Mosaic_2020Feb13_snowdepth_50m.tif",
             date(2020, 2,13), RasterType.DEPTH),
            ("ASO_GrandMesa_Mosaic_2020Feb1-2_snowdepth_3m.tif",
             date(2020, 2, 2), RasterType.DEPTH),
            ("ASO_GrandMesa_Mosaic_2020Feb13_snowdepth_3m.tif",
             date(2020, 2, 13), RasterType.DEPTH),
        ]:
            fpath = join(reprojected, f)
            raster_metadata = metadata_from_single_file(
                Path(fpath), date=dt, type=RasterType.SWE,
                **kwargs
            )
            if "snowdepth_3m" in f:
                raster_metadata['comments'] = "3m snow depth product"
            rs = UploadRaster(
                session, fpath, EPSG,
                cog_dir=geotif_loc, **raster_metadata
            )
            rs.submit()


# Add this so you can run your script directly without running run.py
if __name__ == '__main__':
    main()
