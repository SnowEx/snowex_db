"""
This script uploads the UAVSAR raster to the database after they have been
converted to geotifs.

Usage:
1. Download the data from the GDrive sent from HP.

2. Unzip into your Downloads.

3. Convert data to GeoTiffs, and reprojects it (use convert_uavsar.py)

4. Run this script.

Usage:
    # To run with all the scripts
    python run.py

    # To run individually
    python add_UAVSAR.py

"""
import sys
import glob
from os.path import abspath, expanduser, join
from pathlib import Path

from snowexsql.db import db_session_with_credentials

from snowex_db.upload.raster_mapping import rasters_from_annotation
from snowex_db.upload.rasters import UploadRaster


def main():

    region = 'all'

    if len(sys.argv) > 1:
        region = sys.argv[1]

    # Location of the downloaded data
    downloads = '../download/data/uavsar'

    # Sub folder name under the downloaded data that the tifs were saved to
    geotif_loc = 'geotiffs'

    data = {
        # Tile the data going in for faster retrieval
        'tiled': True,

        # Metadata
        'observer': 'UAVSAR team, JPL',
        'instrument': 'UAVSAR, L-band InSAR',
        'campaign_name': 'Grand Mesa',
        'name': 'UAVSAR Data',
        'doi': "https://asf.alaska.edu/doi/uavsar/#R0ARICRBAKYE",
        'timezone': 'MST',
    }

    # Expand the paths
    downloads = abspath(expanduser(downloads))
    geotif_loc = Path(downloads, geotif_loc)
    geotif_loc.mkdir(parents=True, exist_ok=True)

    output_geotif_loc = geotif_loc.joinpath('converted')
    Path(output_geotif_loc).mkdir(parents=True, exist_ok=True)

    with db_session_with_credentials() as (_engine, session):

        if region in ['all', 'grand_mesa']:
            epsg = 26912  # EPSG code for the Grand Mesa area
            print('Uploading Grand Mesa')
            ########################## Grand Mesa #####################################
            # Grab all the grand mesa annotation files in the original data folder
            ann_files = glob.glob(join(downloads, 'grmesa_*.ann'))
            # Instantiate the uploader
            for f in ann_files:
                # Map the annotation files to the related ones and parse
                # the metadata
                for raster_metadata, raster_path in rasters_from_annotation(
                    Path(f), geotif_loc, **data
                ):
                    rs = UploadRaster(
                        session, raster_path, epsg,
                        cog_dir=output_geotif_loc, **raster_metadata
                    )
                    print("here")
                    rs.submit()

        if region in ['all', 'lowman']:
            print('Uploading Lowman')
            ############################### Idaho - Lowman ####################################
            # Make adjustments to metadata for lowman files
            data['campaign_name'] = 'idaho'
            epsg = 26911

            # Grab all the lowman and reynolds annotation files
            ann_files = glob.glob(join(downloads, 'lowman_*.ann'))

            for f in ann_files:
                for raster_metadata, raster_path in rasters_from_annotation(
                        Path(f), Path(geotif_loc), **data
                ):
                    rs = UploadRaster(
                        session, raster_path, epsg,
                        cog_dir=geotif_loc, **raster_metadata
                    )
                    rs.submit()

        if region in ['all', 'reynolds']:
            print("Uploading Reynolds Creek")

            ############################## Idaho - Reynolds ####################################
            # Make adjustments to metadata for lowman files
            data['campaign_name'] = 'idaho'
            epsg = 26911

            # Grab all the lowman and reynolds annotation files
            ann_files = glob.glob(join(downloads, 'silver_*.ann'))

            for f in ann_files:
                for raster_metadata, raster_path in rasters_from_annotation(
                        Path(f), Path(geotif_loc), **data
                ):
                    rs = UploadRaster(
                        session, raster_path, epsg,
                        cog_dir=geotif_loc, **raster_metadata
                    )
                    rs.submit()


if __name__ == '__main__':
    main()
