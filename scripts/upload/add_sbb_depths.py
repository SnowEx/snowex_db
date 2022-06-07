"""
Uploads the Snowex SBB 2017 depths to the database

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_snow_depths.py # To run individually
"""

import glob
import time
from os.path import abspath, join

from snowexsql.db import get_db
from snowexsql.upload import *


def main():
    # Site name
    start = time.time()
    site_name = 'Senator Beck'
    timezone = 'US/Mountain'

    # Read in the Grand Mesa Snow Depths Data
    base = '/home/micah/projects/m3works/basins/testing/senator_beck/topo/validation/SNOWEX/SNEX17_SD.001/2017.02.06'

    # Start the Database
    db_name = 'localhost/snowex'
    engine, session = get_db(db_name, credentials='./credentials.json')
    csvs = set(glob.glob(join(base, '*SD_SBB*v01*.csv'))) - set(glob.glob(join(base, '*reference*.csv')))
    print(csvs)
    errors = 0

    for f in csvs:
        csv = PointDataCSV(
            f,
            depth_is_metadata=False,
            units='cm',
            site_name=site_name,
            timezone=timezone,
            epsg=26913,
            doi="https://doi.org/10.5067/WKC6VFMT7JTF")

        csv.submit(session)
        errors += len(csv.errors)

    return errors


if __name__ == '__main__':
    main()
