"""
Uploads the Snowex 2020 depths to the database

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_snow_depths.py # To run individually
"""

import glob
from os.path import abspath, join

from snowex_db.upload.points import PointDataCSV
from snowexsql.db import db_session_with_credentials


def main():
    # Site name
    site_name = 'Grand Mesa'
    timezone = 'US/Mountain'
    doi = 'https://doi.org/10.5067/9IA978JIACAR'

    # Read in the Grand Mesa Snow Depths Data
    base = abspath(join('../download/data/SNOWEX/SNEX20_SD.001/'))

    profiles = glob.glob(join(base, '*/*.csv'))

    with db_session_with_credentials() as (_engine, session):
        for f in profiles:
            uploader = PointDataCSV(session,
                                    filename=f, 
                                    campaign_name=site_name,
                                    doi=doi,
                                    site_name=site_name, 
                                    timezone=timezone)
            uploader.submit()
if __name__ == '__main__':
    main()
