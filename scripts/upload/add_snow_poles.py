"""
Uploads the SnowEx 2020 depths derived from cameras looking at poles to the database

1. Downloaded from Catherine Breen email. Later to be updated with a NSIDC DOI
2A. python run.py # To run all together all at once
2B. python add_snow_poles.py # To run individually
"""

import time
from os.path import abspath

from snowexsql.db import get_db
from snowex_db.upload import *


def main():
    # Read in the Grand Mesa Snow Depths Data
    f = abspath('../download/data/SNOWEX/SNEX20_SD_TLI.001/2019.09.29/SNEX20_SD_TLI_clean.csv')

    # Start the Database
    db_name = 'localhost/snowex'
    engine, session = get_db(db_name, credentials='./credentials.json')

    csv = PointDataCSV(
        f,
        depth_is_metadata=False,
        units='cm',
        site_name='Grand Mesa',
        observers='Catherine Breen',
        instrument='camera',
        in_timezone='MST',
        doi='https://doi.org/10.5067/14EU7OLF051V',
        epsg=26912)

    csv.submit(session)
    errors = len(csv.errors)

    return errors


if __name__ == '__main__':
    main()
