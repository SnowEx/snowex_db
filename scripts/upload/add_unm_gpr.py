"""
Read in the SnowEx 2020 UNM GPR. Upload SWE, Two Way Travel, Depth, to
the database.

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_gpr.py # To run individually

"""

import time
from os.path import abspath, expanduser, join

import pandas as pd

from snowexsql.db import get_db
from snowex_db.upload import *


def main():
    file = '../download/data/SNOWEX/SNEX20_UNM_GPR.001/2020.01.28/SNEX20_UNM_GPR.csv'

    kwargs = {
        # Keyword argument to upload depth measurements
        'depth_is_metadata': False,

        # Constant Metadata for the GPR data
        'site_name': 'Grand Mesa',
        'observers': 'Ryan Webb',
        'instrument': 'Mala 800 MHz GPR',
        'in_timezone': 'UTC',
        'out_timezone': 'UTC',
        'doi': 'https://doi.org/10.5067/WE9GI1GVMQF6'
    }

    # Break out the path and make it an absolute path
    file = abspath(expanduser(file))

    # Grab a db connection to a local db named snowex
    db_name = 'localhost/snowex'
    engine, session = get_db(db_name, credentials='./credentials.json')

    # Instantiate the point uploader
    csv = PointDataCSV(file, **kwargs)
    df_original = csv.df.copy()

    # Convert depth to centimeters
    csv.log.info('Converting depth to centimeters...')
    df_original['depth'] = df_original['depth'].mul(100)

    # Loop over the two insturments in the file and separate them for two submissions
    for hz in ['800', '1600']:
        # Change the instrument.
        csv.hdr.info['instrument'] = f'Mala {hz} MHz GPR',
        csv.log.info(f'Isolating {csv.hdr.info["instrument"]} data.')
        csv.df = df_original[df_original['freq_mhz'] == hz].copy()

        # Push it to the database
        csv.submit(session)

    # Close out the session with the DB
    session.close()

    # return the number of errors for run.py can report it
    return len(csv.errors)


if __name__ == '__main__':
    main()
