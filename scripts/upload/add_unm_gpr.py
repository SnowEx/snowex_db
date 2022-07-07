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
    filename = '../download/data/SNOWEX/SNEX20_UNM_GPR.001/2020.01.28/SNEX20_UNM_GPR.csv'

    kwargs = {
        # Keyword argument to upload depth measurements
        'depth_is_metadata': False,

        # Constant Metadata for the GPR data
        'site_name': 'Grand Mesa',
        'observers': 'Ryan Webb',
        'instrument': None, # See loop below
        'in_timezone': 'UTC',
        'out_timezone': 'UTC',
        'doi': 'https://doi.org/10.5067/WE9GI1GVMQF6',
        'epsg': 26912
    }

    # Break out the path and make it an absolute path
    filename = abspath(expanduser(filename))

    # Grab a db connection to a local db named snowex
    db_name = 'localhost/snowex'
    engine, session = get_db(db_name, credentials='./credentials.json')

    # Read in for management of instruments
    df_raw = pd.read_csv(filename)
    low_freq = df_raw['FREQ_MHz'] == 800
    hi_freq = df_raw['FREQ_MHz'] == 1600

    # Instantiate the point uploader
    csv = PointDataCSV(filename, **kwargs)

    # Convert depth to centimeters
    csv.log.info('Converting depth to centimeters...')
    csv.df['depth'] = csv.df['depth'].mul(100)
    df_original = csv.df.copy()

    # Loop over the two insturments in the file and separate them for two submissions
    for hz, ind in [(800, low_freq), (1600, hi_freq)]:
        instrument = f'Mala {hz} MHz GPR'
        csv.log.info(f'Isolating {instrument} data for upload.')
        csv.df = df_original[ind].copy()
        # Change the instrument.
        csv.df['instrument'] = instrument
        # Push it to the database
        csv.submit(session)

    # Close out the session with the DB
    session.close()

    # return the number of errors for run.py can report it
    return len(csv.errors)


if __name__ == '__main__':
    main()
