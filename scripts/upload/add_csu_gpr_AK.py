"""
Read in the SnowEx 2023 CSU GPR data collected at Farmers Loop and Creamers Field.

1. Data is preliminary and currently only available via email from Randall B.
2A. python run.py # To run all together all at once
2B. python add_gpr.py # To run individually

"""

from os.path import abspath, expanduser
from pathlib import Path
from snowexsql.db import db_session_with_credentials
from snowex_db.upload.points import PointDataCSV
import pandas as pd


# Issue #71

def main():
    file = Path('../download/data/SnowEx223_FLCF_1GHz_GPR_CSU.csv')

    kwargs = {
        # Constant Metadata for the GPR data
        'campaign_name': 'farmers-creamers',  # TODO: should this be AK-something?
        'observer': 'Randall Bonnell',
        'instrument': 'gpr',
        'instrument_model': 'pulseEkko pro 1 GHz GPR',
        'timezone': 'UTC',
        'doi': None,  # TODO: presumably this exists now?
        'name': 'CSU GPR Data',
    }

    # Break out the path and make it an absolute path
    file = abspath(expanduser(file))

    # Grab a db connection
    with db_session_with_credentials() as (_engine, session):
        # Instantiate the point uploader
        csv = PointDataCSV(session, file, **kwargs)
        # Push it to the database
        csv.submit()

    # return the number of errors for run.py can report it
    # return len(csv.errors)


if __name__ == '__main__':
    main()
