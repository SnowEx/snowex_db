"""
Read in the SnowEx 2020 profiles from pits.

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_profiles.py # To run individually
"""

import glob
from os.path import join
from pathlib import Path

from snowex_db.upload.layers import UploadProfileData
from snowexsql.db import db_session_with_credentials

def main():
    debug = True
    doi = "https://doi.org/10.5067/DUD2VZEVBJ7S"

    # Obtain a list of Grand mesa pits

    directory = Path(__file__).parent.parent / \
                "download/data/SNOWEX/SNEX20_GM_SP.001/"
    # Grab all the csvs in the pits folder
    filenames = glob.glob(join(directory, '*/*.csv'))
    # Grab all the site details files
    sites = glob.glob(join(directory, '*/*site*.csv'))
    summaries = glob.glob(join(directory, '*/*Summary*.csv'))
    # Remove the site details from the total file list to get only the profiles
    profiles = list(set(filenames) - set(sites) - set(summaries))

    with db_session_with_credentials() as (_engine, session):
        for f in sites:
            uploader = UploadProfileData(f, doi=doi, timezone='MST')
            uploader.submit(session)

        for f in profiles:          
            uploader = UploadProfileData(f, doi=doi, timezone='MST')
            uploader.submit(session)

if __name__ == '__main__':
    main()
