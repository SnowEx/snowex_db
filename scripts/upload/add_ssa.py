"""
Added ssa measurements to the database.
1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_ssa.py # To run individually
"""

import glob
from os.path import abspath, join

from snowex_db.upload.layers import UploadProfileData
from snowexsql.db import db_session_with_credentials
import logging


LOG = logging.getLogger('SSA Upload')

def main():

    # Obtain a list of SSA profiles
    directory = abspath('../download/data/SNOWEX/SNEX20_SSA.001/')
    filenames = glob.glob(join(directory, '*/*.csv'))

    LOG.info(f"Preparing to upload {len(filenames)} files to db.")

    with db_session_with_credentials('./credentials.json') as (
    engine, session):
        for f in filenames:
            LOG.info(f"Uploading {f}...")
            uploader = UploadProfileData(f, doi="https://doi.org/10.5067/SNMM6NGGKWIT", timezone='MST')
            uploader.submit(session)

    # Return the number of errors so run.py can keep track
    # return len(b.errors)


if __name__ == '__main__':
    main()
