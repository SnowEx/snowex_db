"""
Added ssa measurements to the database.
1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_ssa.py # To run individually
"""

import glob
from os.path import abspath, join

from snowex_db.upload.layers import UploadProfileData
from snowex_db.utilities import db_session_with_credentials

def main():

    # Obtain a list of SSA profiles
    directory = abspath('../download/data/SNOWEX/SNEX20_SSA.001/')
    filenames = glob.glob(join(directory, '*/*.csv'))

    with db_session_with_credentials('localhost/snowex', './credentials.json') as (
    session, engine):
        for f in filenames[0:5]:
                uploader = UploadProfileData(f, doi="https://doi.org/10.5067/SNMM6NGGKWIT", timezone='MST')
                uploader.submit(session)

    # Return the number of errors so run.py can keep track
    # return len(b.errors)


if __name__ == '__main__':
    main()
