"""
Read in the SnowEx 2020 Decimated GPR data. Uploaded SWE, Two Way Travel, Depth, to
the database.

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_gpr.py # To run individually

"""

from os.path import abspath, expanduser

from snowexsql.db import db_session_with_credentials
from snowex_db.upload.points import PointDataCSV


def main():
    file = ('../download/data/SNOWEX/SNEX20_BSU_GPR.001/'
            '2020.01.28/SNEX20_BSU_GPR_pE_01282020_01292020_02042020.csv')

    kwargs = {
        # Constant Metadata for the GPR data
        'campaign_name': 'Grand Mesa',
        'observer': 'Tate Meehan',
        'instrument': 'gpr',
        'instrument_model': 'pulse EKKO Pro multi-polarization 1 GHz GPR',
        'timezone': 'UTC',
        'doi': 'https://doi.org/10.5067/Q2LFK0QSVGS2',
        'name': 'BSU GPR Data',
    }

    # Break out the path and make it an absolute path
    file = abspath(expanduser(file))

    # Grab a db connection
    with db_session_with_credentials() as (_engine, session):
        # Instantiate the point uploader
        csv = PointDataCSV(file, **kwargs)
        # Push it to the database
        csv.submit(session)

    # return the number of errors for run.py can report it
    # return len(csv.errors)


if __name__ == '__main__':
    main()
