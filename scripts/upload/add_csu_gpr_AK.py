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

    # Fix quirks
    df = pd.read_csv(file, dtype=str)

    # Upload is not able to handle the Notes col. So just remove it for now
    modified = file.parent.joinpath(file.stem + f'_mod{file.suffix}')
    print(f"Removing Notes Column prior to upload. Writing to {modified}")


    # No time is a problem. Use 12 AKST == 9pm (21:00) UTC
    # df['Time[HHMM]'] = '21:00'

    # Write out the modified version
    # df[coi].to_csv(modified, index=False)

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
