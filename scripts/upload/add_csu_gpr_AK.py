"""
Read in the SnowEx 2023 CSU GPR data collected at Farmers Loop and Creamers Field.

1. Data is preliminary and currently only available via email from Randall B.
2A. python run.py # To run all together all at once
2B. python add_gpr.py # To run individually

"""

from pathlib import Path
from snowexsql.db import get_db
from snowex_db.upload import PointDataCSV
import pandas as pd


def main():
    file = Path('../download/data/SnowEx223_FLCF_1GHz_GPR_CSU.csv')

    # Fix quirks
    df = pd.read_csv(file, dtype=str)

    # Upload is not able to handle the Notes col. So just remove it for now
    modified = file.parent.joinpath(file.stem + f'_mod{file.suffix}')
    print(f"Removing Notes Column prior to upload. Writing to {modified}")

    coi = [c for c in df.columns if c != 'Notes']

    # No time is a problem. Use 12 AKST == 9pm (21:00) UTC
    df['Time[HHMM]'] = '21:00'

    # Write out the modified version
    df[coi].to_csv(modified, index=False)


    kwargs = {
        # Keyword argument to upload depth measurements
        'depth_is_metadata': False,

        # Constant Metadata for the GPR data
        'site_name': 'farmers loop/creamers field',
        'observers': 'Randall Bonnell',
        'instrument': 'pulseEkko pro 1 GHz GPR',
        'in_timezone': 'UTC',
        'out_timezone': 'UTC',
        'doi': None, # Data is preliminary
        'epsg': 26906
    }

    # Grab a db connection to a local db named snowex
    db_name = 'localhost/snowex'
    engine, session = get_db(db_name, credentials='./credentials.json')

    # Instantiate the point uploader
    csv = PointDataCSV(modified, **kwargs)
    # Push it to the database
    csv.submit(session)

    # Close out the session with the DB
    session.close()

    # return the number of errors for run.py can report it
    return len(csv.errors)


if __name__ == '__main__':
    main()
