"""
Read in the SnowEx 2020 UNM GPR. Upload SWE, Two Way Travel, Depth, to
the database.

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_gpr.py # To run individually

"""

from pathlib import Path

import pandas as pd

from snowexsql.db import db_session_with_credentials
from snowex_db.upload.points import PointDataCSV


# Issue #72

def main():
    filename = ('../download/data/nsidc-cumulus-prod-protected/SNOWEX/'
                'SNEX20_UNM_GPR/1/2020/01/28/SNEX20_UNM_GPR.csv')

    kwargs = {
        # Constant Metadata for the GPR data
        'observer': 'Ryan Webb',
        'doi': 'https://doi.org/10.5067/WE9GI1GVMQF6',
        'campaign_name': 'Grand Mesa',
        'instrument': 'gpr',
        # 'instrument_model': 'pulse EKKO Pro multi-polarization 1 GHz GPR',
        'timezone': 'UTC',
        'name': 'UNM GPR Data',
    }

    # Break out the path and make it an absolute path
    file = Path(filename).absolute().resolve()

    # Make two files, filter by frequency
    # Read in for management of instruments
    df_raw = pd.read_csv(filename)

    # Grab a db connection
    with db_session_with_credentials() as (_engine, session):
        for freq in [800, 1600]:
            new_file_name = file.parent.joinpath(f'{file.stem}_{freq}{file.suffix}')
            # Filter the data by frequency
            df_filtered = df_raw[df_raw['FREQ_MHz'] == freq]
            # convert depth to cm
            df_filtered['depth'] = df_filtered['depth'].mul(100)
            # Save the filtered data to a new file
            df_filtered.to_csv(new_file_name, index=False)
            specific_kwargs = dict(
                instrument_model=f'Mala {freq} MHz GPR'
            )
            # Instantiate the point uploader
            csv = PointDataCSV(
                session, str(new_file_name), **{**kwargs, **specific_kwargs}
            )
            # Push it to the database
            csv.submit()


if __name__ == '__main__':
    main()
