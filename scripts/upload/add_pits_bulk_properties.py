"""
Script to upload the Snowex Time Series pits
"""

import glob
import re
from os.path import abspath, join
from pathlib import Path

import pandas as pd

from snowex_db.upload import PointDataCSV
from snowex_db import db_session


def main():
    """
    Add bulk SWE, Depth, Density for 2020 and 2021 timeseires pits
    """
    db_name = 'localhost/snowex'
    debug = True

    # Point to the downloaded data from
    data_dir = abspath('../download/data/SNOWEX/')
    error_msg = []

    path_details = [
        {
            "DOI": "https://doi.org/10.5067/KZ43HVLZV6G4",
            "path": "SNEX20_TS_SP.002/2019.10.24/SNEX20_TS_SP_Summary_SWE_v02.csv"
        },
        {
            "DOI": "https://doi.org/10.5067/QIANJYJGRWOV",
            "path": "SNEX21_TS_SP.001/2020.11.16/SNEX21_TS_SP_Summary_SWE_v01.csv"
        },
        # Preliminary data from 2023 Alask pits
        {
            "DOI": "preliminary_alaska_pits",
            "path": "../SNEX23_preliminary/Data/SnowEx23_SnowPits_AKIOP_Summary_SWE_v01.csv"
        }
    ]
    for info in path_details:
        doi = info["DOI"]
        file_path = join(data_dir, info["path"])
        # Read csv and dump new one without the extra header lines
        df = pd.read_csv(
            file_path,
            skiprows=list(range(32)) + [33]
        )
        new_name = file_path.replace(".csv", "_modified.csv")
        # Filter to columns we want (density, swe, etc)
        columns = [
            'Location', 'Site', 'PitID', 'Date/Local Standard Time', 'UTM Zone',
            'Easting (m)', 'Northing (m)', 'Latitude (deg)', 'Longitude (deg)',
            'Density Mean (kg/m^3)',
            'SWE (mm)', 'HS (cm)', "Snow Void (cm)", 'Flag'
        ]
        df_columns = df.columns.values
        filtered_columns = [c for c in columns if c in df_columns]
        df = df.loc[:, filtered_columns]
        df.to_csv(new_name, index=False)

        # Submit SWE file data as point data
        with db_session(
            db_name, credentials='credentials.json'
        ) as (session, engine):
            pcsv = PointDataCSV(
                new_name, doi=doi, debug=debug,
                depth_is_metadata=False,
                row_based_crs=True,
                row_based_timezone=True
            )
            pcsv.submit(session)


if __name__ == '__main__':
    main()
