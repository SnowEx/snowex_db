"""
Script to upload the Snowex Time Series pits
"""

import glob
import re
from os.path import abspath, join
from pathlib import Path

from snowex_db.upload.layers import UploadProfileData
from snowexsql.db import db_session_with_credentials

tz_map = {'US/Pacific': ['CA', 'NV', 'WA'],
          'US/Mountain': ['CO', 'ID', 'NM', 'UT', 'MT'],
          }


def main():
    """
    Snowex 2021 timeseries pits
    """
    db_name = 'localhost/test'
    # https://nsidc.org/data/snex21_ts_sp/versions/1
    doi = "https://doi.org/10.5067/QIANJYJGRWOV"
    debug = True

    # Point to the downloaded data from
    data_dir = Path('../download/data/SNOWEX/SNEX21_TS_SP.001/').absolute().expanduser()
    error_msg = []

    # Files to ignore
    ignore_files = [
        "SNEX21_TS_SP_Summary_Environment_v01.csv",
        "SNEX21_TS_SP_Summary_SWE_v01.csv",
        "SNEX21_TS_SP_Summary_SWE_v01_modified.csv"
    ]

    with db_session_with_credentials("./credentials.json") as (engine, session):
        # Upload all the site details files
        site_files = list(data_dir.glob('**/*siteDetails*.csv'))
        # Upload each site file
        for site_file in site_files:
            u = UploadProfileData(
                str(site_file),
                doi=doi,
                timezone='MST',
            )
            u.submit(session)

        # find and upload all data files
        for data_file in data_dir.glob("**/*_data_*_v01.csv"):
            if "_gapFilledDensity_" in data_file.name:
                # Use no-gap-filled density for the sole reason that
                # Gap filled density for profiles where the scale was broken
                # are just an empty file after the headers. We should
                # Record that Nan density was collected for the profile
                print(f"Not uploading gap filled density {data_file}")
            else:
                # Instantiate the uploader
                u = UploadProfileData(
                    str(data_file),
                    doi=doi,
                    timezone='MST',
                )
                u.submit(session)


if __name__ == '__main__':
    main()
