"""
Script to upload the Snowex Time Series pits
"""

import glob
import re
from os.path import abspath, join
from pathlib import Path

from snowex_db.upload import PointDataCSV
from snowex_db import db_session


tz_map = {'US/Pacific': ['CA', 'NV', 'WA'],
          'US/Mountain': ['CO', 'ID', 'NM', 'UT', 'MT'],
          'US/Alaska': ["AK"]
          }


def main():
    """
    Add 2020 timeseries pits
    """
    db_name = 'localhost/snowex'
    # Preliminary data
    doi = "preliminary_alaska_pits"
    debug = True
    timezone = "US/Alaska"

    # Point to the downloaded data from
    data_dir = abspath('../download/data/SNEX23_preliminary/Data/pits')
    error_msg = []

    # Files to ignore
    ignore_files = [
        "SnowEx23_SnowPits_AKIOP_Summary_Environment_v01.csv",
        "SnowEx23_SnowPits_AKIOP_Summary_SWE_v01.csv",
        "SnowEx23_SnowPits_AKIOP_Summary_SWE_v01_modified.csv"
    ]

    # Get all the date folders
    unique_folders = Path(
        data_dir
    ).expanduser().absolute().glob("ALASKA*/*20*SNOW_PIT")
    for udf in unique_folders:
        # get all the csvs in the folder
        dt_folder_files = list(udf.glob("*.csv"))
        site_ids = []
        # Get the unique site ids for this date folder
        compiled = re.compile(
            r'SnowEx23_SnowPits_AKIOP_([a-zA-Z0-9]*)_\d{8}.*_v01\.csv'
        )
        for file_path in dt_folder_files:
            file_name = file_path.name
            if file_name in ignore_files:
                print(f"Skipping {file_name}")
                continue
            match = compiled.match(file_name)
            if match:
                code = match.group(1)
                site_ids.append(code)
            else:
                raise RuntimeError(f"No site ID found for {file_name}")

        # Get the unique site ids
        site_ids = list(set(site_ids))

        for site_id in site_ids:
            # Grab all the csvs in the pits folder
            filenames = glob.glob(join(str(udf), f'*_{site_id}_*.csv'))

            # Grab all the site details files
            sites = glob.glob(join(
                str(udf), f'*_{site_id}_*siteDetails*.csv'
            ))

            # Use no-gap-filled density
            density_files = glob.glob(join(
                str(udf), f'*_{site_id}_*_gapFilled_density*.csv'
            ))

            # Remove the site details from the total file list to get only the
            profiles = list(
                set(filenames) - set(sites) -
                set(density_files)  # remove non-gap-filled denisty
            )

            # Submit all profiles associated with pit at a time
            b = UploadProfileBatch(
                filenames=profiles, debug=debug, doi=doi,
                in_timezone=timezone,
                db_name=db_name,
                allow_split_lines=True,  # Logic for split header lines
                header_sep=":"
            )
            b.push()
            error_msg += b.errors

            # Upload the site details
            sd = UploadSiteDetailsBatch(
                filenames=sites, debug=debug, doi=doi,
                in_timezone=timezone,
                db_name=db_name
            )
            sd.push()
            error_msg += sd.errors

    for f, m in error_msg:
        print(f)
    return len(error_msg)


if __name__ == '__main__':
    main()
