"""
Script to upload the Snowex Time Series pits
"""

import glob
import re
from os.path import abspath, join
from pathlib import Path

from snowex_db.batch import UploadProfileBatch, UploadSiteDetailsBatch
from snowex_db.upload import PointDataCSV
from snowex_db import db_session


tz_map = {'US/Pacific': ['CA', 'NV', 'WA'],
          'US/Mountain': ['CO', 'ID', 'NM', 'UT', 'MT'],
          }


def main():
    """
    Add 2020 timeseries pits
    """
    db_name = 'localhost/snowex'

    # Version 2 DOI
    # https://nsidc.org/data/snex20_ts_sp/versions/2
    doi = "https://doi.org/10.5067/KZ43HVLZV6G4"
    debug = True

    # Point to the downloaded data from
    data_dir = abspath('../download/data/SNOWEX/SNEX20_TS_SP.002/')
    error_msg = []

    # Files to ignore
    ignore_files = [
        "SNEX20_TS_SP_Summary_Environment_v02.csv",
        "SNEX20_TS_SP_Summary_SWE_v02.csv",
        "SNEX20_TS_SP_Summary_SWE_v02_modified.csv"
    ]

    # Get all the date folders
    unique_dt_olders = Path(
        data_dir
    ).expanduser().absolute().glob("20*.*.*")
    for udf in unique_dt_olders:
        # get all the csvs in the folder
        dt_folder_files = list(udf.glob("*.csv"))
        site_ids = []
        # Get the unique site ids for this date folder
        compiled = re.compile(
            r'SNEX20_TS_SP_\d{8}_\d{4}_([a-zA-Z0-9]*)_data_.*_v02\.csv'
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
            abbrev = site_id[0:2]
            tz = [k for k, states in tz_map.items() if abbrev in states][0]

            # Grab all the csvs in the pits folder
            filenames = glob.glob(join(str(udf), f'*_{site_id}_*.csv'))

            # Grab all the site details files
            sites = glob.glob(join(
                str(udf), f'*_{site_id}_*siteDetails*.csv'
            ))

            # Grab all the perimeter depths and remove them for now.
            perimeter_depths = glob.glob(join(
                str(udf), f'*_{site_id}_*perimeterDepths*.csv'
            ))

            # Use no-gap-filled density for the sole reason that
            # Gap filled density for profiles where the scale was broken
            # are just an empty file after the headers. We should
            # Record that Nan density was collected for the profile
            density_files = glob.glob(join(
                str(udf), f'*_{site_id}_*_gapFilledDensity_*.csv'
            ))

            # Remove the site details from the total file list to get only the
            profiles = list(
                set(filenames) - set(sites) - set(perimeter_depths) -
                set(density_files)  # remove non-gap-filled denisty
            )

            # Submit all profiles associated with pit at a time
            b = UploadProfileBatch(
                filenames=profiles, debug=debug, doi=doi, in_timezone=tz,
                db_name=db_name,
                allow_split_lines=True  # Logic for split header lines
            )
            b.push()
            error_msg += b.errors

            # Upload the site details
            sd = UploadSiteDetailsBatch(
                filenames=sites, debug=debug, doi=doi, in_timezone=tz,
                db_name=db_name
            )
            sd.push()
            error_msg += sd.errors

            # Submit all perimeters as point data
            with db_session(
                db_name, credentials='credentials.json'
            ) as (session, engine):
                for fp in perimeter_depths:
                    pcsv = PointDataCSV(
                        fp, doi=doi, debug=debug, depth_is_metadata=False,
                        in_timezone=tz,
                        allow_split_lines=True  # Logic for split header lines
                    )
                    pcsv.submit(session)

    for f, m in error_msg:
        print(f)
    return len(error_msg)


if __name__ == '__main__':
    main()
