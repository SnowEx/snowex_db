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
    Currenltly based on the preliminary downloaded zip which has not been submitted yet.
    Folder name is SNEX20_TS_SP_preliminary_v4
    """
    # TODO: write script to clear out the timeseries pits
    #       * maybe delete all pits and then add them back in
    # Version 2 DOI
    # https://nsidc.org/data/snex20_ts_sp/versions/2
    doi = "https://doi.org/10.5067/KZ43HVLZV6G4"
    debug = True

    # TODO: new header of
    #    Pit Comments
    #    Parameter Codes

    # Point to the downloaded data from
    data_dir = abspath('../download/data/SNOWEX/SNEX20_TS_SP.002/')
    error_msg = []

    # OLD file name
    #   pits/CAAMCL_20191220_1300/SNEX20_TS_SP_20191220_1300_CAAMCL_LWC_v01.csv
    # NEW FILE NAME
    #   2019.10.24/SNEX20_TS_SP_20191024_1322_COFEJ2_data_LWC_v02.csv

    # Get all the date folders
    unique_dt_olders = Path(
        data_dir
    ).expanduser().absolute().glob("20*.*.*")
    for udf in unique_dt_olders:
        # get all the csvs in the folder
        dt_folder_files = list(udf.glob("*.csv"))
        all_file_names = [f.name for f in dt_folder_files]
        site_ids = []
        # Get the unique site ids for this date folder
        compiled = re.compile(
            r'SNEX20_TS_SP_\d{8}_\d{4}_([a-zA-Z0-9]*)_data_.*_v02\.csv'
        )
        for file_name in all_file_names:
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

            # all non-gapped filled_density
            non_filled_density = glob.glob(join(
                str(udf), f'*_{site_id}_*_density_*.csv'
            ))

            # Remove the site details from the total file list to get only the
            profiles = list(
                set(filenames) - set(sites) - set(perimeter_depths) -
                set(non_filled_density)  # remove non-gap-filled denisty
            )

            # Submit all profiles associated with pit at a time
            b = UploadProfileBatch(
                filenames=profiles,
                debug=debug, doi=doi,
                in_timezone=tz)
            b.push()
            error_msg += b.errors

            # Upload the site details
            sd = UploadSiteDetailsBatch(filenames=sites,
                                        debug=debug,
                                        doi=doi,
                                        in_timezone=tz)
            sd.push()
            error_msg += sd.errors

            # Submit all perimeters as point data
            with db_session(
                'localhost/snowex', credentials='credentials.json'
            ) as session:
                for fp in perimeter_depths:
                    pcsv = PointDataCSV(
                        fp, doi=doi, debug=debug, depth_is_metadata=False,
                        in_timezone=tz
                    )
                    pcsv.submit(session)

    for f, m in error_msg:
        print(f)
    return len(error_msg)


if __name__ == '__main__':
    main()
