"""
Script to upload the Snowex Time Series pits
"""

import glob
from os.path import abspath, join
import pandas as pd

from snowex_db.batch import UploadProfileBatch, UploadSiteDetailsBatch
from snowex_db.upload import PointDataCSV
from snowexsql.db import get_db

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
    # TODO: fill in this DOI
    doi = None
    debug = True

    # Point to the downloaded data from
    # TODO: update local path
    data_dir = abspath('../download/data/SNEX20_TS_SP_preliminary_v5/')
    # read in the descriptor file
    # TODO: check this path
    desc_df = pd.read_csv(join(data_dir, 'SNEX20_TS_SP_Summary_Environment_v01.csv'))
    error_msg = []

    # get unique site_ids
    site_ids = desc_df['PitID'].unique()

    for site_id in site_ids:
        abbrev = site_id[0:2]
        tz = [k for k, states in tz_map.items() if abbrev in states][0]

        # Grab all the csvs in the pits folder
        filenames = glob.glob(join(data_dir, 'pits', f'{site_id}*/*.csv'))

        # Grab all the site details files
        sites = glob.glob(join(data_dir, 'pits', f'{site_id}*/*site*.csv'))

        # Grab all the perimeter depths and remove them for now.
        perimeter_depths = glob.glob(join(data_dir, 'pits', f'{site_id}*/*perimeter*.csv'))

        # Remove the site details from the total file list to get only the
        profiles = list(set(filenames) - set(sites) - set(perimeter_depths))

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
        engine, session = get_db('localhost/snowex', credentials='credentials.json')
        for fp in perimeter_depths:
            pcsv = PointDataCSV(fp, doi=doi, debug=debug, depth_is_metadata=False, in_timezone=tz)
            pcsv.submit(session)
        session.close()

    for f, m in error_msg:
        print(f)
    return len(error_msg)


if __name__ == '__main__':
    main()
