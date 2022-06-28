"""
Script to upload the Snowex Time Series pits
"""

import glob
from os.path import abspath, join
import pandas as pd

from snowex_db.batch import UploadProfileBatch
from snowex_db.upload import PointDataCSV
from snowexsql.db import get_db

def main():
    """
    Currenltly based on the preliminary downloaded zip which has not been submitted yet.
    Folder name is SNEX20_TS_SP_preliminary_v4
    """
    doi = None
    debug = True

    # Point to the downloaded data from
    data_dir = abspath('../download/data/SNEX20_TS_SP_preliminary_v5/')
    # read in the descriptor file
    desc_df = pd.read_csv(join(data_dir, 'SNEX20_TS_SP_Summary_Environment_v01.csv'))
    error_msg = []

    # get unique site_ids
    site_ids = desc_df['PitID'].unique()

    for site_id in site_ids:
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
            debug=debug, doi=doi)
        b.push()
        error_msg += b.errors

        # Submit all perimeters as point data
        engine, session = get_db('localhost/snowex', credentials='credentials.json')
        for fp in perimeter_depths:
            pcsv = PointDataCSV(fp, doi=doi, debug=debug, depth_is_metadata=False)
            pcsv.submit(session)
        session.close()

    for f, m in error_msg:
        print(f)
    return len(error_msg)


if __name__ == '__main__':
    main()
