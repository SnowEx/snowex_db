

import glob
from os.path import abspath, join
import pandas as pd

from snowex_db.batch import UploadProfileBatch


def main():

    # Obtain a list of Grand mesa pits
    data_dir = abspath('../download/data/run_all/')
    # read in the descriptor file
    desc_df = pd.read_csv(join(data_dir, 'SNEX20_TS_SP_Summary_Environment_v01.csv'))
    errors = 0
    error_msg = []

    # get unique site_ids
    site_ids = desc_df['PitID'].unique()

    for site_id in site_ids:
        site_name = desc_df['Location'][desc_df['PitID'] == site_id].iloc[0]

        # Grab all the csvs in the pits folder
        filenames = glob.glob(join(data_dir, 'pits', f'{site_id}*/*.csv'))

        # Grab all the site details files
        sites = glob.glob(join(data_dir, 'pits', f'{site_id}*/*site*.csv'))

        # Grab all the perimeter depths and remove them for now.
        perimeter_depths = glob.glob(join(data_dir, 'pits', f'{site_id}*/*perimeter*.csv'))

        LWC = glob.glob(join(data_dir, 'pits', f'{site_id}*/*LWC*.csv'))

        # Remove the site details from the total file list to get only the
        profiles = list(set(filenames) - set(sites) - set(perimeter_depths) - set(LWC))
        # Submit all profiles associated with pit at a time
        b = UploadProfileBatch(
            filenames=profiles,
            debug=False, doi=None)
        b.push()
        error_msg += b.errors
    for f,m in error_msg:
        print(f)
    return len(errors)


if __name__ == '__main__':
    main()
