"""
Uploads SnowEx temporary met stations to the database

Source: https://nsidc.org/data/snex_met/versions/1
User guide: https://nsidc.org/sites/default/files/documents/user-guide/snex_met-v001-userguide.pdf

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_met_timeseries.py # To run individually
"""

import glob
import time
from os.path import abspath, join

import pandas as pd
from snowexsql.db import get_db
from snowex_db.upload import *
from snowex_db import db_session


def main():
    # Site name
    start = time.time()
    site_name = 'Grand Mesa'
    timezone = 'MST'

    # Read in the Grand Mesa Snow Depths Data
    base = abspath(join('../download/data/SNOWEX/SNEX_Met.001/'))

    # Start the Database
    db_name = 'localhost/test'

    csvs = glob.glob(join(base, '*/*.csv'))

    # Location mapping from the user guide
    location_mapping = {
        "GMSP": [39.05084, -108.06144],
        "LSOS": [39.05225, -108.09792],
        "ME": [39.10358, -107.88383],
        "MM": [39.03954, -107.94174],
        "MW": [39.03388, -108.21399],
    }

    variable_unit_map = {
        "RH_10ft": "percent",
        "RH_20ft": "percent",
        "BP_kPa_Avg": "kPa",
        "AirTC_20ft_Avg": "degrees Celcius",
        "AirTC_10ft_Avg": "degrees Celcius",
        "WSms_20ft_Avg": "m/s",
        "WSms_10ft_Avg": "m/s",
        "WindDir_10ft_D1_WVT": "degrees",
        "WindDir_20ft_D1_WVT": "degrees",
        "SUp_Avg": "W/m^2",
        "SDn_Avg": "W/m^2",
        "LUpCo_Avg": "W/m^2",
        "LDnCo_Avg": "W/m^2",
        "SM_5cm_Avg": None,
        "SM_20cm_Avg": None,
        "SM_50cm_Avg": None,
        "TC_5cm_Avg": "degrees Celcius",
        "TC_20cm_Avg": "degrees Celcius",
        "TC_50cm_Avg": "degrees Celcius",
        # "DistanceSensToGnd(m)",
        "SnowDepthFilter(m)": "m"
    }

    errors = 0
    with db_session(
            db_name, credentials='credentials.json'
    ) as (session, engine):

        for f in csvs:
            # find the point relative to the file
            point_id = f.split("Met_")[-1].split("_final")[0]
            # get location info from the point id
            lat, lon = location_mapping[point_id]

            # Read in the file
            df = pd.read_csv(f)
            # add location info
            df["latitude"] = [lat] * len(df)
            df["longitude"] = [lon] * len(df)
            df = df.set_index("TIMESTAMP")
            # TODO: what do we do with site_id? is MM the site id?
            #   we can add it as "site" to the df if it is
            df["site"] = [point_id] * len(df)

            # TODO: how do we handle to different heights?
            #  use layer data?

            # Split variables into their own files
            for v, unit in variable_unit_map.items():
                df_cut = df.loc[
                    :, [v, "latitude", "longitude", "site"]
                ]

                new_f = f.replace(".csv", f"local_mod_{v}.csv")
                df_cut.to_csv(new_f, index_label="datetime")
                csv = PointDataCSV(
                    new_f,
                    depth_is_metadata=False,
                    units=unit,
                    site_name=site_name,
                    in_timezone=timezone,
                    epsg=26912,
                    doi="https://doi.org/10.5067/497NQVJ0CBEX")

                csv.submit(session)
                errors += len(csv.errors)

    return errors


if __name__ == '__main__':
    main()
