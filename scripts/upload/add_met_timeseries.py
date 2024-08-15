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
from metloom.pointdata.snowex import SnowExMetInfo

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

    # Variables we will use
    variable_unit_map = {
        "RH_10ft": {
            "units": "percent",
            "notes": "Relative humidity measured at 10 ft tower level",
            "instrument": "Campbell Scientific HC2S3"
        },
        # "RH_20ft": "percent",
        "BP_kPa_Avg": {
            "units": "kPa",
            "notes": "Barometric pressure",
            "instrument": "Campbell Scientific CS106",
        },
        # "AirTC_20ft_Avg": "degrees Celcius",
        "AirTC_10ft_Avg": {
            "units": "degrees Celcius",
            "notes": "Air temperature measured at 10 ft tower level",
            "instrument": "Campbell Scientific HC2S3"
        },
        # "WSms_20ft_Avg": "m/s",
        "WSms_10ft_Avg": {
            "units": "m/s",
            "notes": "Vector mean wind speed measured at 10 ft tower level",
            "instrument": "R.M. Young 05103",
        },
        "WindDir_10ft_D1_WVT": {
            "units": "degrees",
            "notes": "Vector mean wind direction measured at 10 ft tower level",
            "instrument": "R.M. Young 05103",
        },
        # "WindDir_20ft_D1_WVT": "degrees",
        "SUp_Avg": {
            "units": "W/m^2",
            "notes": "Shortwave radiation measured with upward-facing sensor",
            "instrument": "Kipp and Zonnen CNR4",
        },
        "SDn_Avg": {
            "units": "W/m^2",
            "notes": "Shortwave radiation measured with downward-facing sensor",
            "instrument": "Kipp and Zonnen CNR4",
        },
        "LUpCo_Avg": {
            "units": "W/m^2",
            "notes": "Longwave radiation measured with upward-facing sensor",
            "instrument": "Kipp and Zonnen CNR4",
        },
        "LDnCo_Avg": {
            "units": "W/m^2",
            "notes": "Longwave radiation measured with downward-facing sensor",
            "instrument": "Kipp and Zonnen CNR4",
        },
        # "SM_5cm_Avg": None,
        "SM_20cm_Avg": {
            "units": None,
            "notes": "Soil moisture measured at 10 cm below the soil",
            "instrument": "Stevens Water Hydraprobe II",
        },
        # "SM_50cm_Avg": None,
        # "TC_5cm_Avg": "degrees Celcius",
        "TC_20cm_Avg": {
            "units": "degrees Celcius",
            "notes": "Soil temperature measured at 10 cm below the soil",
            "instrument": "Stevens Water Hydraprobe II",
        },
        # "TC_50cm_Avg": "degrees Celcius",
        # "DistanceSensToGnd(m)",
        "SnowDepthFilter(m)": {
            "units": "m",
            "notes": "Temperature corrected, derived snow surface height (filtered)",
            "instrument": "Campbell Scientific SR50A",
        },
    }

    errors = 0
    with db_session(
            db_name, credentials='credentials.json'
    ) as (session, engine):

        for stn_obj in SnowExMetInfo:
            f = join(base, stn_obj.path)
            # Read in the file
            df = pd.read_csv(f)
            # add location info
            df["latitude"] = [stn_obj.latitude] * len(df)
            df["longitude"] = [stn_obj.longitude] * len(df)
            df = df.set_index("TIMESTAMP")
            # SITE ID - use station id
            df["site"] = [stn_obj.station_id] * len(df)
            df["observer"] = ["P. Houser"] * len(df)

            # Split variables into their own files
            for v, info in variable_unit_map.items():
                unit = info["units"]

                df_cut = df.loc[
                    :, [v, "latitude", "longitude", "site"]
                ]
                df_cut["instrument"] = [info["instrument"]] * len(df_cut)

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
