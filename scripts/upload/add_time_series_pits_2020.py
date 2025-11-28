"""
Script to upload the Snowex Time Series pits
"""

import glob
import re
from pathlib import Path
from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials

from snowex_db.upload.layers import UploadProfileBatch

LOG = get_logger()


tz_map = {'US/Pacific': ['CA', 'NV', 'WA'],
          'US/Mountain': ['CO', 'ID', 'NM', 'UT', 'MT'],
          }

SNOWEX_PITS_MAP = {
    "SNEX20_TS_SP": "10.5067/KZ43HVLZV6G4"
}

# Filename keyword to the instrument used
INSTRUMENT_MAP = {
                    "siteDetails": None,
                    "density": "Density Cutter",
                    "temperature": "Digital Thermometer",
                    "LWC": "A2 Sensor",
                    "stratigraphy": "Manual"
                  }

def get_site_id(filename: str) -> str:
    """
    Get the site ID based on the site code in the filename
    """
    compiled = re.compile(
        r'SNEX20_TS_SP_\d{8}_\d{4}_([a-zA-Z0-9]*)_data_.*_v02\.csv'
    )
    match = compiled.match(Path(filename).name)
    if match:
        code = match.group(1)
        return code
    else:
        raise RuntimeError(f"No site ID found for {filename}")


def get_timezone(site_id: str) -> str:
    """
    Get the timezone based on the site code
    """
    abbrev = site_id[0:2]
    tz = [k for k, states in tz_map.items() if abbrev in states][0]
    return tz


def main(file_list: list, doi: str) -> None:
    """
    Add 2020 timeseries pits
    """
    # Constant Metadata for the GPR data
    kwargs = {
        "campaign_name": "2020 Timeseries",
        "doi": doi,
    }

    # Files to remove


    with db_session_with_credentials('./credentials.json') as (_engine, session):

        # Filter by instrument
        for keyword, instrument in INSTRUMENT_MAP.items():
            instrumented_files = [
                f for f in file_list if keyword in Path(f).name
            ]
            kwargs["instrument"] = instrument

            # Filter to sites to manage the timezones
            unique_sites = set([get_site_id(f) for f in instrumented_files])
            
            for site in unique_sites:
                site_files = [
                    f for f in instrumented_files if site in f
                ]
                kwargs["timezone"] = get_timezone(site)
                
                uploader = UploadProfileBatch(session, site_files, **kwargs)
                uploader.push()


if __name__ == '__main__':
    for data_set_id, doi in SNOWEX_PITS_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)