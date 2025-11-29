"""
Script to upload the Snowex Time Series pits
"""

import re
from pathlib import Path
from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials
from snowex_db.utilities import get_timezone_from_site_id, get_site_id_from_filename
from snowex_db.upload.layers import UploadProfileBatch

LOG = get_logger()


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


def main(file_list: list, doi: str) -> None:
    """
    Add 2020 timeseries pits
    """
    # Constant Metadata for the GPR data
    kwargs = {
        "campaign_name": "2020 Timeseries",
        "doi": doi,
    }

    # Regex to get site id from filename
    snowex_reg = r'SNEX20_TS_SP_\d{8}_\d{4}_([a-zA-Z0-9]*)_.*\.csv'

    # Files to ignore
    gap_filled_density = [f for f in file_list if "gapDensity" in f]
    file_list = list(set(file_list) - set(gap_filled_density))

    with db_session_with_credentials('./credentials.json') as (_engine, session):

        # Filter by instrument
        for keyword, instrument in INSTRUMENT_MAP.items():
            instrumented_files = [
                f for f in file_list if keyword in Path(f).name
            ]
            kwargs["instrument"] = instrument
            LOG.info(f"\n\nUploading {len(instrumented_files)} files with keyword: {keyword}")

            # Filter to sites to manage the timezones
            unique_sites = list(set([get_site_id_from_filename(f, snowex_reg) for f in instrumented_files]))
            
            for site in unique_sites:
                site_files = [
                    f for f in instrumented_files if site in f
                ]
                kwargs["timezone"] = get_timezone_from_site_id(site)
                
                uploader = UploadProfileBatch(session, site_files, **kwargs)
                uploader.push()


if __name__ == '__main__':
    for data_set_id, doi in SNOWEX_PITS_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)