"""
Read in the SnowEx 2020 profiles from pits.
"""

from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials

from snowex_db.upload.layers import UploadProfileData

LOG = get_logger()

# Map of DATA SET ID to DOI from NSIDC
# * https://nsidc.org/data/snex20_gm_sp/versions/1
IOP_PIT_DOI = {
    "SNEX20_GM_SP": "10.5067/DUD2VZEVBJ7S",
}

INSTRUMENT_FILE_MAP = {
    "density": "Density Cutter",
    "temperature": "Thermometer",
    "lwc": "A2 Sensor",
    "stratigraphy": "Manual",
}

def main(file_list: list, doi: str) -> None:
    # Filter to CSV only
    file_list = [file for file in file_list if str(file).lower().endswith(".csv")]

    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")
    
    with db_session_with_credentials() as (_engine, session):
        for pit_data in file_list:
            # Skipping summary files, all information is in each CSV
            if "Summary" in pit_data:
                continue

            instrument = INSTRUMENT_FILE_MAP.get(pit_data.split("_")[-2].lower(), "")
            LOG.info(f"Uploading {pit_data} file.")
            pid_upload = UploadProfileData(
                session, filename=pit_data, doi=doi, instrument=instrument
            )
            pid_upload.submit()

if __name__ == '__main__':
    for data_set_id, doi in IOP_PIT_DOI.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
