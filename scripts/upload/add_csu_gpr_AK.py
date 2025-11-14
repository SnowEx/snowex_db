"""
Read in the SnowEx 2023 CSU GPR data collected in Alaska 2023
"""

from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials

from snowex_db.upload.points import PointDataCSV

LOG = get_logger()

GPR_CSU_MAP = {
    "SNEX23_CSU_GPR": "10.5067/3X5Q3X7Y87U3"
}

SITE_MAP = {
    "FLLCF": "Farmers Loop/Creamers Field",
    "BCEF": "Cumulative Experimental Forest",
    "CPCW": "Caribou/Poker Creek Research Watershed"
}

def main(file_list: list, doi: str) -> None:

    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")

    kwargs = {
        # Constant Metadata for the GPR data
        "campaign_name": "Alaska 2023",
        "observer": "Randall Bonnell",
        "instrument": "gpr",
        "instrument_model": "1 GHz GPR",
        "timezone": "UTC",
        "doi": doi,
        "name": "CSU GPR Data",
    }

    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            LOG.info(f"Uploading {file} file.")
            site = SITE_MAP.get(file.split("_")[6].upper(), "")
            # Instantiate the point uploader
            uploader = PointDataCSV(session, file, id=site, **kwargs)
            # Push it to the database
            uploader.submit()


if __name__ == '__main__':
    for data_set_id, doi in GPR_CSU_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
