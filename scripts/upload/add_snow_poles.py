"""
Script to uploads the Snowex Snow Pole data to the database\
"""

from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials
from snowex_db.upload.points import PointDataCSV
LOG = get_logger()

SNOWEX_POLES_MAP = {
    "SNEX20_SD_TLI": "10.5067/14EU7OLF051V"
}

def main(file_list: list, doi: str) -> None:

    # Data to skip for now
    ignore_data = [
        "raw",
    ]

    # Filter to CSV and relevant data
    file_list = [
        file
        for file in file_list
        if not any(name in str(file) for name in ignore_data)
    ]

    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")

    kwargs = {
        "site_name": "Grand Mesa",
        "campaign_name": "Grand Mesa",
        "timezone": "US/Mountain",
        "instrument": "camera",
        "observer": "Catherine Breen",
        "units": "cm",
        "doi": doi,
    }

    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            uploader = PointDataCSV(session,
                                    file, 
                                    **kwargs)
            uploader.submit()


if __name__ == '__main__':
    for data_set_id, doi in SNOWEX_POLES_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
