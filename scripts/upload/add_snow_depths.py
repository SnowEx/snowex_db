"""
Script to uploads the Snowex 2020 depths to the database\
"""

from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials
from snowex_db.upload.points import PointDataCSV
LOG = get_logger()

SNOWEX_DEPTHS_MAP = {
    "SNEX20_SD": "10.5067/9IA978JIACAR"
}


def main(file_list: list, doi: str) -> None:
    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")

    kwargs = {
        "site_name": "Grand Mesa",
        "campaign_name": "Grand Mesa",
        "timezone": "US/Mountain",
        "doi": doi,
    }

    file_list = [
        file
        for file in file_list
        if str(file).lower().endswith(".csv")
    ]

    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            uploader = PointDataCSV(session,
                                    file, 
                                    **kwargs)
            uploader.submit()


if __name__ == '__main__':
    for data_set_id, doi in SNOWEX_DEPTHS_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
