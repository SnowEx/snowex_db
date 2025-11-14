"""
Read in the SnowEx 2020 BSU GPR data. Uploaded SWE, Two Way Travel, Depth, to
the database.
"""
from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials

from snowex_db.upload.points import PointDataCSV

LOG = get_logger()

GPR_BSU_MAP = {
    "SNEX20_BSU_GPR": "10.5067/Q2LFK0QSVGS2"
}

def main(file_list: list, doi: str) -> None:
    # Constant Metadata for the GPR data
    kwargs = {
        "campaign_name": "Grand Mesa",
        "site": "Grand Mesa",
        "observer": "Tate Meehan",
        "instrument": "gpr",
        "instrument_model": "1 GHz GPR",
        "timezone": "UTC",
        "doi": doi,
        "name": "BSU GPR Data",
    }

    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            # Skip the downsampled version
            if "downsampled" in file:
                continue

            LOG.info(f"Uploading {file} file.")
            uploader = PointDataCSV(session, file, **kwargs)
            # Push it to the database
            uploader.submit()


if __name__ == '__main__':
    for data_set_id, doi in GPR_BSU_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
