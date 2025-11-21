"""
Read in the SnowEx 2020 Colorado State GPR data.
Upload SWE, Two Way Travel, and Depth.
"""

from earthaccess_data import get_files
from import_logger import get_logger

from snowexsql.db import db_session_with_credentials
from snowex_db.upload.points import PointDataCSV

LOG = get_logger()

GPR_CSU_MAP = {
    "SNEX20_GM_CSU_GPR": "10.5067/S5EGFLCIAB18",
}


def main(file_list: list, doi: str) -> None:
    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")

    kwargs = {
        # Constant Metadata for the GPR data
        "campaign_name": "Grand Mesa",
        "site": "Grand Mesa",
        "observer": "Randall Bonnell",
        "instrument": "gpr",
        "instrument_model": "pulse EKKO Pro multi-polarization 1 GHz GPR",
        "timezone": "UTC",
        "doi": doi,
        "name": "CSU GPR Data",
    }

    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            LOG.info(f"Uploading {file} file.")

            # Instantiate the point uploader
            uploader = PointDataCSV(session, file, **kwargs)

            # Push it to the database
            uploader.submit()


if __name__ == "__main__":
    for data_set_id, doi in GPR_CSU_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
