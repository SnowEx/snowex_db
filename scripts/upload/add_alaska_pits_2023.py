"""
Alaska 2023 campaign
"""

from pathlib import Path


from earthaccess_data import get_files
from import_logger import get_logger

from snowexsql.db import db_session_with_credentials
from snowex_db.upload.layers import UploadProfileData


LOG = get_logger()


# Map of DATA SET ID to DOI from NSIDC
# * https://nsidc.org/data/snex23_mar23_sp/versions/1
IOP_PIT_DOI = {
    "SNEX23_MAR23_SP": "10.5067/SJZ90KNPKCYR",
}

INSTRUMENT_FILE_MAP = {
    "density": "Density Cutter",
    "temperature": "Thermometer",
    "lwc": "A2 Sensor",
    "stratigraphy": "Manual",
}


def main(file_list: list, doi: str) -> None:
    # Files to ignore
    ignore_files = [
        "Summary_Environment",
        "Summary_SWE",
        "Summary_SWE",
    ]

    # Data to skip for now
    ignore_data = [
        "gapFilled_density",
    ]

    # Filter to CSV and relevant data
    file_list = [
        file
        for file in file_list
        if str(file).lower().endswith(".csv")
        and not any(name in file for name in ignore_files)
        and not any(name in file for name in ignore_data)
    ]

    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")

    with db_session_with_credentials() as (_engine, session):
        for pit_data in file_list:
            LOG.info(f"Uploading {pit_data} file.")
            file_name_parts = Path(pit_data).name.split("_")

            instrument = INSTRUMENT_FILE_MAP.get(file_name_parts[-2].lower(), "")

            pid_upload = UploadProfileData(
                session,
                filename=pit_data,
                doi=doi,
                instrument=instrument,
            )
            pid_upload.submit()


if __name__ == "__main__":
    for data_set_id, doi in IOP_PIT_DOI.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
