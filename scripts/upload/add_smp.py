"""
Added SMP measurements from:
* 2020 Grand Mesa
"""
import fileinput

from earthaccess_data import get_files
from import_logger import get_logger

from snowexsql.db import db_session_with_credentials
from snowex_db.upload.layers import UploadProfileData

LOG = get_logger()

# Map of DATA SET ID to DOI from NSIDC
# * https://nsidc.org/data/snex20_smp/versions/1
SMP_DOI = {
    "SNEX20_SMP": "10.5067/ZYW6IHFRYDSE",
}

def add_header_indicator(file: str) -> None:
    """
    This is a workaround for all SMP files that don't use a '#' for the last header
    row with the column names. This information is always on line number 7 when converted
    from the original binary file to a CSV.

    Args:
        file: File to edit
    """
    with fileinput.FileInput(file, inplace=True) as f:
        for line_number, line in enumerate(f):
            if line_number == 6 and line.startswith('Depth'):
                print(f"# {line}", end='')
            else:
                print(line, end='')

def main(file_list: list, doi: str) -> None:
    LOG.info("Starting SMP Upload")

    # SMP has binary .PNT and .CSV files
    file_list = [file for file in file_list if str(file).endswith(".CSV")]

    # Keyword arguments.
    smp_metadata = {
        'campaign_name': 'Grand Mesa',
        'timezone': 'UTC',
        'instrument': 'SnowMicroPen',
        'header_sep': ':',
        'doi': doi,
    }

    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            add_header_indicator(file)
            # SMP data do not have the site ID in the metadata and only in the filename
            metadata = file.split("_")
            pit_id = metadata[-2]
            measurement = metadata[-3]

            LOG.info(f"  Adding site {pit_id} and measurement {measurement[-5:]}")

            # Now make a unique site ID to create an entry per location measurement
            pit_id = f"{metadata[-2]}-{measurement[-5:]}"

            uploader = UploadProfileData(
                session, filename=file, id=pit_id, comments=measurement, **smp_metadata
            )

            uploader.submit()

if __name__ == '__main__':
    for data_set_id, doi in SMP_DOI.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
