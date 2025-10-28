#!/usr/bin/env python

"""
Added SSA measurements from:
* 2020
"""

from import_logger import get_logger
from earthaccess_data import get_files
from snowexsql.db import db_session_with_credentials
from snowex_db.upload.layers import UploadProfileData

<<<<<<< HEAD
LOG = get_logger()

# Map of DATA SET ID to DOI from NSIDC
# * https://nsidc.org/data/snex20_ssa/versions/1
SSA_DOI = {
    "SNEX20_SSA": "10.5067/SNMM6NGGKWIT",
    # "SNEX23_SSA": "10.5067/BSEP59ADC6XN",
    # "SNEX23_SSA_SO": "10.5067/9SY1H2L0BY0X",
    # "SNEX23_OCT22_SSA" : "10.5067/CPQ2DA73IZVH",
}
=======

>>>>>>> 1892ebd (removed Batch Uploader classes)


def main(file_list, doi):
    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")

    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            LOG.info(f"Uploading: {file}")
            uploader = UploadProfileData(
                session, filename=file, doi=doi, timezone='MST'
            )
            uploader.submit()

if __name__ == '__main__':
    for data_set_id, doi in SSA_DOI.items():
        with get_files(data_set_id, doi) as files:
            LOG.info("Starting SSA upload")
            main(files, doi)
