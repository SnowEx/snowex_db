"""
Read in the SnowEx 2020 UNM GPR.
Upload SWE, Two Way Travel, and Depth.
"""

from pathlib import Path

import pandas as pd

from earthaccess_data import get_files
from import_logger import get_logger
from snowexsql.db import db_session_with_credentials

from snowex_db.upload.points import PointDataCSV


LOG = get_logger()

GPR_UNM_MAP = {
    "SNEX20_UNM_GPR": "10.5067/WE9GI1GVMQF6",
}


def main(file_list: list, doi: str) -> None:
    LOG.info(f"Uploading DOI: {doi} with {len(file_list)} files.")

    kwargs = {
        # Constant Metadata for the GPR data
        "observer": "Ryan Webb",
        "doi": "https://doi.org/10.5067/WE9GI1GVMQF6",
        "campaign_name": "Grand Mesa",
        "instrument": "gpr",
        "timezone": "UTC",
        "name": "UNM GPR Data",
    }

    # Grab a db connection
    with db_session_with_credentials() as (_engine, session):
        for file in file_list:
            # Make two files, filter by frequency
            # Read in for management of instruments
            df_raw = pd.read_csv(file)
            file = Path(file)

            for freq in [800, 1600]:
                new_file_name = file.parent.joinpath(f"{file.stem}_{freq}{file.suffix}")
                # Filter the data by frequency
                df_filtered = df_raw[df_raw["FREQ_MHz"] == freq]
                # convert depth to cm
                df_filtered.loc[:, ["DEPTH"]] = df_filtered["DEPTH_m"].mul(100)
                # Drop the original depth column
                df_filtered = df_filtered.drop(columns=["DEPTH_m"])
                # Unused column
                df_filtered = df_filtered.drop(columns=["FREQ_MHz"])
                # Save the filtered data to a new file
                df_filtered.to_csv(new_file_name, index=False)
                specific_kwargs = dict(instrument_model=f"Mala {freq} MHz GPR")
                # Instantiate the point uploader
                uploader = PointDataCSV(
                    session, str(new_file_name), **{**kwargs, **specific_kwargs}
                )
                # Push it to the database
                uploader.submit()

                # Remove temporary file
                Path(new_file_name).unlink()


if __name__ == "__main__":
    for data_set_id, doi in GPR_UNM_MAP.items():
        with get_files(data_set_id, doi) as files:
            main(files, doi)
