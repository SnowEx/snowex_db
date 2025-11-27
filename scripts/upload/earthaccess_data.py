import contextlib
from typing import Generator, List

import earthaccess
import os
from pathlib import Path

# option to set custom download folder via env variable
DOWNLOAD_FOLDER = Path(
    os.environ.get(
        "SNOWEX_DOWNLOAD_FOLDER",
        Path(__file__).parent.parent / "download" / "data"
    )
)

@contextlib.contextmanager
def get_files(
        data_set_id:str, doi:str,
        key_words:List[str]=None
) ->Generator[List[Path], None, None]:
    """
    Load locally downloaded files or get them from NSIDC via earthaccess

    Args:
        data_set_id: DATA SET ID from NSIDC website
        doi: DOI from NSIDC website
    """
    key_words = key_words or []
    source_files = DOWNLOAD_FOLDER.joinpath(data_set_id)
    files = []

    if source_files.exists() and source_files.is_dir():
        files = [
            file.as_posix() for file in source_files.glob("*.csv")
        ]

    if len(files) == 0:
        found_data = earthaccess.search_data(doi=doi)
        if key_words:
            # Filter by key words in file name
            found_data = [
                item for item in found_data
                if any(
                    kw in item["umm"]['GranuleUR'] for kw in key_words
                )
            ]

        if found_data:
            earthaccess.login()
            files = earthaccess.download(
                found_data,
                local_path=source_files,
            )
        else:
            print("No data found for the given parameters.")

    yield files
