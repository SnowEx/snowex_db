import contextlib
from typing import Generator, List

import earthaccess
from pathlib import Path

DOWNLOAD_FOLDER = Path(__file__).parent.parent / "download" / "data"


@contextlib.contextmanager
def get_files(data_set_id:str, doi:str) ->Generator[List[Path], None, None]:
    """
    Load locally downloaded files or get them from NSIDC via earthaccess

    Args:
        data_set_id: DATA SET ID from NSIDC website
        doi: DOI from NSIDC website
    """
    source_files = DOWNLOAD_FOLDER.joinpath(data_set_id)
    files = []

    if source_files.exists() and source_files.is_dir():
        files = list(source_files.glob("*.csv"))

    if len(files) == 0:
        earthaccess.login()
        files = earthaccess.download(
            earthaccess.search_data(doi=doi),
            local_path=source_files,
        )

    yield files
