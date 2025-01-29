import os
from pathlib import Path

import pytest

# Environment variable to load the correct credentials
os.environ['SNOWEXSQL_TESTS'] = 'True'


@pytest.fixture(scope="session")
def data_dir():
    return Path(__file__).parent.joinpath("data").absolute().resolve()
