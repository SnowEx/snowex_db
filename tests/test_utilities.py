from datetime import date
from os.path import dirname, join

import pytest

from snowex_db.utilities import (
    read_n_lines, find_files, find_kw_in_lines, assign_default_kwargs,
    get_file_creation_date, get_site_id_from_filename, get_timezone_from_site_id
)


def test_read_nline():
    """
    Test we can read a specific number of lines from a file
    """
    f = join(dirname(__file__), 'data', 'density.csv')
    line = read_n_lines(f, 1)
    assert line[0] == '# Location,Grand Mesa\n'


def test_find_files():
    """
    Test we can find files using patterns and extensions
    """
    d = join(dirname(__file__), 'data')

    files = find_files(d, 'adf', 'w001001x')
    assert len(files) == 2


@pytest.mark.parametrize(
    "kw, lines, expected", [
        # Typical use
        ('snow', ['snowpits', 'nothing'], 0),
        # Didn't find anything
        ('ice', ['snow', 'ex', 'is', 'awesome'], -1),
    ]
)
def test_find_kw_in_lines(kw, lines, expected):
    """
    test finding a keyword in a list of strings
    """
    assert find_kw_in_lines(kw, lines, addon_str='') == expected


class TestAssignDefaultKwargs:
    """
    Test the function assign_default_kwargs. This class is necessary so
    we can add attributes to it without raising exceptions.
    """

    @pytest.mark.parametrize(
        "kwargs, defaults, leave, expected_kwargs, expected_attr", [
            # Assert missing attributes are added to object and removed from kwargs
            ({}, {'test': False}, [], {}, {'test': False}),
            # Assert we don't overwrite the kwargs provided by user
            ({'test': True, }, {'test': False}, [], {}, {'test': True}),
            # Assert we leave non-default keys and still assign defaults
            (
                    {'stays': True, },
                    {'test': False},
                    [],
                    {'stays': True},
                    {'test': False},
            ),
            # Assert keys can be left in the mod kwargs but still be used
            (
                    {'leave_test': True},
                    {'test': False, 'leave_test': True},
                    ['leave_test'],
                    {'leave_test': True},
                    {'test': False, 'leave_test': True},
            ),
        ]
    )
    def test_assign_default_kwargs(
        self, kwargs, defaults, leave, expected_kwargs, expected_attr
    ):
        """
        Test we can assign object attributes to an object given kwargs and defaults

        """
        # Make a dummy object for testing

        # Modify obj, and removed default kw in kwargs
        mod_kwargs = assign_default_kwargs(self, kwargs, defaults, leave)

        # 1. Test We have removed kw from mod_kwargs
        for k, v in expected_kwargs.items():
            assert v == mod_kwargs[k]

        # 2. Test the object has the attributes/values
        for k, v in expected_attr.items():
            assert getattr(self, k) == v


def test_get_file_creation_date():
    """
    Test the get_file_creation_date produces a datetime object
    """
    result = get_file_creation_date(__file__)
    assert type(result) is date


def test_get_site_id_from_filename():
    """
    Test getting site ID from filename
    """
    filename = "SNEX20_TS_SP_20191029_1210_COFEJ1_data_gapFilledDensity_v02.csv"
    regex = r'SNEX20_TS_SP_\d{8}_\d{4}_([a-zA-Z0-9]*)_data_.*_v02\.csv'
    site_id = get_site_id_from_filename(filename, regex)
    assert site_id == "COFEJ1"


@pytest.mark.parametrize('site_id, expected_tz', [
                         ('COGM', 'US/Mountain'),
                         ('CAAM', 'US/Pacific'),
                         ])
def test_get_timezone_from_site_id(site_id, expected_tz):
    """
    Test getting timezone from site ID
    """
    tz = get_timezone_from_site_id(site_id)
    assert tz == expected_tz