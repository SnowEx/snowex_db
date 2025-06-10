import numpy as np
import pytest

from snowex_db.string_management import (
    standardize_key, get_encapsulated, strip_encapsulated, parse_none,
    kw_in_here, get_alpha_ratio, line_is_header, remap_data_names
)


@pytest.mark.parametrize(
    "in_str, expected", [
        ('SMP instrument #', 'smp_instrument_#'),
        ('Dielectric Constant A', 'dielectric_constant_a'),
        ('Specific surface area (m^2/kg)', 'specific_surface_area'),
        # Ensure we remove a csv byte order mark in latin encoding
        ("ï»¿Camera", "camera"),
        (' Temperature \n', 'temperature'),
    ]
)
def test_standardize_key(in_str, expected):
    """
    Test whether we can clean out the column header from a csv and standardize them
    """
    assert standardize_key(in_str) == expected


@pytest.mark.parametrize(
    'args, kwargs', [
        # Test multiple values being returned
        (
                [
                    'Density [kg/m^3], Date [yyyymmdd]', '[]',
                    ['kg/m^3', 'yyyymmdd'],
                ],
                {'errors': False},
        ),
        # Test single value being return with parenthesis
        (['Time (seconds)', '()', ['seconds']], {'errors': False}),
        # Test Single encapsulator used as both
        (['Name "Surveyor"', '"', ['Surveyor']], {'errors': False}),
        # Test nothing returned
        (['Name', '()', []], {'errors': False}),
        # Test our value error for incorrect encapsulation
        (['Name', '()()', []], {'errors': True}),
    ]
)
def test_get_encapsulated(args, kwargs):
    """
    Test where we can remove chars in a string
    """
    s, encaps, expected = args
    # Errors out to test exception
    errors = kwargs['errors']

    if not errors:
        results = get_encapsulated(s, encaps)
        for r, e in zip(results, expected):
            assert r == e
    else:
        with pytest.raises(ValueError):
            results = get_encapsulated(s, encaps)


@pytest.mark.parametrize(
    's, encaps, expected', [
        ('Density [kg/m^3], Date [yyyymmdd]', '[]', 'Density , Date '),
        ('Time (seconds)', '()', 'Time '),
        ('Name "Surveyor"', '"', 'Name '),
        # test for mm and comments exchange
        ('grain_size (mm), comments', '()', 'grain_size , comments'),
    ]
)
def test_strip_encapsulated(s, encaps, expected):
    """
    Test where we can remove chars in a string
    """
    r = strip_encapsulated(s, encaps)
    assert r == expected


@pytest.mark.parametrize(
    'str_value, expected', [
        # Expected nones
        ('NaN', None),
        ('NONE', None),
        (-9999, None),  # integer case
        ('-9999', None),
        ('-9999.0', None),
        (-9999.0, None),
        (np.nan, None),
        # Shouldn't modify anything
        (10.5, 10.5),
        ("Comment", "Comment"),
    ]
)
def test_parse_none(str_value, expected):
    """
    Test we can convert nones and nans to None and still pass through everything
    else.
    """
    assert parse_none(str_value) == expected


@pytest.mark.parametrize(
    'args, kwargs, expected', [
        # Test we find kw in the list
        (['test', ['turtle', 'test']], {'case_sensitive': False}, True),
        # Test we find kw in an entry in the list
        (['test', ['turtle', 'testing']], {'case_sensitive': False}, True),
        # Test the kw is found in a dictionary key list case-sensitive
        (['test', {'shell': 1, 'Test': 1}], {'case_sensitive': False}, True),
        # Test the kw is not found in the list
        (['test', ['turtle', 'shell']], {'case_sensitive': False}, False),
        # Test the kw is not found in the list case-sensitive
        (['test', ['shell', 'Test']], {'case_sensitive': True}, False),
        # Test the kw is found in the list case-sensitive
        (['Test', ['shell', 'Test']], {'case_sensitive': True}, True),
    ]
)
def test_kw_in_here(args, kwargs, expected):
    """
    Tests we can find key words in list keys, case in/sensitive test
    """
    assert kw_in_here(*args, **kwargs) == expected


@pytest.mark.parametrize(
    "str_line, encapsulator, expected", [
        # Test simple 50/50
        ("1A", None, 1),
        # Use the ignore encapsulator
        ('1"A"', '""', 0),
        # Check for the div by zero
        ('A', None, 1),

    ]
)
def test_get_alpha_ratio(str_line, encapsulator, expected):
    result = get_alpha_ratio(str_line, encapsulator=encapsulator)
    assert result == expected


@pytest.mark.parametrize(
    "line, header_sep, header_indicator, previous_alpha_ratio, expected_columns, expected",
    [
        # Test using a header indicator is the end all.
        ("# flags, ", None, '#', None, None, True),
        ("flags, ", None, '#', None, None, False),

        # Simple looking for 2 columns
        ("# flags, ", ',', None, None, 2, True),
        # Test with a real entry from stratigraphy which has lots of chars
        ("35.0,33.0,< 1 mm,DF,F,D,NaN", ',', '#', 7, 0.5, False),
        # Test a complicated string with encapsulation right after a normal header
        ('107.0,85.0,< 1 mm,FC,4F,D,"Variable."', ',', '#', 7, 1, False),

    ]
)
def test_line_is_header(
    line, header_sep, header_indicator, previous_alpha_ratio, expected_columns,
    expected
):
    result = line_is_header(
        line, header_sep, header_indicator, previous_alpha_ratio,
        expected_columns
    )
    assert result == expected


@pytest.mark.parametrize(
    "original, rename_map, expected", [
        # simple rename
        (['avg_density'], {'avg_density': 'density'}, ['density']),
        # No matches, so retain original info
        (['avg_density'], {'dummy': 'dummy_1'}, ['avg_density']),
        # Test dictionary replacement
        ({'observers': 'MJ'}, {'observers': 'observers'}, {'observers': 'MJ'}),
        # Test plain string replacement
        ('twt', {'twt': 'two_way_travel'}, 'two_way_travel'),

    ]
)
def test_remap_data_names(original, rename_map, expected):
    result = remap_data_names(original, rename_map)
    assert result == expected
