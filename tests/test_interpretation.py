from datetime import date, time

import numpy as np
import pandas as pd
import pytest
import pytz

from snowex_db.interpretation import *


@pytest.mark.filterwarnings('ignore:Assuming')
@pytest.mark.parametrize("card,expected", [('n', 0), ('S', 180), ('S/SW', 202.5), ('West', 270)])
def test_cardinal_to_degrees(card, expected):
    """
    Test if we can convert cardinal directions correctly
    """
    d = convert_cardinal_to_degree(card)
    assert d == expected


@pytest.mark.filterwarnings('ignore:Assuming')
def test_cardinal_to_degrees_value_error():
    """
    Test test_cardinal_to_degrees raises an error when it doesn't know the value
    """

    # Test composite directions
    with pytest.raises(ValueError):
        d = convert_cardinal_to_degree('')


mst = pytz.timezone('US/Mountain')
this_day = date(year=2020, month=1, day=1)
this_time = time(hour=7, tzinfo=pytz.utc)


@pytest.mark.parametrize("data, in_tz, expected_date, expected_time", [
    ({'date/time': '2020-01-01-00:00'}, 'US/Mountain', this_day, this_time),
    ({'date/local_time': '2020-01-01-00:00'}, 'US/Mountain', this_day, this_time),
    ({'date': '2020-01-01', 'time': '00:00'}, 'US/Mountain', this_day, this_time),
    # Test converting of the UTC GPR format which assumes the input tz is UTC
    ({'utcyear': 2020, 'utcdoy': 1, 'utctod': '070000.00'}, 'UTC', this_day, this_time),
    # Test handling the milli seconds
    ({'utcyear': 2019, 'utcdoy': 35, 'utctod': 214317.222}, 'UTC', date(year=2019, month=2, day=4),
     time(hour=21, minute=43, second=17, microsecond=222000, tzinfo=pytz.UTC)),
    # Test parsing of the new GPR data time on NSIDC
    ({'date': '012820', 'time': '161549.557'}, 'UTC', date(year=2020, month=1, day=28),
     time(hour=16, minute=15, second=49, microsecond=557000, tzinfo=pytz.UTC)),
    ({'Date&Time': '1/27/2020 11:00'}, 'US/Mountain', date(year=2020, month=1, day=27),
     time(hour=18, tzinfo=pytz.utc)),
    ({'date': '1/27/2020'}, 'US/Pacific', date(year=2020, month=1, day=27),
     time(hour=8, tzinfo=pytz.utc)),
    ({'date/local standard time': '2019-12-20T13:00'}, 'US/Pacific', date(2019, 12, 20),
     time(hour=21, minute=0, tzinfo=pytz.utc)),
    # Test CSU GPR time
    ({'date': '020620', 'time': 'nan'}, 'US/Mountain', date(2020, 2, 6), None),
    # Test Mala GPR time
    ({'date': '28-Jan-20', 'time': '16:07'}, 'UTC', date(2020, 1, 28), time(16,7,tzinfo=pytz.utc)),

])
def test_add_date_time_keys(data, in_tz, expected_date, expected_time):
    """
    Test that the date and time keys can be added from various scenarios
    """
    d = add_date_time_keys(data, in_timezone=in_tz, out_timezone='UTC')
    assert d['date'] == expected_date
    assert d['time'] == expected_time

    # Ensure we always remove the original keys used to interpret
    # for k in ['utcdoy', 'utctod', 'datetime']:
    #     assert k not in d.keys()


@pytest.mark.parametrize("depths, expected, desired_format, is_smp",
                         [
                             # Test Snow Height --> surface datum
                             ([10, 5, 0], [0, -5, -10], 'surface_datum', False),
                             # Test SMP --> surface_datum
                             ([0, 5, 10], [0, -5, -10], 'surface_datum', True),
                             # Test surface_datum --> surface_datum
                             ([0, -5, -10], [0, -5, -10], 'surface_datum', False),
                             # Test Snow Height --> snow_height
                             ([10, 5, 0], [10, 5, 0], 'snow_height', False),
                             # Test surface_datum--> snow_height
                             ([0, -5, -10], [10, 5, 0], 'snow_height', False),
                             # Test SMP --> snow_height
                             ([0, 5, 10], [10, 5, 0], 'snow_height', True),

                         ])
def test_standardize_depth(depths, expected, desired_format, is_smp):
    """
    Test setting the depth format datum assignment (e.g. depth from surface or ground)
    """
    dd = pd.Series(depths)
    new = standardize_depth(dd, desired_format=desired_format, is_smp=is_smp)

    for i, d in enumerate(expected):
        assert new.iloc[i] == d


@pytest.mark.parametrize("layer,name, expected",
                         [
                             # Test normal averaging
                             ({'density_a': 180, 'density_b': 200, 'density_c': 'nan'}, 'density', 190.0),
                             # Test all nans scenario
                             ({'dielectric_constant_a': 'nan', 'dielectric_constant_b': 'nan'}, 'dielectric', np.nan)
                         ])
def test_avg_from_multi_sample(layer, name, expected):
    """
    Test whether we can extract the avg sample
    """
    received = avg_from_multi_sample(layer, name)

    assert str(received) == str(expected)


@pytest.mark.parametrize('data_name, expected', [
    ('amplitude of pass 1', 'Overpass Duration: 2020-01-01 10:00:00 - 2020-01-01 12:00:00 (UTC)'),
    ('correlation',
     '1st Overpass Duration: 2020-01-01 10:00:00 - 2020-01-01 12:00:00 (UTC), 2nd Overpass Duration 2020-02-01 '
     '10:00:00 - 2020-02-01 12:00:00 (UTC)'),

])
def test_get_InSar_flight_comment(data_name, expected):
    """
    Test we can formulate a usefule comment for the uavsar annotation file
    and a dataname
    """
    blank = '{} time of acquisition for pass {}'

    desc = {blank.format('start', '1'): {'value': pd.to_datetime('2020-01-01 10:00:00 UTC')},
            blank.format('stop', '1'): {'value': pd.to_datetime('2020-01-01 12:00:00 UTC')},
            blank.format('start', '2'): {'value': pd.to_datetime('2020-02-01 10:00:00 UTC')},
            blank.format('stop', '2'): {'value': pd.to_datetime('2020-02-01 12:00:00 UTC')}}

    comment = get_InSar_flight_comment(data_name, desc)
    assert comment == expected


@pytest.mark.parametrize("info, key, expected_zone", [
    # Test a string zone is used
    ({'utm_zone': '12N'}, 'utm_zone', 12),
    # Test utm_zone not provided
    ({}, 'utm_zone', None),
    # Test single/double digit zones
    ({'utm_zone': '6'}, 'epsg', 26906),
    ({'utm_zone': '11'}, 'epsg', 26911),

])
def test_manage_utm_zone(info, key, expected_zone):
    result = manage_utm_zone(info)
    assert result[key] == expected_zone

