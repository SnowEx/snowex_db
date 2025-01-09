import datetime

from snowexsql.data import PointData
from snowex_db.upload import PointDataCSV

from .sql_test_base import TableTestBase, pytest_generate_tests


class PointsBase(TableTestBase):
    args = []
    kwargs = dict(in_timezone='US/Mountain',
                  depth_is_metadata=False,
                  site_name='Grand Mesa',
                  epsg=26912,
                  observers='TEST')
    TableClass = PointData
    UploaderClass = PointDataCSV


class TestDensityAlaska(TableTestBase):
    """
    # TODO: Test that row based tzinfo and crs works (for alaska data)
    """
    pass


class TestGPRPointData(PointsBase):
    gpr_dt = datetime.date(2019, 1, 28)

    args = ['gpr.csv']
    params = {
        'test_count': [
            # Test that we uploaded 10 records
            dict(data_name='two_way_travel', expected_count=10)
        ],

        'test_value': [
            # Test the actual value of the dataset
            dict(data_name='two_way_travel', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
                 expected=8.3),
            dict(data_name='density', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
                 expected=250.786035454008),
            dict(data_name='depth', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
                 expected=102.662509421414),
            dict(data_name='swe', attribute_to_check='value', filter_attribute='date', filter_value=gpr_dt,
                 expected=257.463237275561),
            # Test our unit assignment
            dict(data_name='two_way_travel', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
                 expected='ns'),
            dict(data_name='density', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
                 expected='kg/m^3'),
            dict(data_name='depth', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
                 expected='cm'),
            dict(data_name='swe', attribute_to_check='units', filter_attribute='date', filter_value=gpr_dt,
                 expected='mm'),
        ],

        'test_unique_count': [
            # Test we have 5 unique dates
            dict(data_name='swe', attribute_to_count='date', expected_count=3)
        ]
    }


class TestPoleDepthData(PointsBase):
    dt = datetime.date(2020, 2, 1)

    args = ['pole_depths.csv']
    kwargs = dict(in_timezone='US/Mountain',
                  depth_is_metadata=False,
                  site_name='Grand Mesa',
                  epsg=26912,
                  observers='TEST')
    params = {
        'test_count': [
            # Test that we uploaded 14 records
            dict(data_name='depth', expected_count=14)
        ],

        'test_value': [
            # Test the actual value of the dataset
            dict(data_name='depth', attribute_to_check='value', filter_attribute='date', filter_value=dt,
                 expected=101.2728),
        ],

        'test_unique_count': [
            # Test we have 3 unique dates
            dict(data_name='depth', attribute_to_count='time', expected_count=4)
        ]
    }

    def test_camera_description(self):
        """
        Tests that camera id is added to the description on upload
        """
        result = self.session.query(PointData.equipment).filter(PointData.date == datetime.date(2020, 1, 27)).all()
        assert 'camera id = W1B' == result[0][0]


class TestPerimeterDepthData(PointsBase):
    dt = datetime.date(2019, 12, 20)

    args = ['perimeters.csv']
    kwargs = dict(in_timezone='US/Mountain', out_timezone='US/Mountain',
                  depth_is_metadata=False, )
    params = {
        'test_count': [
            # Test that we uploaded 14 records
            dict(data_name='depth', expected_count=9)
        ],

        'test_value': [
            # Test the actual value of the dataset
            dict(data_name='depth', attribute_to_check='value', filter_attribute='date', filter_value=dt,
                 expected=121),
        ],

        'test_unique_count': [
            # Test we have 3 unique dates
            dict(data_name='depth', attribute_to_count='site_id', expected_count=1)
        ]
    }
