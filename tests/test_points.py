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
