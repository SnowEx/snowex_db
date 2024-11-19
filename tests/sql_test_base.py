from os.path import dirname, join

from numpy.testing import assert_almost_equal
from sqlalchemy import asc

from snowexsql.db import get_db, initialize
from tests.db_setup import DBSetup


def pytest_generate_tests(metafunc):
    """
    Function used to parametrize functions. If the function is in the
    params keys then run it. Otherwise run all the tests normally.
    """
    # Were params provided?
    if hasattr(metafunc.cls, 'params'):
        if metafunc.function.__name__ in metafunc.cls.params.keys():
            funcarglist = metafunc.cls.params[metafunc.function.__name__]
            argnames = sorted(funcarglist[0])
            metafunc.parametrize(
                argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
            )


class TableTestBase(DBSetup):
    """
    Test any table by picking
    """
    # Class to use to upload the data
    UploaderClass = None

    # Positional arguments to pass to the uploader class
    args = []

    # Keyword args to pass to the uploader class
    kwargs = {}

    # Always define this using a table class from data.py and is used for ORM
    TableClass = None



    def get_query(self, filter_attribute, filter_value, query=None):
        """
        Return the base query using an attribute and value that it is supposed
        to be

        Args:
            filter_attribute: Name of attribute to search for
            filter_value: Value that attribute should be to filter db search
            query: If were extended a query use it instead of forming a new one
        Return:
            q: Uncompiled SQLalchemy Query object
        """

        if query is None:
            query = self.session.query(self.TableClass)

        fa = getattr(self.TableClass, filter_attribute)
        q = query.filter(fa == filter_value).order_by(asc(fa))
        return q

    def check_count(self, count_attribute, data_name, expected_count):
        """
        Test the record count of a data type
        """
        q = self.get_query(count_attribute, data_name)
        records = q.all()
        assert len(records) == expected_count

    def check_value(self, count_attribute, data_name, attribute_to_check, filter_attribute, filter_value, expected):
        """
        Test that the first value in a filtered record search is as expected
        """
        # Filter  to the data type were querying
        q = self.get_query(count_attribute, data_name)

        # Add another filter by some attribute
        q = self.get_query(filter_attribute, filter_value, query=q)

        records = q.all()
        if records:
            received = getattr(records[0], attribute_to_check)
        else:
            received = None

        try:
            received = float(received)
        except:
            pass

        if type(received) == float:
            assert_almost_equal(received, expected, 6)
        else:
            assert received == expected

    def check_unique_count(self, count_attribute, data_name, attribute_to_count, expected_count):
        """
        Test that the number of unique values in a given attribute is as expected
        """
        # Add another filter by some attribute
        q = self.get_query(count_attribute, data_name)
        records = q.all()
        received = len(set([getattr(r, attribute_to_count) for r in records]))
        assert received == expected_count
