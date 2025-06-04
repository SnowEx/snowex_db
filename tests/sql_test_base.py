import numpy as np
import pytest
from numpy.testing import assert_almost_equal
from snowexsql.db import db_session_with_credentials
from snowexsql.tables import MeasurementType
from sqlalchemy import asc

from tests.db_setup import DBSetup


def safe_float(r):
    try:
        return float(r)
    except Exception:
        return r


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

    def filter_measurement_type(self, session, measurement_type, query=None):
        if query is None:
            query = session.query(self.TableClass)

        query = query.join(
            self.TableClass.measurement_type
        ).filter(MeasurementType.name == measurement_type)
        return query

    def get_query(self, session, filter_attribute, filter_value, query=None):
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
            query = session.query(self.TableClass)

        fa = getattr(self.TableClass, filter_attribute)
        q = query.filter(fa == filter_value).order_by(asc(fa))
        return q

    def check_count(self, data_name):
        """
        Test the record count of a data type
        """
        with db_session_with_credentials() as (engine, session):
            q = self.filter_measurement_type(session, data_name)
            records = q.all()
        return len(records)

    def check_value(
            self, measurement_type, attribute_to_check, filter_attribute,
            filter_value, expected
    ):
        """
        Test that the first value in a filtered record search is as expected
        """
        # Filter  to the data type were querying
        with db_session_with_credentials() as (engine, session):
            q = self.filter_measurement_type(session, measurement_type)

            # Add another filter by some attribute
            q = self.get_query(session, filter_attribute, filter_value, query=q)

            records = q.all()
        if records:
            received = [getattr(r, attribute_to_check) for r in records]
            received = [safe_float(r) for r in received]
        else:
            received = None

        if type(received) == float:
            assert_almost_equal(received, expected, 6), f"Assertion failed: Expected {expected}, but got {received}"
        elif isinstance(received, list) and np.issubdtype(np.array(received).dtype, np.number):
            # Compare arrays, treating NaNs as equal
            assert np.array_equal(np.array(received), np.array(expected), equal_nan=True)
        else:
            assert pytest.approx(received) == expected, f"Assertion failed: Expected {expected}, but got {received}"

    def check_unique_count(self, data_name, attribute_to_count, expected_count):
        """
        Test that the number of unique values in a given attribute is as expected
        """
        # Add another filter by some attribute
        with db_session_with_credentials() as (engine, session):
            q = self.filter_measurement_type(session, data_name)
            records = q.all()
        received = len(set([getattr(r, attribute_to_count) for r in records]))
        assert received == expected_count
