import numpy as np
import pytest
from numpy.testing import assert_almost_equal
from sqlalchemy import asc

from snowexsql.tables import MeasurementType


def safe_float(r):
    try:
        return float(r)
    except Exception:
        return r


# TODO: Review methods for similarity and combine those
class TableTestBase:
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

    @pytest.fixture(autouse=True)
    def setup(self, session):
        # Store the session as an attribute to use in helper functions
        self._session = session # noqa

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
            session: DB Session object
            filter_attribute: Name of attribute to search for
            filter_value: Value that attribute should be to filter db search
            query: If were extended a query use it instead of forming a new one
        Return:
            q: Uncompiled SQLAlchemy Query object
        """

        if query is None:
            query = session.query(self.TableClass)

        filter_attribute = getattr(self.TableClass, filter_attribute)
        query = query.filter(
            filter_attribute == filter_value
        ).order_by(
            asc(filter_attribute)
        )
        return query

    def check_count(self, data_name):
        """
        Test the record count of a data type
        """
        q = self.filter_measurement_type(self._session, data_name)
        records = q.all()
        return len(records)

    def check_value(
            self, measurement_type, attribute_to_check, filter_attribute,
            filter_value, expected
    ):
        """
        Test that the first value in a filtered record search is as expected
        """
        # Filter to the queried data type
        q = self.filter_measurement_type(self._session, measurement_type)

        # Add another filter by some attribute
        q = self.get_query(self._session, filter_attribute, filter_value, query=q)

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
            assert pytest.approx(received) == expected, \
                f"Assertion failed: Expected {expected}, but got {received}"

        return records

    def check_unique_count(self, data_name, attribute_to_count, expected_count):
        """
        Test that the number of unique values in a given attribute is as expected
        """
        # Add another filter by some attribute
        q = self.filter_measurement_type(self._session, data_name)
        records = q.all()
        received = len(set([getattr(r, attribute_to_count) for r in records]))
        assert received == expected_count

    def get_value(self, table, attribute):
        obj = getattr(table, attribute)
        result = self._session.query(obj).all()
        return result[0][0]

    def get_values(self, table, attribute):
        obj = getattr(table, attribute)
        result = self._session.query(obj).all()
        return [r[0] for r in result]

    def get_records(self, table, attribute, value):
        """
        Fetches records that match criteria.

        Using the session object from the test class allows for lazy loading
        associated records.

        Arguments:
            table: Table to query
            attribute: The name of the attribute in the table to use as a filter.
            value: The attribute value to filter by.

        Returns:
            A list of records
        """
        attribute = getattr(table, attribute)
        return self._session.query(table).filter(attribute == value).all()

