import numpy as np
from shapely.wkb import loads as load_wkb
from shapely.wkt import loads as load_wkt
from snowexsql.db import db_session_with_credentials
from snowexsql.tables import MeasurementType, Observer

from snowex_db.upload.layers import UploadProfileBatch, UploadProfileData


class WithUploadedFile:
    UploaderClass = UploadProfileData
    kwargs = {}

    def upload_file(self, session, filename):
        u = self.UploaderClass(
            session, filename, **self.kwargs
        )
        u.submit()

    def get_records(self, session, table, attribute, value):
        """
        Fetches records that match criteria.

        Using the session object from the test class allows for lazy loading
        associated records.

        Arguments:
            session: The database session from the test class
            table: Table to query
            attribute: The name of the attribute in the table to use as a filter.
            value: The attribute value to filter by.

        Returns:
            A list of records
        """
        attribute = getattr(table, attribute)
        return session.query(table).filter(attribute == value).all()

    def get_value(self, table, attribute):
        with db_session_with_credentials() as (engine, session):
            obj = getattr(table, attribute)
            result = session.query(obj).all()
        return result[0][0]

    def get_values(self, table, attribute):
        with db_session_with_credentials() as (engine, session):
            obj = getattr(table, attribute)
            result = session.query(obj).all()
        return [r[0] for r in result]

    def _check_metadata(self, table, attribute, expected_value):
        # Get multiple values for observers
        if table in [Observer, MeasurementType]:
            result = self.get_values(table, attribute)
        else:
            result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt), f"Assertion failed: Expected {geom_from_wkt}, but got {geom_from_wkb}"
        elif isinstance(expected_value, float) and np.isnan(expected_value):
            assert np.isnan(result)
        else:
            assert result == expected_value, f"Assertion failed: Expected {expected_value}, but got {result}"


class WithUploadBatchFiles(WithUploadedFile):
    """
    Overwrite `upload_file` function to support passing of multiple files.
    """
    UploaderClass = UploadProfileBatch

    def upload_file(self, filenames, session):
        u = self.UploaderClass(
            filenames=filenames, session=session, debug=True, **self.kwargs
        )

        u.push()
