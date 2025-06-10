import numpy as np
from shapely.wkb import loads as load_wkb
from shapely.wkt import loads as load_wkt
from snowexsql.db import db_session_with_credentials
from snowexsql.tables import MeasurementType, Observer

from snowex_db.upload.layers import UploadProfileBatch, UploadProfileData
from tests.db_setup import DBSetup


class WithUploadedFile(DBSetup):
    UploaderClass = UploadProfileData
    kwargs = {}

    def upload_file(self, fname):
        with db_session_with_credentials() as (engine, session):
            u = self.UploaderClass(fname, **self.kwargs)

            # Allow for batches and single upload
            if 'batch' in self.UploaderClass.__name__.lower():
                u.push()
            else:
                u.submit(session)

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
    UploaderClass = UploadProfileBatch

    def upload_file(self, filenames):
        u = self.UploaderClass(filenames=filenames, **self.kwargs)

        u.push()
