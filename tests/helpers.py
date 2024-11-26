import numpy as np
from shapely.wkb import loads as load_wkb
from shapely.wkt import loads as load_wkt
from snowexsql.tables import Observer, MeasurementType

from snowex_db.upload.layers import UploadProfileData
from tests.db_setup import DBSetup, db_session_with_credentials


class WithUploadedFile(DBSetup):
    UploaderClass = UploadProfileData
    kwargs = {}

    def upload_file(self, fname):
        with db_session_with_credentials(
                self.database_name(), self.CREDENTIAL_FILE
        ) as (session, engine):
            u = self.UploaderClass(fname, **self.kwargs)

            # Allow for batches and single upload
            if 'batch' in self.UploaderClass.__name__.lower():
                u.push()
            else:
                u.submit(session)

    def get_value(self, table, attribute):
        with db_session_with_credentials(
                self.database_name(), self.CREDENTIAL_FILE
        ) as (session, engine):
            obj = getattr(table, attribute)
            result = session.query(obj).all()
        return result[0][0]

    def get_values(self, table, attribute):
        with db_session_with_credentials(
                self.database_name(), self.CREDENTIAL_FILE
        ) as (session, engine):
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

            assert geom_from_wkb.equals(geom_from_wkt)
        elif isinstance(expected_value, float) and np.isnan(expected_value):
            assert np.isnan(result)
        else:
            assert result == expected_value
