from snowexsql.tables import PointObservation, MeasurementType

from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class PointBaseTesting(TableTestBase, WithUploadedFile):
    def filter_measurement_type(self, session, measurement_type, query=None):
        if query is None:
            query = session.query(self.TableClass)

        query = query.join(
            self.TableClass.observation
        ).join(
            PointObservation.measurement_type
        ).filter(MeasurementType.name == measurement_type)
        return query
