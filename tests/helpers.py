import numpy as np
import pytest
from shapely.wkb import loads as load_wkb
from shapely.wkt import loads as load_wkt
from snowexsql.tables import MeasurementType, Observer

from snowex_db.upload.layers import UploadProfileData


class WithUploadedFile:
    UploaderClass = UploadProfileData
    kwargs = {}

    def upload_file(self, session, filename):
        u = self.UploaderClass(
            session, filename, **self.kwargs
        )
        u.submit()

    def _check_location(self, table, expected_lon, expected_lat, attribute="geom"):
        """
        Check that the geometry stored in the table matches expected lon/lat

        Args:
            table: table name
            expected_lon: expected longitude
            expected_lat: expected latitude
            attribute: attribute name storing geometry (default "geom")

        Returns:

        """
        result = self.get_value(table, attribute)
        geom_from_wkb = load_wkb(bytes(result.data))
        assert geom_from_wkb.x == pytest.approx(expected_lon)
        assert geom_from_wkb.y == pytest.approx(expected_lat)

    def _check_metadata(self, table, attribute, expected_value):
        # Get multiple values for observers
        if table in [Observer, MeasurementType]:
            result = self.get_values(table, attribute)
        else:
            result = self.get_value(table, attribute)

        if isinstance(expected_value, float) and np.isnan(expected_value):
            assert np.isnan(result)
        else:
            assert result == expected_value, f"Assertion failed: Expected {expected_value}, but got {result}"

