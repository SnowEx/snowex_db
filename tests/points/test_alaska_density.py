from datetime import datetime, timezone, date

import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation
from snowexsql.tables.campaign_observation import CampaignObservation

from snowex_db.upload.points import PointDataCSV
from tests.points._base import PointBaseTesting



class TestDensityAlaska(PointBaseTesting):
    """
    # TODO: Test that row based tzinfo and crs works (for alaska data)
    """
    pass
