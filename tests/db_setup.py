from snowexsql.tables.campaign_observation import CampaignObservation
import pytest
from snowexsql.db import get_db, initialize
from snowexsql.tables import (Campaign, DOI, Instrument, LayerData,
                              MeasurementType, Observer, Site,
                              PointObservation, PointData)
from snowexsql.tables.site import SiteObservers
from sqlalchemy import orm

# DB Configuration and Session
SESSION = orm.scoped_session(orm.sessionmaker())


class DBSetup:
    """
    Base class for all our tests. Ensures that we clean up after every class
    that's run
    """

    def setup(self):
        """
        Setup the database for testing
        """
        self.engine, self.session, self.metadata = get_db(return_metadata=True)

        initialize(self.engine)

    @pytest.fixture(scope="class")
    def db(self):
        self.setup()
        yield self.engine
        self.teardown()

    def teardown(self):
        """
        Clean up after class completed.

        NOTE: Not dropping the DB since this is done at every test class
              initialization
        """
        self.session.query(LayerData).delete()
        self.session.query(SiteObservers).delete()
        self.session.query(PointData).delete()
        self.session.query(CampaignObservation).delete()
        self.session.query(Observer).delete()
        self.session.query(Site).delete()
        self.session.query(Instrument).delete()
        self.session.query(Campaign).delete()
        self.session.query(DOI).delete()
        self.session.query(MeasurementType).delete()
        self.session.commit()

        self.session.close()
