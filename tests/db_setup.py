import json
from contextlib import contextmanager
from os.path import dirname, join

from sqlalchemy import orm

from snowexsql.db import get_db, initialize
from snowexsql.tables import (Campaign, DOI, Instrument, LayerData,
                              MeasurementType, Observer, Site)
from snowexsql.tables.site import SiteObservers
import pytest

# DB Configuration and Session
CREDENTIAL_FILE = join(dirname(__file__), 'credentials.json')
DB_INFO = json.load(open(CREDENTIAL_FILE))
SESSION = orm.scoped_session(orm.sessionmaker())


@contextmanager
def db_session_with_credentials(db_name, credentials_file):
    # use default_name
    db_name = db_name
    engine, session = get_db(db_name, credentials=credentials_file)
    yield session, engine
    session.close()


class DBSetup:
    """
    Base class for all our tests. Ensures that we clean up after every class
    that's run
    """
    CREDENTIAL_FILE = CREDENTIAL_FILE
    DB_INFO = DB_INFO

    @classmethod
    def database_name(cls):
        return cls.DB_INFO["address"] + "/" + cls.DB_INFO["db_name"]

    def setup(self):
        """
        Setup the database for testing
        """
        self.engine, self.session, self.metadata = get_db(
            self.database_name(),
            credentials=self.CREDENTIAL_FILE,
            return_metadata=True
        )

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
        self.session.query(Observer).delete()
        self.session.query(Site).delete()
        self.session.query(Instrument).delete()
        self.session.query(Campaign).delete()
        self.session.query(DOI).delete()
        self.session.query(MeasurementType).delete()
        self.session.commit()

        self.session.close()
