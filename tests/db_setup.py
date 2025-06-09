import pytest
from sqlalchemy import orm

from snowexsql.db import get_db, initialize

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
        self.engine, self.session = get_db()
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
        self.session.close()
        self.engine.dispose()
