import os
from pathlib import Path

import pytest
from snowexsql.db import DB_CONNECTION_OPTIONS, db_connection_string, initialize
from sqlalchemy import create_engine, orm

SESSION = orm.scoped_session(orm.sessionmaker())
# Environment variable to load the correct credentials
os.environ['SNOWEXSQL_TESTS'] = 'True'


@pytest.fixture(scope="session")
def data_dir():
    return Path(__file__).parent.joinpath("data").absolute().resolve()

@pytest.fixture(scope='session')
def test_db_info():
    return db_connection_string()

@pytest.fixture(scope='session')
def sqlalchemy_engine(test_db_info):
    engine = create_engine(
        test_db_info,
        pool_pre_ping=True,
        connect_args={
            'connect_timeout': 10,
            **DB_CONNECTION_OPTIONS
        }
    )
    initialize(engine)

    yield engine

    engine.dispose()


@pytest.fixture(scope="session")
def connection(sqlalchemy_engine):
    with sqlalchemy_engine.connect() as connection:
        # Configure session
        SESSION.configure(
            bind=connection, join_transaction_mode="create_savepoint"
        )

        yield connection


@pytest.fixture(scope="class", autouse=True)
def session(connection):
    # Based on:
    # https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites  ## noqa

    transaction = connection.begin()

    # Create a new session
    session = SESSION()

    yield session

    # rollback
    # Everything that happened with the Session above
    # (including calls to commit()) are rolled back.
    session.close()
    transaction.rollback()
