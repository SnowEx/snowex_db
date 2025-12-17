"""Top-level package for snowex_db."""

__author__ = """Micah Johnson"""
__version__ = '0.1.0'

# from snowexsql.db import get_db
# from snowexsql.api import DB_NAME
# from contextlib import contextmanager


# @contextmanager
# def db_session(db_name, credentials):
#     # use default_name
#     db_name = db_name or DB_NAME
#     engine, session = get_db(db_name, credentials=credentials)
#     yield session, engine
#     session.close()
