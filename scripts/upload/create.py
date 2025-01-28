"""
Script used to create the database and tables for the first time
"""
from snowexsql.db import get_db, initialize
# from snowex_db.utilities import get_LOGger
from sqlalchemy import text as sqltext
import argparse
import logging

LOG = logging.getLogger(__name__)


def main(overwrite=False, db='snowex', credentials='./credentials.json'):
    """
    Main function to manage creating our tables in the databases

    Args:
        overwrite: Bool indicating whether to ask the user before overwriting the db
        db: Name of a local database to write tables to
    """

    engine, session = get_db(f"localhost/{db}", credentials=credentials)

    if overwrite:
        initialize(engine)
        LOG.warning('Database cleared!\n')
        try:
            with engine.connect() as connection:
                # Autocommit so the user is created before granting access
                connection = connection.execution_options(
                    isolation_level="AUTOCOMMIT")
                connection.execute(
                    sqltext("CREATE USER snow WITH PASSWORD 'hackweek';")
                )
                connection.execute(
                    sqltext("GRANT USAGE ON SCHEMA public TO snow;")
                )
        except Exception as e:
            LOG.error("Failed on user creation")
            raise e

        for t in ['sites', 'points', 'layers', 'images']:

            sql = f'GRANT SELECT ON {t} TO snow;'
            LOG.info(f'Adding read only permissions for table {t}...')
            with engine.connect() as connection:
                connection.execute(sqltext(sql))
    else:
        LOG.warning('Aborted. Database has not been modified.\n')

    session.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Script to create our databases using the python library')
    parser.add_argument('--db', dest='db', default='snowex',
                        help='Name of the database locally to add tables to')
    parser.add_argument('--overwrite', dest='overwrite', action='store_true',
                        help='Whether or not to bypass the overwriting prompt and auto overwrite everything. Should '
                             'be used for the install only .')
    parser.add_argument('--credentials', dest='credentials', default='./credentials.json',
                        help='Past to a json containing')
    args = parser.parse_args()

    # Allow users to bypass the overwriting prompt for install only!
    if args.overwrite:
        overwrite = True
    else:
        a = input(
            '\nWARNING! You are about to overwrite the entire database! Press Y to continue, press any other key to '
            'abort: ')

        if a.lower() == 'y':
            overwrite = True

    main(overwrite=overwrite, db=args.db, credentials=args.credentials)

