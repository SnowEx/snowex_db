import logging
import getpass
import os
from import_logger import get_logger
from sqlalchemy import text
from snowexsql.tables.base import Base  
from snowexsql.db import db_session_with_credentials

logging.basicConfig(level=logging.INFO)
LOG = get_logger()


def create_tables(engine):
    """
    Create tables from ORM defined in snowexsql.tables.base.Base

    Args:
        engine: SQLAlchemy engine instance connected to the target database
    """
    LOG.info("Creating tables if they do not exist...")
    Base.metadata.create_all(engine)

def create_role_and_grants(conn, admin_pw, user_pw):
    """
    Create admin and user roles for the database. Additional roles can be 
    added as needed.

    Args:
        engine: SQLAlchemy engine instance connected to the target database
        admin_pw: Password for the snowex_admin role, supplied at runtime
        user_pw: Password for the snowex_user role, supplied at runtime
    """
    LOG.info("Setting up roles and permissions...")

    conn.execute(text(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'snowex_admin') THEN
            CREATE ROLE snowex_admin WITH LOGIN PASSWORD '{admin_pw}';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'snowex_api') THEN
            CREATE ROLE snowex_api WITH LOGIN PASSWORD '{user_pw}';
        END IF;
    END
    $$;
    """))
        # Grant usage and default privileges
    conn.execute(text("GRANT USAGE ON SCHEMA public TO snowex_admin;"))
    conn.execute(text("""
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON TABLES TO snowex_admin;
    """))
    conn.execute(text("""
    GRANT ALL ON SCHEMA public TO snowex_admin;
    """))
    conn.execute(text("GRANT USAGE ON SCHEMA public TO snowex_api;"))
    conn.execute(text("""
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO snowex_api;
    """))
    LOG.info("Role and permissions configured.")


POSTGIS_SETTINGS = {
    "postgis.enable_outdb_rasters": "1",
    "postgis.gdal_enabled_drivers": "'ENABLE_ALL'"
}

# Optional GDAL VSI options (e.g., AWS credentials)
GDAL_VSI_OPTIONS = os.getenv('GDAL_VSI_OPTIONS') 
# Example: "AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=yyy" 

if GDAL_VSI_OPTIONS:
    POSTGIS_SETTINGS["postgis.gdal_vsi_options"] = f"'{GDAL_VSI_OPTIONS}'"

def apply_postgis_settings(conn, dbname):
    """
    Apply PostGIS settings to the database.
    
    Args:
    engine: SQLAlchemy engine instance connected to the target database
    """
    LOG.info("Connecting to database...")
    for key, value in POSTGIS_SETTINGS.items():
        LOG.info(f"Applying setting: {key} = {value}")
        conn.execute(text(f"ALTER DATABASE {dbname}  \
                            SET {key} = {value};"))
    LOG.info("Reloading PostgreSQL configuration...")
    conn.execute(text("SELECT pg_reload_conf();"))
    LOG.info("PostGIS configuration applied successfully.")

def main():
    admin_pw = getpass.getpass("Enter password for snowex_admin: ")
    user_pw = getpass.getpass("Enter password for snowex_user: ")
    with db_session_with_credentials() as (engine, _session):
        create_tables(engine)
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            dbname = engine.url.database
            create_role_and_grants(conn, admin_pw, user_pw)
            apply_postgis_settings(conn, dbname)

if __name__ == "__main__":
    LOG.info("Initializing database...")
    main()
    LOG.info("Database initialization complete.")



