"""
File to remove all snowpits from the database
"""
import argparse
from snowexsql.api import db_session
from snowexsql.data import LayerData
from snowexsql.db import get_db


def main():
    parser = argparse.ArgumentParser(
        description='Script to create our databases using the python library')
    parser.add_argument('--db', dest='db', default='snowex',
                        help='Name of the database locally to add tables to')
    parser.add_argument('--dry_run', dest='dry_run', action='store_true',
                        help='Try a dry run or not')
    parser.add_argument('--credentials', dest='credentials',
                        default='./credentials.json',
                        help='Past to a json containing')
    args = parser.parse_args()

    credentials = args.credentials
    db_name = f'localhost/{args.db}'
    dry_run = args.dry_run

    # All measurement 'types' associate with pits
    types_pit = [
        'sample_signal', 'grain_size', 'density', 'reflectance',
        'permittivity', 'lwc_vol', 'manual_wetness',
        'equivalent_diameter', 'specific_surface_area', 'grain_type',
        'temperature', 'hand_hardness'
    ]
    # Start a session
    engine, session = get_db(db_name, credentials=credentials)
    print(f"Connected to {db_name}")
    try:
        q = session.query(LayerData).filter(
            LayerData.pit_id is not None  # Filter to results with pit id
        ).filter(
            LayerData.type.in_(types_pit)  # Filter to correct type
        )
        result = q.count()
        # Rough count of pits
        estimated_number = int(result / float(len(types_pit)) / 10.0)
        print(f"Found {result} records")
        print(f"This is roughly {estimated_number} pits")
        if dry_run:
            print("THIS IS A DRYRUN, not deleting")
        else:
            if result > 0:
                print("Deleting pits from the database")
                # Delete
                q.delete()
                session.commit()
            else:
                print("No results, nothing to delete")
        session.close()
    except Exception as e:
        print("Errored out, rolling back")
        print(e)
        session.rollback()
        raise e

    print("Done")


if __name__ == '__main__':
    main()
