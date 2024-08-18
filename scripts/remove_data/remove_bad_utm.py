"""
A set of data got uploaded with a bad EPSG value. This script removes them
"""
from snowexsql.data import LayerData, PointData
from snowexsql.db import get_db


def delete_data(session, qry):
    ans = input(f"You are about to delete {qry.count()}, Continue? (Y/n)")
    if ans == 'Y':
        qry.delete()
        session.commit()
    else:
        print('Aborted!')


def main():
    credentials = ''

    print('Querying DB for GPR with bad EPSG...')
    # Remove GPR data with bad EPSG in AK
    engine, session = get_db("localhost/snowex", credentials=credentials)
    # Filter to AK
    qry = session.query(PointData).filter(PointData.utm_zone==6)
    # Filter to GPR used in AK by CSU
    qry = qry.filter(PointData.observers=='Randall Bonnell')
    qry = qry.filter(PointData.instrument == 'pulseEkko pro 1 GHz GPR')
    delete_data(session, qry)

    print('Querying DB for Pits with bad EPSG...')
    # Delete the AK pits with bad EPSG.
    qry = session.query(LayerData).filter(LayerData.utm_zone==6)
    types_pit = ['sample_signal', 'grain_size', 'density',
                 'reflectance', 'permittivity', 'lwc_vol',
                 'manual_wetness', 'equivalent_diameter',
                 'specific_surface_area', 'grain_type','temperature',
                 'hand_hardness'
                 ]
    qry = qry.filter(LayerData.types.in_(types_pit))
    delete_data(session, qry)

    session.close()

if __name__ == '__main__':
    main()
