"""
A set of data got uploaded with a bad EPSG value. This script removes them
"""
from snowexsql.tables import LayerData, PointData
from snowexsql.db import get_db


def delete_data(session, qry):
    ans = input(f"You are about to delete {qry.count()}, Continue? (Y/n)")
    if ans == 'Y':
        qry.delete()
        session.commit()
    else:
        print('Aborted!')


def remove_bad_gpr(session):
    print('Querying DB for GPR with bad EPSG...')
    # Remove GPR data with bad EPSG in AK
    qry = session.query(PointData).filter(PointData.utm_zone==6)
    # Filter to GPR used in AK by CSU
    qry = qry.filter(PointData.observers=='Randall Bonnell')
    qry = qry.filter(PointData.instrument == 'pulseEkko pro 1 GHz GPR')
    delete_data(session, qry)


def remove_bad_pits(session):
    print('Querying DB for Pits with bad EPSG...')
    # Delete the AK pits with bad EPSG.
    qry = session.query(LayerData).filter(LayerData.utm_zone==6)
    types_pit = ['sample_signal', 'grain_size', 'density',
                 'reflectance', 'permittivity', 'lwc_vol',
                 'manual_wetness', 'equivalent_diameter',
                 'specific_surface_area', 'grain_type','temperature',
                 'hand_hardness'
                 ]
    qry = qry.filter(LayerData.type.in_(types_pit))
    delete_data(session, qry)


def remove_summary_swe(session):
    print('Querying DB for summary swe with bad EPSG...')

    # Delete the AK summary swe with bad EPSG.
    types = ['snow_void', 'swe', 'density', 'depth']

    qry = session.query(PointData).filter(LayerData.utm_zone==6)
    qry = qry.filter(PointData.type.in_(types))
    qry = qry.filter(PointData.instrument==None)
    delete_data(session, qry)


def main():
    credentials = ''
    engine, session = get_db("localhost/snowex", credentials=credentials)
    # remove_bad_gpr(session)
    # remove_bad_pits(session)
    remove_summary_swe(session)
    session.close()


if __name__ == '__main__':
    main()
