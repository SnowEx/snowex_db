"""
A set of data got uploaded with a bad EPSG value. This script removes them
"""
from snowexsql.data import LayerData, PointData
from snowexsql.api import DB_NAME, db_session

def delete_data(session, qry):
    ans = input(f"You are about to delete {qry.count()}, Continue? (Y/n)")
    if ans == 'Y':
        qry.delete()
        session.commit()
    else:
        print('Aborted!')


def main():

    # Remove GPR data with bad EPSG in AK
    with (db_session(DB_NAME) as (session, engine)):
        # Filter to alaska
        qry = session.query(PointData).filter(PointData.utm_zone==6)
        # Filter to GPR by Randall
        qry = qry.filter(PointData.observers=='Randall Bonnell')
        qry = qry.filter(PointData.instrument == 'pulseEkko pro 1 GHz GPR')
        delete_data(session, qry)

        # Delete the AK pits with bad EPSG.
        qry = session.query(LayerData).filter(LayerData.utm_zone==6)
        types_pit = ['sample_signal', 'grain_size', 'density',
                     'reflectance', 'permittivity', 'lwc_vol',
                     'manual_wetness', 'equivalent_diameter',
                     'specific_surface_area', 'grain_type','temperature',
                     'hand_hardness'
                     ]
        qry = qry.filter(PointData.types.in_(types_pit))
        delete_data(session, qry)

if __name__ == '__main__':
    main()
