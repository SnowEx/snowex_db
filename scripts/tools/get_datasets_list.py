"""
Builds list of types,data, instruments and doi in the db
"""
from snowexsql.api import PointData, LayerData, ImageData
from snowexsql.db import get_db
from snowexsql.api import DB_NAME
from snowexsql.conversions import query_to_pandas
from pathlib import Path
from datetime import datetime

import pandas as pd

def get_datasets(table_cls):
    engine, session = get_db(DB_NAME, credentials=None)

    print(f'Collecting datasets in {table_cls.__name__}...')
    qry = session.query(table_cls.site_name, table_cls.type, table_cls.instrument,
                        table_cls.doi)
    qry = qry.order_by(table_cls.site_name).limit(100).distinct()
    datasets = query_to_pandas(qry, engine)
    session.close()
    datasets = datasets.astype(str)
    datasets[datasets=='None'] = "N/A"
    return datasets


def main():
    today = datetime.today().strftime('%Y-%m-%d')
    outfile = f'snowex_datasets_{today}.txt'
    points = get_datasets(PointData)
    layers = get_datasets(LayerData)
    rasters = get_datasets(ImageData)

    print(f"Writing results to {outfile}...")
    msg = "{:<30}{:<30}{:<30}{:<30}\n"
    hdr = msg.format("Site Name", 'Dataset', 'Instrument', 'DOI')

    with open(outfile, mode='w+') as fp:
        fp.write(f'Updated {today}\n')
        # Write out a header
        fp.write(hdr)
        fp.write('='*len(hdr)+'\n')

        # Write out results
        for df in [points, layers, rasters]:
            for i, row in df.iterrows():
                fp.write(msg.format(row['site_name'].title(),
                                    row['type'], row['instrument'],
                                    row['doi']))


if __name__ == '__main__':
    main()