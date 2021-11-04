# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import csv
import random
import sqlite3

import numpy as np
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import dask.dataframe as dd

PANDAS_FILE = 'mydatapandas.parquet'

DASK_FILE = 'mydatadask.parquet'

PYARROW_FILE = 'mydatapyarrow.parquet'

MYDATA_CSV = 'mydata.csv'

NUM_OF_RECORDS = 10


def main():
    file = open(MYDATA_CSV)
    reader = csv.reader(file)
    lines = len(list(reader))
    print(lines)

    # Convert csv to Parquet

    # Using PyArrow
    table = pv.read_csv(MYDATA_CSV)
    pq.write_table(table, '%s' % PYARROW_FILE)

    # Using Dask
    df = dd.read_csv(MYDATA_CSV)
    df.to_parquet('%s' % DASK_FILE, write_index=False)

    # Using Panadas
    df = pd.read_csv(MYDATA_CSV)
    df.to_parquet('%s' % PANDAS_FILE)


    # The diffences between Dask and the other two methods of converting the
    # CSV file into Parquet are
    # 1. Dask also contains a metadata folder
    # 2. Dask is not a pure library but more of a parallel computation framework
    # (much like spark)
    # 3. <Jordan - fill in the rest please>

if __name__ == '__main__':
    main()


