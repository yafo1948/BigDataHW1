import dask.dataframe as dd
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq

PANDAS_FILE = 'mydatapandas.parquet'

DASK_FILE = 'mydatadask.parquet'

PYARROW_FILE = 'mydatapyarrow.parquet'

MYDATA_CSV = 'mydata.csv'


def main():
    file = open(MYDATA_CSV, 'rb')
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1.count('\n'))
    #
    # reader = csv.reader(file)
    # lines = len(list(reader))
    # print(lines)

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
    # 2. Dask is not a pure library but more of a parallel computation framework (much like spark)
    # 3. A Dask DataFrame is partitioned row-wise, grouping rows by index value for efficiency.
    # which means it'll add more info per given row when compared to a pandas row


if __name__ == '__main__':
    main()
