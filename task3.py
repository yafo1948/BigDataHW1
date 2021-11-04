# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import csv
import random
import sqlite3

import numpy as np
import pandas as pd
#import pyarrow.csv as pv
#import pyarrow.parquet as pq
import dask.dataframe as dd
import os

PANDAS_FILE = 'mydatapandas.parquet'

DASK_FILE = 'mydatadask.parquet'

PYARROW_FILE = 'mydatapyarrow.parquet'

MYDATA_CSV = 'mydata.csv'

NUM_OF_RECORDS = 10


def main():
    b = os.path.getsize(MYDATA_CSV)
    print(b)
    middle = int(b/2)
    print(middle)
    first_chunk(middle)
    last_chunk(middle)

def first_chunk(middle):
    file = open(MYDATA_CSV,"rb")
    arr=list(file.read(middle).decode(encoding='utf-8'))
    print(arr)
    print(arr.count('\n')+1)

def last_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    file.seek(middle,0)
    arr1 = list(file.read(middle).decode(encoding='utf-8'))
    print(arr1)
    print(arr1.count('\n'))

if __name__ == '__main__':
    main()


