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
    file.seek(middle+1,0)
    arr1 = list(file.read(middle).decode(encoding='utf-8'))
    print(arr1)
    print(arr1.count('\n'))

if __name__ == '__main__':
    main()

#Task 3.3:	Explain why total number of lines from the first chunk and second chunk is larger than the number of lines calculated in the step (1) of Task 2.
    # the total number of lines from first_chunk + last_chunk = 12 > 11 because the data were split in a way that happened
    #to be in the middle of a "line", hence counting the split line twice

#3.4.	Suggest an algorithm to resolve the issue from the step (3) and implement it.
    #if prev_char not = "\n", then last_chunk = last_chunk - 1

#3.5.	Check the algorithm of step (4) with multiple chunks. Define a chunk size to be 16MB. Write a function that process “mydata.csv “
# in chunks and count number of lines for each chunk. For example, first chunk will be 0-16MB, second chunk 16MB-32BM, and so on, until the last chunk, which might be smaller.
    #change last_chunk - 1 to next_chunk -1
