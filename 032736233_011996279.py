import os
import csv
import sqlite3

import numpy as np
import pandas as pd

import dask.dataframe as dd
import pyarrow.csv as pv
import pyarrow.parquet as pq

LINE_BREAK = '\n'
MYDATA_CSV = 'mydata.csv'
NUM_OF_RECORDS = 1000000
PANDAS_FILE = 'mydatapandas.parquet'
DASK_FILE = 'mydatadask.parquet'
PYARROW_FILE = 'mydatapyarrow.parquet'
CHUNK_SIZE = 16 * 1024 * 1024
DB_NAME = 'mydb.db'


def seeder():
    fruit = ['Orange', 'Grape', 'Apple', 'Banana', 'Pineapple', 'Avocado']
    color = ['Red', 'Green', 'Yellow', 'Blue']
    rng = np.random.default_rng(123)
    df = pd.DataFrame()
    df["fruit"] = np.random.choice(fruit, NUM_OF_RECORDS)
    df["price"] = rng.integers(low=10, high=101, size=NUM_OF_RECORDS)
    df["color"] = np.random.choice(color, NUM_OF_RECORDS)
    df["id"] = df.index + 1
    df.to_csv('%s' % MYDATA_CSV, index=False)
    print("end of seeder")

    sql_create_mydata_table = """CREATE TABLE IF NOT EXISTS mydata (
                                        id integer PRIMARY KEY,
                                        fruit text,
                                        price text,
                                        color text
                                    );"""

    # create a database connection
    conn = create_connection(DB_NAME)

    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_mydata_table)
    else:
        print("Error! cannot create the database connection.")

    with open(MYDATA_CSV, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)
        query = 'insert into mydata({0}) values ({1})'
        query = query.format(','.join(columns), ','.join('?' * len(columns)))
        cursor = conn.cursor()

        for data in reader:
            cursor.execute(query, data)

        conn.commit()

        # Predicate example
        select_all_id_between_4_and_7 = """select * from mydata where id BETWEEN 4 AND 7"""

        cursor.execute(select_all_id_between_4_and_7)

        rows = cursor.fetchall()

        # Projection example
        select_id_of_all_banana = """select id from mydata where fruit=='Banana'"""

        cursor.execute(select_id_of_all_banana)

        rows = cursor.fetchall()


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        print(e)


def create_parquet_files():
    file = open(MYDATA_CSV, 'rb')
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1.count(LINE_BREAK))
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

    # The differences between Dask and the other two methods of converting the
    # CSV file into Parquet are
    # 1. Dask also contains a metadata folder
    # 2. Dask is not a pure library but more of a parallel computation framework (much like spark)
    # 3. A Dask DataFrame is partitioned row-wise, grouping rows by index value for efficiency.
    # which means it'll add more info per given row when compared to a pandas row

# ###################################################


def split_csv_files():
    file_size = os.path.getsize(MYDATA_CSV)
    print("file size: " + str(file_size) + " bytes")
    middle = int(file_size / 2)
    print("The middle point of the file in bytes: " + str(middle))
    amount_of_lines = first_chunk(middle)
    amount_of_lines += last_chunk(middle)
    print("amount of lines in file: " + str(amount_of_lines))


def first_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    arr = list(file.read(middle).decode(encoding='utf-8'))
    print(arr)
    corrected_amount_of_lines = arr.count(LINE_BREAK)
    print("amount of lines in first chunk: " + str(corrected_amount_of_lines))
    return corrected_amount_of_lines


def last_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    file.seek(middle+1, 0)
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1)
    amount_of_lines = arr1.count(LINE_BREAK)
    print(amount_of_lines)
    return amount_of_lines


# ###################################################

def resolved_algorithm():
    """
    # 1. Read 'middle' amount of bytes from the file
    # 2. Count the number of line breaks ('\n') for the first chunk
    # 3. Seek to middle+1 bytes on the file
    # 4. Read the remaining amount of bytes
    # 5. Count the number of line breaks ('\n') for the second chunk
    # 6. If the first char on the chunk is a Line break ('\n') -> subtract 1 from the count
    # 7. Return the sum of lines from both chunks
    """
    file_size = os.path.getsize(MYDATA_CSV)
    print("file size: " + str(file_size) + " bytes")
    middle = int(file_size / 2)
    print(middle)
    amount_of_lines = resolved_first_chunk(middle)
    amount_of_lines += resolved_last_chunk(middle)
    print("The amount of lines in the file: " + str(amount_of_lines))


def resolved_first_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    arr = list(file.read(middle).decode(encoding='utf-8'))
    print(arr)
    amount_of_line_breaks_in_chunk = arr.count(LINE_BREAK)

    print("amount of lines in first chunk: " + str(amount_of_line_breaks_in_chunk))
    return amount_of_line_breaks_in_chunk


def resolved_last_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    file.seek(middle+1, 0)
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1)
    amount_of_line_breaks_in_chunk = arr1.count(LINE_BREAK)

    if arr1[0] == LINE_BREAK:
        amount_of_line_breaks_in_chunk -= 1

    print(amount_of_line_breaks_in_chunk)
    return amount_of_line_breaks_in_chunk

# ###################################################


def multiple_chunks():
    file_size = os.path.getsize(MYDATA_CSV)
    print("file size: " + str(file_size) + " bytes")
    print(CHUNK_SIZE)
    current_pos = 0
    lines_counter = 0

    while current_pos < file_size:
        if current_pos + CHUNK_SIZE > file_size:
            lines_counter += multiple_last_chunk(current_pos)
        else:
            lines_counter += multiple_first_chunk(current_pos)
        current_pos += CHUNK_SIZE
    print("The total number of lines in the file: " + str(lines_counter))


def multiple_first_chunk(current_pos):
    with open(MYDATA_CSV, "rb") as file:
        file.seek(current_pos)
        arr = list(file.read(CHUNK_SIZE).decode(encoding='utf-8'))
        print(arr)
        amount_of_line_breaks = arr.count(LINE_BREAK)

    if arr[0] == LINE_BREAK:
        amount_of_line_breaks -= 1

    print("amount of lines in chunk: " + str(amount_of_line_breaks))
    return amount_of_line_breaks


def multiple_last_chunk(current_pos):
    file = open(MYDATA_CSV, "rb")
    file.seek(current_pos, 0)
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1)
    amount_of_line_breaks = arr1.count(LINE_BREAK)

    if arr1[0] == LINE_BREAK:
        amount_of_line_breaks -= 1

    print(amount_of_line_breaks)
    return amount_of_line_breaks

# ###################################################


if __name__ == '__main__':
    # Task 1
    seeder()

    # Task 2
    create_parquet_files()

    # Task 3
    split_csv_files()

    # Task 3.4
    resolved_algorithm()

    # Task 3.5
    multiple_chunks()
