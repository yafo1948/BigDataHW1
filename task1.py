import csv
import sqlite3

import numpy as np
import pandas as pd

MYDATA_CSV = 'mydata.csv'

NUM_OF_RECORDS = 1000000


def main():
    fruit = ['Orange', 'Grape', 'Apple', 'Banana', 'Pineapple', 'Avocado']
    color = ['Red', 'Green', 'Yellow', 'Blue']
    rng = np.random.default_rng(123)
    df = pd.DataFrame()
    df["fruit"] = np.random.choice(fruit, NUM_OF_RECORDS)
    df["price"] = rng.integers(low=10, high=101, size=NUM_OF_RECORDS)
    df["color"] = np.random.choice(color, NUM_OF_RECORDS)
    df["id"] = df.index + 1
    df.to_csv('%s' % MYDATA_CSV, index=False)
    print("end")

    database = r"mydb.db"

    sql_create_mydata_table = """CREATE TABLE IF NOT EXISTS mydata (
                                        id integer PRIMARY KEY,
                                        fruit text,
                                        price text,
                                        color text
                                    );"""

    # create a database connection
    conn = create_connection(database)

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


if __name__ == '__main__':
    main()
