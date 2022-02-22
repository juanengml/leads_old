import sqlite3
from sqlite3 import Error
import pandas as pd
import glob
import os


def df_to_sqlite(df: pd.DataFrame, db_file, tbName):
    conn = sqlite3.connect(db_file)
    return df.to_sql(tbName, conn, if_exists='append', index=False)


def get_column_names_from_db_table(dbName, table_name):
    con = sqlite3.connect(dbName)
    sql_cursor = con.cursor()

    table_column_names = 'PRAGMA table_info(' + table_name + ');'
    sql_cursor.execute(table_column_names)
    table_column_names = sql_cursor.fetchall()

    column_names = list()

    for name in table_column_names:
        column_names.append(name[1])

    return column_names
