import sqlite3
from sqlite3 import Error
import pandas as pd
import glob
import os

def cvs_to_sqlite(dbName,path_recebidos,tbName):
    con = sqlite3.connect(dbName)
    
    all_files = glob.glob(path_recebidos + "\*" + ".csv")
    n=0
    li=[]    
    for filename in all_files:  
        try:
            df = pd.read_csv(filename,sep=';')
            df.to_sql(tbName, con, if_exists='append', index=False)
            n+=1
        except:
            li.append(filename)
    arq_ok=n
    return arq_ok, li


def get_column_names_from_db_table(dbName, table_name):
    con = sqlite3.connect(dbName)
    sql_cursor= con.cursor()

    table_column_names = 'PRAGMA table_info(' + table_name + ');'
    sql_cursor.execute(table_column_names)
    table_column_names = sql_cursor.fetchall()

    column_names = list()

    for name in table_column_names:
        column_names.append(name[1])

    return column_names