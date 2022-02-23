import sqlite3
import pandas as pd
from xmlrpc.client import Boolean
db_file = '/home/marcello/git/lumini/leads_old/database/chinook.db'
db_file_leads = '/home/marcello/git/lumini/leads_old/database/leads.db'

def do_maintainer_databases() -> None:
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor(db_file)
        cursor.execute('PRAGMA auto_vacuum = FULL;')
    conn.close()


def df_to_sqlite(df: pd.DataFrame, db_file, tbName) -> pd.DataFrame:
    conn = sqlite3.connect(db_file)
    return df.to_sql(tbName, conn, if_exists='append', index=False)

def do_select(db_file, sql, table_name) -> pd.DataFrame:
    """Executa e mostra o resultado de uma consulta SQL.
    
    Parameters:
    ----------
        db_file: string
        sql: string

    Returns:
    ----------
        df: pandas.DataFrame
        False: Boolean
    
    Example:
    ----------
        sql = 'SELECT * FROM tracks LIMIT 10'
        do_select(db_file, sql)
    """
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            table_column_names = 'PRAGMA table_info(' + table_name + ');'
            cursor.execute(table_column_names)
            columns = cursor.fetchall()
            
            column_names = list()
            for name in columns:
                column_names.append(name[1])
            df = pd.DataFrame(column_names)
            
            cursor.execute(sql)
            rows = cursor.fetchall()
        conn.close()
        return pd.DataFrame(data=rows, columns=column_names)
    except Exception as ex:
        print(ex)
        conn.close()
        return False


def do_ddl(db_file, sql_ddl) -> Boolean:
    """Executa um script SQL/DDL para criar ou alterar a estrutura de tabelas e etc.
    
    Parameters:
    ----------
        db_file: string
        sql: string

    Returns:
    ----------
        True: script executado com sucesso
        False: ocorreu erro
    
    Example:
    ----------
        ddl = '
        CREATE TABLE IF NOT EXISTS TESTE ( 
            COL_KEY INTEGER PRIMARY KEY AUTOINCREMENT, 
            COL_NAME TEXT NOT NULL
        );'
        do_ddl(db_file, ddl)

        Lista o squema de tabelas do SQLite
        -----------------------------------
        sql = '
            SELECT 
                name
            FROM 
                sqlite_schema
            WHERE 
                type ='table' AND 
                name NOT LIKE 'sqlite_%';
        '
        do_select(db_file, sql)
    """
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_ddl)
            conn.commit()
        conn.close()
        return True
    except:
        conn.close()
        return False


def do_insert(db_file, sql) -> Boolean:
    """Executa um código SQL de inserção de dados.
    
    Parameters:
    ----------
        db_file: string
        sql: string

    Returns:
    ----------
        True: script executado com sucesso
        False: ocorreu erro
    
    Example:
    ----------
        sql = '
            INSERT INTO playlists (Name) VALUES('teste audio 2');
        '
        do_insert(db_file, sql)

        Para visualizar os dados inseridos
        ----------------------------------
        sql = "
            SELECT * FROM playlists WHERE name like '%audio%' 
        "
        do_select(db_file, sql)
    """
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        conn.close()
        return True
    except:
        conn.close()
        return False


def do_update(db_file, sql) -> Boolean:
    """Executa um código SQL de alteração de dados.
    
    Parameters:
    ----------
        db_file: string
        sql: string

    Returns:
    ----------
        True: script executado com sucesso
        False: ocorreu erro
    
    Example:
    ----------
        sql = "
            UPDATE playlists
            SET name = 'teste audio 0'
            WHERE NAME = 'teste audio 1'
        "
        do_update(db_file, sql)

        Para visualizar os dados alterados
        ----------------------------------
        sql = "
            SELECT * FROM playlists WHERE name like '%teste%' 
        "
        do_select(db_file, sql)
    """
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        conn.close()
        return True
    except:
        return False


def do_delete(db_file, sql) -> Boolean:
    """Executa um código SQL de deleção de dados.
    
    Parameters:
    ----------
        db_file: string
        sql: string

    Returns:
    ----------
        True: script executado com sucesso
        False: ocorreu erro
    
    Example:
    ----------
        sql = "
            DELETE FROM playlists
            WHERE name = 'teste audio 2'
        "
        do_delete(db_file, sql)

        Para visualizar os dados deletados
        ----------------------------------
        sql = "
            SELECT * FROM playlists WHERE name like '%teste%' 
        "
        do_select(db_file, sql)
    """
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        conn.close()
        return True
    except:
        return False

do_maintainer_databases

# db_file = '/home/marcello/git/lumini/leads_old/database/chinook.db'
# sql = """
#     SELECT * FROM tracks LIMIT 100
# """
# df = do_select(db_file, sql, 'tracks')

# df.to_csv('/home/marcello/git/lumini/leads_old/df_teste.csv', index=False)