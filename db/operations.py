import os
import sqlite3

from config import main_path

DB_FILE = os.path.join(main_path, 'leads.db')


def do_insert(sql, values, db_file=DB_FILE) -> bool:
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
            cursor.execute(sql, values)
            conn.commit()
        conn.close()
        return True
    except:
        conn.close()
        return False


def do_select(sql, values, db_file=DB_FILE) -> list:
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
            cursor.execute(sql, values)
            data = cursor.fetchall()
        conn.close()
        return data
    except Exception as ex:
        print(ex)
        conn.close()
        return None


def do_update(sql, values, db_file=DB_FILE) -> bool:
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
            cursor.execute(sql, values)
            conn.commit()
        conn.close()
        return True
    except:
        return False
