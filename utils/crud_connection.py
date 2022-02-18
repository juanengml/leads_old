import sqlite3
from xmlrpc.client import Boolean
db_file = '/home/marcello/git/lumini/leads_old/database/chinook.db'
sqlite3.connect(db_file).close()

def do_select(db_file, sql) -> Boolean:
    """Executa e mostra o resultado de uma consulta SQL.
    
    Parameters:
    ----------
        db_file: string
        sql: string

    Returns:
    ----------
        True: o resultado é apresentado
        False: ocorreu erro
    
    Example:
    ----------
        sql = 'SELECT * FROM tracks LIMIT 10'
        do_select(db_file, sql)
    """
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        conn.close()
        return True
    except:
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



