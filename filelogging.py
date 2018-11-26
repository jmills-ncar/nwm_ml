import sqlite3
import pathlib


def create_db(sqlite_path: str,
              table_name: str):

    conn = sqlite3.connect(sqlite_path, timeout=30, isolation_level=None)
    with conn:
        cur = conn.cursor()

        file_index_name = table_name + '_' + 'filename_idx'
        status_index_name = table_name + '_' + 'status_idx'

        cur.execute('''CREATE TABLE IF NOT EXISTS {0}("filename","status")'''. \
                    format(table_name))
        cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS {0} ON {1}(filename)'''. \
                    format(file_index_name, table_name))
        cur.execute('''CREATE INDEX IF NOT EXISTS {0} ON {1}(status)'''. \
                    format(status_index_name, table_name))
        cur.close()
    conn.close()


def write_log(sqlite_path: str,
              forecast: str,
              filetype: str,
              files: list,
              failed: bool):

    # Get table name
    if filetype == 'timeslice':
        table_name = 'timeslice'
    else:
        table_name = forecast + '_' + filetype

    # connect to db and create database if does not exist
    if not pathlib.Path(sqlite_path).exists():
        create_db(sqlite_path,table_name)

    # Connect to DB for writing
    conn = sqlite3.connect(sqlite_path, timeout=30, isolation_level=None)
    with conn:
        cur = conn.cursor()
        # Insert links into table
        if failed:
            file_tuples = [(str(file), 'failed') for file in files]
        else:
            file_tuples = [(str(file), 'success') for file in files]

        sql = "insert into {0}(filename,status) " \
              "VALUES(?,?)".format(table_name)
        cur.executemany(sql, file_tuples)
        cur.close()
        conn.commit()
    conn.close()


def read_log(sqlite_path: str,
             forecast: str,
             filetype: str,
             failed: bool):

    # Get table name
    if filetype == 'timeslice':
        table_name = 'timeslice'
    else:
        table_name = forecast + '_' + filetype

    # connect to db and create database if does not exist
    if not pathlib.Path(sqlite_path).exists():
        create_db(sqlite_path,table_name)

    # Connect to DB for reading
    conn = sqlite3.connect(sqlite_path, timeout=30, isolation_level=None)
    with conn:
        conn.row_factory = lambda cursor, row: row[0]
        cur = conn.cursor()
        if failed:
            query = "SELECT filename FROM {0} where status='failed'".\
                format(table_name)
        else:
            query = "SELECT filename FROM {0} where status='success'".\
                format(table_name)
        cur.execute(query)
        files = cur.fetchall()
        cur.close()
    conn.close()

    return files
