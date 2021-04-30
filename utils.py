import json
import pyodbc
import requests


def read_json(fileName):
    with open(fileName, 'r', encoding='utf-8') as outfile:
        data = json.load(outfile)
        return data


def connect(confdb, autocommit=False):
    DB = confdb['DB']
    server = confdb['server']
    UID = confdb['UID']
    PWD = confdb['PWD']

    query = f'''
        Driver={{SQL Server Native Client 11.0}};
        Server={server};
        Database={DB};
        Trusted_Connection=no;
        UID={UID};
        PWD={PWD};
    '''

    try:
        conn = pyodbc.connect(query, autocommit=autocommit)
    except Exception as e:
        raise Exception("error in pyodbc.connect(), e:", str(e))

    return conn


def truncate_table(conn, tablename, to_commit=True):
    cursor = conn.cursor()
    cursor.execute(f'truncate table {tablename}')
    if to_commit:
        conn.commit()


def get_latest_prices(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        return None

    prices = resp.json()
    return prices


def insert(conn, data, tablename, to_commit=True):
    cursor = conn.cursor()
    cursor.fast_executemany = True

    query = f"""
        insert into {tablename} (symbol, price) 
        values (?,?);
    """
    cursor.executemany(query, data)
    conn.commit()
