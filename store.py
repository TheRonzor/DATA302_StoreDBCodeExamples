import sqlite3
import pandas as pd

def Connect():
    conn = sqlite3.connect('../databases/store.db')
    curs = conn.cursor()
    return conn, curs

def RunAction(sql, params=None):
    conn, curs = Connect()
    if params is not None:
        curs.execute(sql, params)
    else:
        curs.execute(sql)
    conn.close()
    return
    
def RunQuery(sql, params=None):
    conn, curs = Connect()
    if params is not None:
        results = pd.read_sql(sql, conn, params=params)
    else:
        results = pd.read_sql(sql, conn)
    conn.close()
    return results