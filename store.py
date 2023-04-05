import sqlite3
import pandas as pd
from traceback import print_exc as pe

def Connect():
    conn = sqlite3.connect('../databases/store.db')
    curs = conn.cursor()
    curs.execute("PRAGMA foreign_keys=ON;")
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

def RebuildTables():
    RunAction("DROP TABLE IF EXISTS tOrderDetail;")
    RunAction("DROP TABLE IF EXISTS tOrder;")
    RunAction("DROP TABLE IF EXISTS tProd;")
    RunAction("DROP TABLE IF EXISTS tCust;")
    RunAction("DROP TABLE IF EXISTS tZip;")
    RunAction("DROP TABLE IF EXISTS tState;")
    
    sql = """
    CREATE TABLE tCust (
        cust_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first TEXT NOT NULL,
        last TEXT NOT NULL,
        addr TEXT NOT NULL,
        zip TEXT NOT NULL REFERENCES tZip(zip)
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tZip (
        zip TEXT PRIMARY KEY CHECK(length(zip)==5),
        city TEXT NOT NULL,
        st TEXT NOT NULL REFERENCES tState(st)
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tState (
        st TEXT PRIMARY KEY CHECK(length(st)==2),
        state TEXT NOT NULL
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tOrder (
        order_id INTEGER PRIMARY KEY AUTOINCREMENT,
        cust_id INTEGER NOT NULL REFERENCES tCust(cust_id),
        date TEXT NOT NULL CHECK(date LIKE '____-__-__')
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tOrderDetail (
        order_id INTEGER NOT NULL REFERENCES tOrder(order_id),
        prod_id INTEGER NOT NULL REFERENCES tProd(prod_id),
        qty INTEGER NOT NULL CHECK(qty>0),
        PRIMARY KEY (order_id, prod_id)
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tProd (
        prod_id INTEGER PRIMARY KEY,
        prod_desc TEXT NOT NULL,
        unit_price REAL NOT NULL
    );"""
    RunAction(sql)
    
    return

def LoadTable(df, table_name):
    '''df should have column names identical to the database table column names'''
    
    conn, curs = Connect()
    
    sql = "INSERT INTO " + table_name + \
      ' (' + ','.join(list(df.columns)) +') VALUES (:' + \
      ',:'.join(list(df.columns)) + ');'
    
    try:
        for i, row in enumerate(df.to_dict(orient='records')):
            curs.execute(sql, row)
    except:
        print(i)
        print(row)
        pe()
        conn.rollback()
        conn.close()
        return False
    
    conn.commit()
    conn.close()
    return True

def LoadLookups():
    tProd = pd.read_csv('data/lookups/prods.csv')
    tState = pd.read_csv('data/lookups/states.csv')
    tZip = pd.read_csv('data/lookups/zips.csv', dtype={'zip':str})

    tProd.columns = ['prod_id', 'prod_desc', 'unit_price']
    tZip.columns = ['zip', 'city', 'st']

    if not LoadTable(tProd, 'tProd'):
        return False
    if not LoadTable(tState,'tState'):
        return False
    if not LoadTable(tZip, 'tZip'):
        return False
    
    return True
    
def RebuildDB():
    RebuildTables()
    LoadLookups()
    return