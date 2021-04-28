#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
script to copy loads of data between databases
- reads by default connections.csv and queries.csv
- parameters:
    -- connections file (csv, tab delimited)
    -- queries file (csv, tab delimited)

"""

import sys
import os
import signal
import code
from timeit import default_timer as timer
from time import sleep
from datetime import datetime

import threading
import queue

import pandas as pd

def initConnections(p_filename):
    conns={}
    try:
        c=pd.read_csv(p_filename, delimiter='\t')
    except (Exception) as error:
        logPrint("error Loading [{0}]: [{1}]".format(p_filename, error))
        sys.exit(1)
    for ecol in expected_conns_columns:
        if ecol not in c:
            logPrint("Missing column on connections file: [{0}]".format(ecol))
            sys.exit(1)

    for i in range(0,len(c)):
        cName=c["name"][i]
        if cName=="" or cName[0]=="#":
            continue

        logPrint("initConnections[{0}]: trying to connect...".format(cName))
        if c["driver"][i]=="pyodbc":
            sGetVersion='SELECT version()'
            try:
                import pyodbc
                nc=pyodbc.connect(driver="{ODBC Driver 17 for SQL Server}", server=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i],encoding = "UTF-8", nencoding = "UTF-8" )
            except (Exception, pyodbc.DatabaseError) as error:
                logPrint("initConnections({0}): DB error [{1}]".format(cName,error))
                sys.exit(1)

        if c["driver"][i]=="cx_Oracle":
            sGetVersion='SELECT * FROM V$VERSION'
            try:
                import cx_Oracle
                nc=cx_Oracle.connect(c["user"][i], c["password"][i], "{0}/{1}".format(c["server"][i], c["database"][i]), encoding = "UTF-8", nencoding = "UTF-8" )
            except (Exception) as error:
                logPrint("initConnections({0}): DB error [{1}]".format(cName,error))
                sys.exit(1)

        if c["driver"][i]=="psycopg2":
            sGetVersion='SELECT version()'
            try:
                import psycopg2
                nc=psycopg2.connect(host=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i])
            except (Exception) as error:
                logPrint("initConnections({0}): DB error [{1}]".format(cName,error))
                sys.exit(1)

        if c["driver"][i]=="mysql":
            sGetVersion='SELECT version()'
            try:
                import mysql.connector
                nc=mysql.connector.connect(host=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i])
            except (Exception) as error:
                logPrint("initConnections({0}): DB error [{1}]".format(cName,error))
                sys.exit(1)

        if c["driver"][i]=="mariadb":
            sGetVersion='SELECT version()'
            try:
                import mariadb
                nc=mariadb.connect(host=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i])
            except (Exception) as error:
                logPrint("initConnections({0}): DB error [{1}]".format(cName,error))
                sys.exit(1)

        cur=nc.cursor()
        logPrint("initConnections({0}): Testing connection, getting version with [{1}]...".format(cName, sGetVersion))
        try:
            cur.execute(sGetVersion)
            db_version = cur.fetchone()
            logPrint("initConnections({0}): ok, connected to DB version: {1}".format(cName, db_version))
            cur.close()
        except (Exception) as error:
            logPrint("initConnections({0}): DB error [{1}]".format(cName, error))

        conns[c["name"][i]]=nc

    return conns

def initQueries(p_filename):
    try:
        q=pd.read_csv(p_filename,delimiter='\t')
    except (Exception) as error:
        logPrint("error Loading [{0}]: [{1}]".format(p_filename, error))
        sys.exit(1)
    return q

def preCheck(p_connections, p_queries):
    logPrint("checking sources and destinations...")
    for ecol in expected_query_columns:
        if ecol not in p_queries:
            logPrint("Missing column on queries file: [{0}]".format(ecol))
            sys.exit(2)

    for i in range(0,len(p_queries)):
        source=p_queries["source"][i]
        if source=="" or source[0]=="#":
            continue
        dest=p_queries["dest"][i]
        mode=p_queries["mode"][i]
        query=p_queries["query"][i]
        table=p_queries["table"][i]

        if source not in p_connections:
            logPrint("ERROR: data source [{0}] not declared on connections.csv. giving up.".format(source))
            sys.exit(2)
        if dest not in p_connections:
            logPrint("ERROR: data destination [{0}] not declared on connections.csv. giving up.".format(dest))
            sys.exit(2)


# MAIN
def Main():
    global g_readT
    global g_writeT
    global g_fullstop

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    if len(sys.argv) < 3:
        q_filename='queries.csv'
    else:
        q_filename=sys.argv[2]

    if len(sys.argv) < 2:
        c_filename='connections.csv'
    else:
        c_filename=sys.argv[1]

    _connections=initConnections(c_filename)
    _queries=initQueries(q_filename)

    preCheck(_connections,_queries)
    copyData(_connections,_queries)

if __name__ == '__main__':
    Main()