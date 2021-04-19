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
import code
from timeit import default_timer as timer
from datetime import datetime

import pandas as pd

expected_conns_columns=("name","driver","server","database","user","password")
expected_query_columns=("source","dest","mode","query","table")


def logPrint(psErrorMessage, p_logfile=''):
    sMsg = '{0}: {1}'.format(str(datetime.now()), psErrorMessage)
    print(sMsg, file = sys.stderr)
    if p_logfile!='':
        print(sMsg, file = p_logfile)


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
        if c["name"][i]=="" or c["name"][i][0]=="#":
            continue

        logPrint("trying to connect to datasource[{0}]...".format(c["name"][i]))
        if c["driver"][i]=="pyodbc":
            sGetVersion='SELECT version()'
            try:
                import pyodbc
                nc=pyodbc.connect(driver="{ODBC Driver 17 for SQL Server}", server=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i],encoding = "UTF-8", nencoding = "UTF-8" )
            except (Exception, pyodbc.DatabaseError) as error:
                logPrint(error)
                sys.exit(1)

        if c["driver"][i]=="cx_Oracle":
            sGetVersion='SELECT * FROM V$VERSION'
            try:
                import cx_Oracle
                nc=cx_Oracle.connect(c["user"][i], c["password"][i], "{0}/{1}".format(c["server"][i], c["database"][i]), encoding = "UTF-8", nencoding = "UTF-8" )
            except (Exception) as error:
                logPrint(error)
                sys.exit(1)

        if c["driver"][i]=="psycopg2":
            sGetVersion='SELECT version()'
            try:
                import psycopg2
                nc=psycopg2.connect(host=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i])
            except (Exception) as error:
                logPrint(error)
                sys.exit(1)

        if c["driver"][i]=="mysql":
            sGetVersion='SELECT version()'
            try:
                import mysql.connector
                nc=mysql.connector.connect(host=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i])
            except (Exception) as error:
                logPrint(error)
                sys.exit(1)

        if c["driver"][i]=="mariadb":
            sGetVersion='SELECT version()'
            try:
                import mariadb
                nc=mariadb.connect(host=c["server"][i], database=c["database"][i], user=c["user"][i], password=c["password"][i])
            except (Exception) as error:
                logPrint(error)
                sys.exit(1)

        cur=nc.cursor()
        logPrint("Testing connection, getting version with [{0}]...".format(sGetVersion))
        try:
            cur.execute(sGetVersion)
            db_version = cur.fetchone()
            logPrint("ok, connected to DB version: {0}".format(db_version))
            cur.close()
        except (Exception) as error:
            logPrint(error)

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

        if source not in _connections:
            logPrint("ERROR: data source [{0}] not declared on connections.csv. giving up.".format(source))
            sys.exit(2)
        if dest not in _connections:
            logPrint("ERROR: data destination [{0}] not declared on connections.csv. giving up.".format(dest))
            sys.exit(2)

def copyData(p_connections,p_queries):
    for i in range(0,len(p_queries)):
        source=p_queries["source"][i]
        if source=="" or source[0]=="#":
            continue
        dest=p_queries["dest"][i]
        mode=p_queries["mode"][i]
        query=p_queries["query"][i]
        table=p_queries["table"][i]

        sLogFile="{0}.{1}.running.log".format(dest,table)
        fLogFile=open(sLogFile,'a')

        logPrint("starting copy from [{0}] to [{1}].[{2}], with query:[{3}]".format(source,dest,table,query),fLogFile)

        try:
            bErrorOccurred=False
            iTotalDataLines = 0

            if mode.upper()=='T':
                logPrint("cleaning up table (truncate) [{0}].[{1}]".format(dest,table) ,fLogFile)
                cCleanData = p_connections[dest].cursor()
                cCleanData.execute("truncate table {0}".format(table))
                cCleanData.close()

            if mode.upper()=='D':
                logPrint("cleaning up table (delete) [{0}].[{1}]".format(dest,table) ,fLogFile)
                cCleanData = p_connections[dest].cursor()
                cCleanData.execute("delete from {0}".format(table))
                cCleanData.close()

            cGetData = p_connections[source].cursor()
            cPutData = p_connections[dest].cursor()

            logPrint('running source query...', fLogFile)

            tStart=timer()
            cGetData.execute(query)

            iCols = 0
            sColsPlaceholders = ''
            sColNames=''
            for col in cGetData.description:

                iCols = iCols+1
                sColNames = sColNames + '"{0}",'.format(col[0])
                sColsPlaceholders = sColsPlaceholders + "%s,"

            sColNames = sColNames[:-1]
            sColsPlaceholders = sColsPlaceholders[:-1]

            iQuery=''
            if "insert_cols" in p_queries:
                sOverrideCols=str(p_queries["insert_cols"][i])
                if sOverrideCols != '':
                    if sOverrideCols == '@':
                        iQuery = "INSERT INTO {0} VALUES ({1})".format(table,sColsPlaceholders)
                        sIcolType="from destination"
                    elif sOverrideCols == '@l':
                        iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.lower(),sColsPlaceholders)
                        sIcolType="from source, lowercase"
                    elif sOverrideCols == '@u':
                        iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.upper(),sColsPlaceholders)
                        sIcolType="from source, upercase"
                    elif sOverrideCols != 'nan':
                        iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sOverrideCols,sColsPlaceholders)
                        sIcolType="overridden"
            if iQuery=='':
                iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames,sColsPlaceholders)
                sIcolType="from source"

            logPrint("insert query (cols={0}): [{1}]".format(sIcolType,iQuery) ,fLogFile)
            iDataRows = 0
            logPrint('entering insert loop...',fLogFile)
            while True:
                oData = cGetData.fetchmany(1000)
                if not oData:
                    break
                iResult  = cPutData.executemany(iQuery, oData)
                p_connections[dest].commit()
                iDataRows = iDataRows + cPutData.rowcount

            cPutData.close()
            cGetData.close()
            iTotalDataLines += iDataRows
            logPrint('{0} rows copied in {1:.2f} seconds.'.format(iDataRows, timer() - tStart), fLogFile)
        except (Exception) as error:
            bErrorOccurred=True
            p_connections[dest].rollback()
            logPrint("ERROR: [{0}]".format(error), fLogFile)
        finally:
            fLogFile.close()
            if bErrorOccurred:
                sLogFileFinalName = "{0}.{1}.ERROR.log".format(dest,table)
            else:
                sLogFileFinalName = "{0}.{1}.ok.log".format(dest,table)
            if bErrorOccurred or mode==mode.upper():
                os.rename(sLogFile, sLogFileFinalName)

# MAIN


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
