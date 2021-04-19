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

# GLOBAL VARS

expected_conns_columns=("name","driver","server","database","user","password")
expected_query_columns=("source","dest","mode","query","table")

g_dataBuffer=queue.Queue(16)
g_readRecords=queue.Queue()
g_writtenRecords=queue.Queue()

g_defaultFetchSize=1000

g_fetchSize=g_defaultFetchSize

g_readT = False
g_writeT = False
g_Working = True

def logPrint(psErrorMessage, p_logfile=''):
    sMsg = '{0}: {1}'.format(str(datetime.now()), psErrorMessage)
    print(sMsg, file = sys.stderr, flush=True)
    if p_logfile!='':
        print(sMsg, file = p_logfile, flush=True)


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

        if source not in p_connections:
            logPrint("ERROR: data source [{0}] not declared on connections.csv. giving up.".format(source))
            sys.exit(2)
        if dest not in p_connections:
            logPrint("ERROR: data destination [{0}] not declared on connections.csv. giving up.".format(dest))
            sys.exit(2)


def readData(p_connection, p_cursor):
    global g_dataBuffer
    global g_readRecords
    global g_fetchSize

    print("readData Started")
    while g_Working:
        try:
            rStart=timer()
            bData = p_cursor.fetchmany(g_fetchSize)
        except (Exception) as error:
            logPrint("DB Read Error: [{0}]".format(error))
            g_dataBuffer.put(False)
            break
        g_dataBuffer.put(bData, block=True)
        g_readRecords.put( (p_cursor.rowcount, (timer()-rStart)) )
        if not bData:
            g_dataBuffer.put((False))
            break
    print("readData Ended")
 

def writeData(p_connection, p_cursor, p_iQuery):
    global g_dataBuffer
    global g_writtenRecords

    print("writeData Started")
    while g_Working:
        try:
            bData = g_dataBuffer.get(block=False,timeout=2)
        except queue.Empty:
            continue
        if not bData:
            g_writtenRecords.put( (False, float(0)) )
            break
        iStart=timer()
        iResult  = p_cursor.executemany(p_iQuery, bData)
        p_connection.commit()
        g_writtenRecords.put( (p_cursor.rowcount, (timer()-iStart)) )
    print("writeData Ended")


def copyData(p_connections,p_queries):
    global g_fetchSize
    global g_defaultFetchSize

    for i in range(0,len(p_queries)):
        source=p_queries["source"][i]
        if source=="" or source[0]=="#":
            continue
        dest=p_queries["dest"][i]
        mode=p_queries["mode"][i]
        query=p_queries["query"][i]
        table=p_queries["table"][i]
        if "fetch_size" in p_queries:
            qFetchSize=int(p_queries["fetch_size"][i])
            if qFetchSize == 0:
                g_fetchSize=g_defaultFetchSize
            else:
                g_fetchSize=qFetchSize

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

            iTotalDataLinesRead = 0
            iTotalDataLinesWritten = 0

            bFetchRead = True
            bFinishedRead = False
            bFinishedWrite = False

            iTotalReadSecs=.001
            iTotalWrittenSecs=.001

            logPrint('entering insert loop...',fLogFile)
            g_readT=threading.Thread(target=readData, args=(p_connections[source], cGetData))
            g_readT.start()
            g_writeT=threading.Thread(target=writeData, args=(p_connections[dest], cPutData, iQuery))
            g_writeT.start()
            while g_Working:
                try:
                    if bFetchRead:
                        bFetchRead=False
                        recRead,readSecs = g_readRecords.get(block=False,timeout=1)
                        if not recRead:
                            bFinishedRead=True
                        else:
                            iTotalDataLinesRead += recRead
                            iTotalReadSecs += readSecs
                    else:
                        bFetchRead=True
                        recWritten,writeSecs = g_writtenRecords.get(block=False,timeout=1)
                        if not recWritten:
                            bFinishedWrite=True
                        else:
                            iTotalDataLinesWritten += recWritten
                            iTotalWrittenSecs += writeSecs
                except queue.Empty:
                    continue

                print("{0} records read ({1:.2f}/sec), {2} records written ({3:.2f}/sec)\r".format(iTotalDataLinesRead, (iTotalDataLinesRead/iTotalReadSecs), iTotalDataLinesWritten, (iTotalDataLinesWritten/iTotalWrittenSecs)),end='')
                if bFinishedRead and bFinishedWrite:
                    break

            cPutData.close()
            cGetData.close()
 
            logPrint('{0} rows copied in {1:.2f} seconds ({2:.2f}/sec).'.format(iTotalDataLinesWritten, (timer() - tStart), (iTotalDataLinesWritten/iTotalWrittenSecs)), fLogFile)
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

def sig_handler(signum, frame):
    global g_readT
    global g_writeT
    global g_Working
    logPrint('signal received, signaling stop to threads...')
    g_Working = False
    while True:
        try:
            dummy=g_dataBuffer.get(block=False, timeout=1)
        except queue.Empty:
            break
    try:
        if g_readT:
            print("waiting for read thread...")
            g_readT.join()
        if g_writeT:
            print("waiting for write thread...")
            g_writeT.join()
    finally:
        logPrint('thread cleanup finished.')


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