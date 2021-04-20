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

g_ErrorOccurred=False

def logPrint(psErrorMessage, p_logfile=''):
    sMsg = "{0}: {1}".format(str(datetime.now()), psErrorMessage)
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

def readData(p_index, p_connection, p_cursor):
    global g_Working
    global g_dataBuffer
    global g_readRecords
    global g_fetchSize
    global g_ErrorOccurred


    print("readData({0}): Started".format(p_index), flush=True)
    while g_Working:
        try:
            rStart=timer()
            bData = p_cursor.fetchmany(g_fetchSize)
        except (Exception) as error:
            logPrint("ReadData({0}): DB Error: [{1}]".format(p_index, error))
            g_ErrorOccurred=True
            break
        g_readRecords.put( (p_cursor.rowcount, (timer()-rStart)) )
        g_dataBuffer.put(bData, block=True)
        if not bData:
            break
    g_dataBuffer.put(False)
    g_readRecords.put((False,float(0)))
    print("\nreadData({0}): Ended".format(p_index), flush=True)
 
def writeData(p_index, p_connection, p_cursor, p_iQuery):
    global g_Working
    global g_dataBuffer
    global g_writtenRecords
    global g_ErrorOccurred

    print("writeData({0}): Started".format(p_index), flush=True)
    while g_Working:
        try:
            bData = g_dataBuffer.get(block=False,timeout=2)
        except queue.Empty:
            continue
        if not bData:
            print ("\nwriteData({0}): 'no more data' message received".format(p_index), flush=True)
            break
        iStart=timer()
        try:
            iResult  = p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except (Exception) as error:
            logPrint("writeData({0}): DB Error: [{1}]".format(p_index,error))
            p_connection.rollback()
            g_ErrorOccurred=True
            break
        g_writtenRecords.put( (p_cursor.rowcount, (timer()-iStart)) )

    g_writtenRecords.put( (False, float(0)) )
    print("\nwriteData({0}): Ended".format(p_index), flush=True)


def copyData(p_connections,p_queries):
    global g_Working
    global g_fetchSize
    global g_defaultFetchSize
    global g_ErrorOccurred

    for i in range(0,len(p_queries)):
        qIndex=i+1
        g_ErrorOccurred = False
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

        logPrint("copyData({0}): starting copy from [{1}] to [{2}].[{3}], with query:[{4}]".format(qIndex, source, dest, table,query),fLogFile)

        try:
            bErrorOccurred=False
            iTotalDataLines = 0

            if mode.upper()=='T':
                logPrint("copyData({0}): cleaning up table (truncate) [{1}].[{2}]".format(qIndex, dest,table) ,fLogFile)
                cCleanData = p_connections[dest].cursor()
                cCleanData.execute("truncate table {0}".format(table))
                cCleanData.close()

            if mode.upper()=='D':
                logPrint("copyData({0}): cleaning up table (delete) [{1}].[{2}]".format(qIndex, dest, table) ,fLogFile)
                cCleanData = p_connections[dest].cursor()
                cCleanData.execute("delete from {0}".format(table))
                cCleanData.close()

            cGetData = p_connections[source].cursor()
            cPutData = p_connections[dest].cursor()

            logPrint("copyData({0}): running source query...".format(qIndex), fLogFile)

            tStart=timer()
            cGetData.execute(query)
            logPrint("copyData({0}): source query took {1:.2f} seconds to reply.".format(qIndex, (timer() - tStart)), fLogFile)

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

            logPrint("copyData({0}): insert query (cols={1}): [{2}]".format(qIndex, sIcolType, iQuery) ,fLogFile)

            iTotalDataLinesRead = 0
            iTotalDataLinesWritten = 0

            bFetchRead = True
            bFinishedRead = False
            bFinishedWrite = False

            iTotalReadSecs=.001
            iTotalWrittenSecs=.001

            logPrint("copyData({0}): entering insert loop...".format(qIndex),fLogFile)

            while True:
                try:
                    dummy=g_dataBuffer.get(block=False,timeout=1)
                except queue.Empty:
                    break
            g_readT=threading.Thread(target=readData, args=(qIndex, p_connections[source], cGetData))
            g_readT.start()
            sleep(2)
            g_writeT=threading.Thread(target=writeData, args=(qIndex, p_connections[dest], cPutData, iQuery))
            g_writeT.start()
            print("", flush=True)
            while g_Working:
                try:
                    if bFetchRead:
                        bFetchRead=False
                        recRead,readSecs = g_readRecords.get(block=False,timeout=1)
                        if not recRead:
                            bFinishedRead=True
                        else:
                            iTotalDataLinesRead = recRead
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

                print("\r{0} records read ({1:.2f}/sec), {2} records written ({3:.2f}/sec)       ".format(iTotalDataLinesRead, (iTotalDataLinesRead/iTotalReadSecs), iTotalDataLinesWritten, (iTotalDataLinesWritten/iTotalWrittenSecs)),end='', flush=True)
                if bFinishedRead and bFinishedWrite:
                    break

            cPutData.close()
            cGetData.close()

            print ("", flush=True)
            logPrint("copyData({0}): {1} rows copied in {2:.2f} seconds ({3:.2f}/sec).".format(qIndex, iTotalDataLinesWritten, (timer() - tStart), (iTotalDataLinesWritten/iTotalWrittenSecs)), fLogFile)
        except (Exception) as error:
            g_ErrorOccurred=True
            logPrint("copyData({0}): ERROR: [{1}]".format(qIndex, error), fLogFile)
        finally:
            fLogFile.close()
            if g_ErrorOccurred:
                sLogFileFinalName = "{0}.{1}.ERROR.log".format(dest,table)
            else:
                sLogFileFinalName = "{0}.{1}.ok.log".format(dest,table)
            if bErrorOccurred or mode==mode.upper():
                os.rename(sLogFile, sLogFileFinalName)
        if not g_Working:
            break

def sig_handler(signum, frame):
    global g_readT
    global g_writeT
    global g_Working
    logPrint("\nsigHander: break received, signaling stop to threads...")
    g_Working = False
    g_ErrorOccurred = True    
    while True:
        try:
            dummy=g_dataBuffer.get(block=False, timeout=1)
        except queue.Empty:
            break
    try:
        if g_readT:
            print("sigHandler: waiting for read thread...", flush=True)
            g_readT.join()
        if g_writeT:
            print("sigHandler: waiting for write thread...", flush=True)
            g_writeT.join()
    finally:
        logPrint("sigHandler: thread cleanup finished.")


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