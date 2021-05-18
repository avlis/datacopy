#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
script to copy loads of data between databases
- reads by default connections.csv and jobs.csv
- parameters on command line:
    -- connections file (csv, tab delimited)
    -- jobs.csv file (csv, tab delimited)
    -- log file prefix

or by ENV var:
    -- CONNECTIONS_FILE
    -- JOB_FILE
    -- LOG_FILE
    -- TEST_QUERIES (dry run)
    -- QUEUE_SIZE
    -- QUEUE_FB4NEWR
    -- REUSE_WRITERS

"""

import sys
import os
import signal
import code
from timeit import default_timer as timer
from time import sleep
from datetime import datetime

import multiprocessing as mp
mp.set_start_method('fork')

import queue

import pandas as pd

import re

# GLOBAL VARS

expected_conns_columns=("name","driver","server","database","user","password")
expected_query_columns=("source","dest","mode","query","table")

check_bd_version_cmd = {
    "pyodbc": "SELECT @@version",
    "cx_Oracle": "SELECT * FROM V$VERSION",
    "psycopg2":"SELECT version()",
    "mysql":"SELECT version()",
    "mariadb":"SELECT version()",
    "":""
}

g_connections={}
g_queries={}

queueSize=int(os.getenv('QUEUE_SIZE',256))
g_usedQueueBeforeNew=int(queueSize/int(os.getenv('QUEUE_FB4NEWR',3)))
g_dataBuffer=mp.Queue(queueSize)
g_readRecords=mp.Queue()
g_writtenRecords=mp.Queue()

g_defaultFetchSize=1000

g_logFileName = ''
g_readP = {}
g_writeP = {}
g_Working = True

if os.getenv('REUSE_WRITERS','')=='yes':
        g_ReuseWriters = True
else:
        g_ReuseWriters = False


if os.getenv('TEST_QUERIES','')=='yes':
        g_testQueries = True
else:
        g_testQueries = False

g_ErrorOccurred=False
g_nbrParallelWriters=1


def logPrint(psErrorMessage, p_logfile=''):
    sMsg = "{0}: {1}".format(str(datetime.now()), psErrorMessage)
    print(sMsg, file = sys.stdout, flush=True)
    if p_logfile!='':
        print(sMsg, file = p_logfile, flush=True)

def cx_Oracle_OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    import cx_Oracle
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

def loadConnections(p_filename):
    global g_connections

    conns={}
    try:
        c=pd.read_csv(p_filename, delimiter='\t')
    except (Exception) as error:
        logPrint("error Loading [{0}]: [{1}]".format(p_filename, error))
        sys.exit(1)
    for ecol in expected_conns_columns:
        if ecol not in c:
            logPrint("loadConnections: Missing column on connections file: [{0}]".format(ecol))
            sys.exit(1)

    for i in range(len(c)):
        cName=c["name"][i]
        if cName=="" or cName[0]=="#":
            continue
        nc={"driver": c["driver"][i], "server": c["server"][i], "database":c["database"][i], "user":c["user"][i], "password":c["password"][i]}

        conns[cName]=nc

    g_connections=conns

def initConnections(p_name, p_readOnly, p_qtd, p_logFile):
    global g_connections
    nc = {}

    if p_name in g_connections:
        c=g_connections[p_name]

    print("initConnections[{0}]: trying to connect...".format(p_name), file=sys.stderr, flush=True)
    if c["driver"]=="pyodbc":
        try:
            import pyodbc
            for x in range(p_qtd):
                nc[x]=pyodbc.connect(driver="{ODBC Driver 17 for SQL Server}", server=c["server"], database=c["database"], user=c["user"], password=c["password"],encoding = "UTF-8", nencoding = "UTF-8", readOnly=p_readOnly )
        except (Exception, pyodbc.DatabaseError) as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error), p_logFile)
            sys.exit(1)

    if c["driver"]=="cx_Oracle":
        try:
            import cx_Oracle
            for x in range(p_qtd):
                nc[x]=cx_Oracle.connect(c["user"], c["password"], "{0}/{1}".format(c["server"], c["database"]), encoding = "UTF-8", nencoding = "UTF-8" )
                nc[x].outputtypehandler = cx_Oracle_OutputTypeHandler
        except (Exception) as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error), p_logFile)
            sys.exit(1)

    if c["driver"]=="psycopg2":
        try:
            import psycopg2
            from psycopg2 import pool
            tpool = psycopg2.pool.ThreadedConnectionPool(1, p_qtd, host=c["server"], database=c["database"], user=c["user"], password=c["password"])
            for x in range(p_qtd):
                nc[x]=tpool.getconn()
                nc[x].readonly=p_readOnly
        except (Exception) as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error), p_logFile)
            sys.exit(1)

    if c["driver"]=="mysql":
        try:
            import mysql.connector
            for x in range(p_qtd):
                nc[x]=mysql.connector.connect(host=c["server"], database=c["database"], user=c["user"], password=c["password"])
        except (Exception) as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error), p_logFile)
            sys.exit(1)

    if c["driver"]=="mariadb":
        try:
            import mariadb
            for x in range(p_qtd):
                nc[x]=mariadb.connect(host=c["server"], database=c["database"], user=c["user"], password=c["password"])
        except (Exception) as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error), p_logFile)
            sys.exit(1)

    try:
        sGetVersion=check_bd_version_cmd[c["driver"]]
        cur=nc[0].cursor()
        print("initConnections({0}): Testing connection, getting version with [{1}]...".format(p_name, sGetVersion), file=sys.stderr, flush=True)
        cur.execute(sGetVersion)
        db_version = cur.fetchone()
        print("initConnections({0}): ok, connected to DB version: {1}".format(p_name, db_version), file=sys.stderr, flush=True)
        logPrint("initConnections({0}): connected".format(p_name, db_version), p_logFile)
        cur.close()
    except (Exception) as error:
        logPrint("initConnections({0}): error [{1}]".format(p_name, error), p_logFile)
        sys.exit(1)

    return nc

def loadQueries(p_filename):
    global g_queries
    try:
        g_queriesRaw=pd.read_csv(p_filename,delimiter='\t')
        g_queriesRaw.index=g_queriesRaw.index+1
        g_queries = g_queriesRaw[ g_queriesRaw.source.str.contains("^[A-Z,a-z,0-9]") ].reset_index()
    except (Exception) as error:
        logPrint("error Loading [{0}]: [{1}]".format(p_filename, error))
        sys.exit(1)

def preCheck():
    global g_connections
    global g_queries

    logPrint("checking sources and destinations...")
    for ecol in expected_query_columns:
        if ecol not in g_queries:
            logPrint("Missing column on queries file: [{0}]".format(ecol))
            sys.exit(2)

    for i in range(0,len(g_queries)):
        source=g_queries["source"][i]
        if source=="" or source[0]=="#":
            continue
        dest=g_queries["dest"][i]
        mode=g_queries["mode"][i]
        query=g_queries["query"][i]
        table=g_queries["table"][i]

        if source not in g_connections:
            logPrint("ERROR: data source [{0}] not declared on connections.csv. giving up.".format(source))
            sys.exit(2)
        if dest not in g_connections:
            logPrint("ERROR: data destination [{0}] not declared on connections.csv. giving up.".format(dest))
            sys.exit(2)

        if query[0]=='@':
            if not os.path.isfile(query[1:]):
                logPrint("ERROR: query file [{0}] does not exist! giving up.".format(query[1:]))
                sys.exit(2)

        if "regexes" in g_queries:
            regex=g_queries["regexes"][i]
            if regex[0]=='@':
                if not os.path.isfile(regex[1:]):
                    logPrint("ERROR: regex file [{0}] does not exist! giving up.".format(regex[1:]))
                    sys.exit(2)


def readData(p_index, p_connection, p_cursor, p_fetchSize, p_query, p_closeStream, p_logFile):
    global g_Working
    global g_dataBuffer
    global g_readRecords
    global g_ErrorOccurred
    global g_nbrParallelWriters

    if g_testQueries:
        logPrint("readData({0}): test queries only mode, exiting".format(p_index), p_logFile)
        g_dataBuffer.put(False)
        g_readRecords.put((False,float(0)))
        return

    print("\nreadData({0}): Started".format(p_index), file=sys.stderr, flush=True)
    if p_query:
        try:
            p_cursor.execute(p_query)
        except (Exception) as error:
            logPrint("ReadData({0}): DB Error: [{1}]".format(p_index, error), p_logFile)
            g_ErrorOccurred=True

    while g_Working and not g_ErrorOccurred:
        try:
            rStart=timer()
            bData = p_cursor.fetchmany(p_fetchSize)
        except (Exception) as error:
            logPrint("ReadData({0}): DB Error: [{1}]".format(p_index, error), p_logFile)
            g_ErrorOccurred=True
            break
        if not bData:
            break
        if p_cursor.rowcount>0:
            iRowCount=p_cursor.rowcount
        else:
            iRowCount=len(bData)
        if iRowCount>0:
            g_readRecords.put( (p_index, iRowCount, (timer()-rStart)) )

        g_dataBuffer.put(bData, block=True)
    if p_closeStream or g_ErrorOccurred:
        print("\nreadData({0}): signaling write threads of the end of data.".format(p_index), file=sys.stderr, flush=True)
        for x in range(g_nbrParallelWriters):
            g_dataBuffer.put(False)
    else:
        print("\nreadData({0}): end of data, but keeeping the stream open".format(p_index), file=sys.stderr, flush=True)

    try:
        p_cursor.close()
    except:
        None
    try:
        p_connection.close()
    except:
        None

    g_readRecords.put((p_index, False,float(0)))
    print("\nreadData({0}): Ended".format(p_index), file=sys.stderr, flush=True)

def writeData(p_index, p_connection, p_cursor, p_iQuery, p_logFile, p_thread):
    global g_Working
    global g_dataBuffer
    global g_writtenRecords
    global g_ErrorOccurred

    print("\nwriteData({0}:{1}): Started".format(p_index, p_thread), file=sys.stderr, flush=True)
    while g_Working and not g_ErrorOccurred:
        try:
            bData = g_dataBuffer.get(block=True,timeout=100)
        except queue.Empty:
            continue
        if not bData:
            print ("\nwriteData({0}:{1}): 'no more data' message received".format(p_index, p_thread), file=sys.stderr, flush=True)
            break
        iStart=timer()
        try:
            iResult  = p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except (Exception) as error:
            logPrint("writeData({0}:{1}): DB Error: [{2}]".format(p_index, p_thread, error), p_logFile)
            if not p_connection.closed:
                p_connection.rollback()
            g_ErrorOccurred=True
            break
        g_writtenRecords.put( (p_cursor.rowcount, (timer()-iStart)) )
    try:
        p_cursor.close()
    except:
        None
    try:
        p_connection.close()
    except:
        None
    g_writtenRecords.put( (False, p_thread ) )
    print("\nwriteData({0}:{1}): Ended".format(p_index, p_thread), file=sys.stderr, flush=True)

def prepQuery(p_index):
    global g_queries
    global g_defaultFetchSize
    global g_testQueries
    global g_ReuseWriters

    bCloseStream=None

    qIndex=g_queries["index"][p_index]
    source=g_queries["source"][p_index]
    dest=g_queries["dest"][p_index]
    mode=g_queries["mode"][p_index]
    query=g_queries["query"][p_index]
    if query[0]=='@':
        with open(query[1:], 'r') as file:
            query = file.read()

    if "regexes" in g_queries:
        regexes=g_queries["regexes"][p_index]
        if regexes[0]=='@':
            with open(regexes[1:], 'r') as file:
                regexes=file.read().split('\n')
        else:
            if len(regexes)>0:
                regexes=[regexes.replace('/','\t')]
            else:
                regexes=None

        for regex in regexes:
            r=regex.split('\t')
            #result = re.sub(r"(\d.*?)\s(\d.*?)", r"\g<1> \g<2>", string1)
            if len(r)==2:
                query = re.sub( r[0], r[1], query )

    table=g_queries["table"][p_index]

    if "fetch_size" in g_queries:
        qFetchSize=int(g_queries["fetch_size"][p_index])
        if qFetchSize == 0:
            fetchSize=g_defaultFetchSize
        else:
            fetchSize=qFetchSize
    else:
        fetchSize=g_defaultFetchSize

    if "parallel_writers" in g_queries and not g_testQueries:
        qParallelWriters=int(g_queries["parallel_writers"][p_index])
        if qParallelWriters == 0:
            nbrParallelWriters=1
        else:
            nbrParallelWriters=qParallelWriters
    else:
        nbrParallelWriters=1

    if g_ReuseWriters:
        if p_index<len(g_queries)-1 and g_queries["dest"][p_index+1]==dest and g_queries["table"][p_index+1]==table:
                bCloseStream=False
        else:
            bCloseStream=True
    else:
        bCloseStream=True

    print("\nprepQuery({0}): source=[{1}], dest=[{2}], table=[{3}] closeStream=[{4}]".format(qIndex, source, dest, table, bCloseStream), file=sys.stderr, flush=True)
    return (source, dest, mode, query, table, fetchSize, nbrParallelWriters, bCloseStream)

def copyData():
    global g_Working
    global g_defaultFetchSize
    global g_ErrorOccurred
    global g_nbrParallelWriters
    global g_logFileName
    global g_readRecords
    global g_writtenRecords

    global g_connections
    global g_queries

    global g_readP
    global g_writeP

    global g_dataBuffer

    cPutConn = {}
    cPutData = {}
    cGetConn = {}
    cGetData = {}

    bCloseStream=True

    jobID=0
    while jobID <len(g_queries) and g_Working:
        prettyJobID=g_queries["index"][jobID]
        g_ErrorOccurred = False

        try:
            (source, dest, mode, query, table, fetchSize, nbrParallelWriters, bCloseStream) = prepQuery(jobID)
        except (Exception) as error:
            g_ErrorOccurred=True
            logPrint("copyData::OuterPrepQuery({0}): ERROR: [{1}]".format(prettyJobID, error))

        if g_logFileName=='':
            sLogFilePrefix="{0}.{1}".format(dest,table)
        else:
            sLogFilePrefix="{0}".format(g_logFileName)

        try:
            fLogFile=open("{0}.running.log".format(sLogFilePrefix),'a')
        except:
            g_ErrorOccurred=True
            logPrint("copyData::openLogFile({0}): ERROR: [{1}]".format(prettyJobID, error))
            sys.exit(1)

        g_nbrParallelWriters=nbrParallelWriters

        logPrint("copyData({0}): starting copy from [{1}] to [{2}].[{3}], with query:\n***\n{4}\n***".format(prettyJobID, source, dest, table,query),fLogFile)
        logPrint("copyData({0}): number of writers for this query: [{1}]".format(prettyJobID, g_nbrParallelWriters) ,fLogFile)

        try:
            g_ErrorOccurred=False
            if not g_testQueries:
                if mode.upper()=='T':
                    logPrint("copyData({0}): cleaning up table (truncate) [{1}].[{2}]".format(prettyJobID, dest,table) ,fLogFile)
                    cConn=initConnections(dest, False, 1, fLogFile)[0]
                    cCleanData = cConn.cursor()
                    cCleanData.execute("truncate table {0}".format(table))
                    cConn.commit()
                    cCleanData.close()
                    cConn.close()

                if mode.upper()=='D':
                    logPrint("copyData({0}): cleaning up table (delete) [{1}].[{2}]".format(prettyJobID, dest, table) ,fLogFile)
                    cConn=initConnections(dest, False, 1, fLogFile)[0]
                    cCleanData = cConn.cursor()
                    cCleanData.execute("delete from {0}".format(table))
                    cConn.commit()
                    cCleanData.close()
                    cConn.close()

            cGetConn[jobID] = initConnections(source, True, 1, fLogFile)[0]
            cGetData[jobID] = cGetConn[jobID].cursor()

            logPrint("copyData({0}): running source query...".format(prettyJobID), fLogFile)

            tStart=timer()
            cGetData[jobID].execute(query)
            logPrint("copyData({0}): source query took {1:.2f} seconds to reply.".format(prettyJobID, (timer() - tStart)), fLogFile)

            iCols = 0
            sColsPlaceholders = ''
            sColNames=''
            for col in cGetData[jobID].description:

                iCols = iCols+1
                sColNames = sColNames + '"{0}",'.format(col[0])
                sColsPlaceholders = sColsPlaceholders + "%s,"

            sColNames = sColNames[:-1]
            sColsPlaceholders = sColsPlaceholders[:-1]

            iQuery=''
            if "insert_cols" in g_queries:
                sOverrideCols=str(g_queries["insert_cols"][jobID])
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

            logPrint("copyData({0}): insert query (cols={1}): [{2}]".format(prettyJobID, sIcolType, iQuery) ,fLogFile)

            bFetchRead = True
            bFinishedRead = False

            logPrint("copyData({0}): entering insert loop...".format(prettyJobID),fLogFile)

            #clean up any garbage
            while True:
                try:
                    dummy=g_dataBuffer.get(block=True,timeout=1)
                except queue.Empty:
                    break

            g_readP[jobID]=mp.Process(target=readData, args=(prettyJobID, cGetConn[jobID], cGetData[jobID], fetchSize, None, bCloseStream, fLogFile))
            iTotalDataLinesRead = 0
            iTotalReadSecs=.001

            g_readP[jobID].start()

            runningWriters=0
            cPutConn= initConnections(dest, False, g_nbrParallelWriters, fLogFile)
            iTotalDataLinesWritten = 0
            iTotalWrittenSecs=.001
            for x in range(g_nbrParallelWriters):
                sleep(2)
                cPutData[x] = cPutConn[x].cursor()
                g_writeP[x]=(mp.Process(target=writeData, args=(prettyJobID, cPutConn[x], cPutData[x], iQuery, fLogFile, x+1)))
                g_writeP[x].start()
                runningWriters+=1

            bWait4Buffers=False
            while g_Working and not g_ErrorOccurred and runningWriters>0:
                try:
                    if bFetchRead:
                        bFetchRead=False
                        for x in range(10):
                            if not bWait4Buffers:
                                readerID,recRead,readSecs = g_readRecords.get(block=True,timeout=1)
                            if not recRead or bWait4Buffers:
                                if bCloseStream:
                                    bFinishedRead=True
                                else:
                                    if g_dataBuffer.qsize()>g_usedQueueBeforeNew:
                                        bWait4Buffers=True
                                        break
                                    else:
                                        bWait4Buffers=False

                                    if jobID<len(g_queries)-1:
                                        jobID+=1
                                        prettyJobID=g_queries["index"][jobID]
                                        try:
                                            (source, dest, mode, query, table, fetchSize, nbrParallelWriters, bCloseStream) = prepQuery(jobID)
                                        except (Exception) as error:
                                            g_ErrorOccurred=True
                                            logPrint("copyData::InnerPrepQuery({0}): ERROR: [{1}]".format(prettyJobID, error), fLogFile)
                                        cGetConn[jobID] = initConnections(source, True, 1, fLogFile)[0]
                                        cGetData[jobID] = cGetConn[jobID].cursor()
                                        g_readP[jobID]=mp.Process(target=readData, args=(prettyJobID, cGetConn[jobID], cGetData[jobID], fetchSize, query, bCloseStream, fLogFile))
                                        iTotalDataLinesRead = 0
                                        iTotalReadSecs=.001
                                        g_readP[jobID].start()
                                        break
                                    else:
                                        bFinishedRead=True
                            else:
                                iTotalDataLinesRead = recRead
                                iTotalReadSecs += readSecs
                            if not g_Working or g_ErrorOccurred:
                                break
                    else:
                        if not bFinishedRead:
                            bFetchRead=True
                        recWritten,writeSecs = g_writtenRecords.get(block=True, timeout=1)
                        if not recWritten:
                            runningWriters-=1

                        else:
                            iTotalDataLinesWritten += recWritten
                            iTotalWrittenSecs += writeSecs
                except queue.Empty:
                    None

                print("\r{0:,} records read ({1:.2f}/sec), {2:,} records written ({3:.2f}/sec), data queue len: {4}       ".format(iTotalDataLinesRead, (iTotalDataLinesRead/iTotalReadSecs), iTotalDataLinesWritten, (iTotalDataLinesWritten/iTotalWrittenSecs), g_dataBuffer.qsize()), file=sys.stdout, end='', flush=True)

            if g_ErrorOccurred:
                #clean up any remaining data
                while True:
                    try:
                        dummy=g_dataBuffer.get(block=True,timeout=1)
                    except queue.Empty:
                        break
                for x in range(g_nbrParallelWriters):
                    g_dataBuffer.put(False)

            print("\n", file=sys.stdout, flush=True)
            logPrint("copyData({0}): {1:,} rows copied in {2:.2f} seconds ({3:.2f}/sec).".format(prettyJobID, iTotalDataLinesWritten, (timer() - tStart), (iTotalDataLinesWritten/iTotalWrittenSecs)), fLogFile)
            for i in g_readP:
                g_readP[i].terminate()
            for i in g_writeP:
                g_writeP[i].terminate()

        except (Exception) as error:
            g_ErrorOccurred=True
            logPrint("copyData({0}): ERROR: [{1}]".format(prettyJobID, error), fLogFile)
        finally:
            fLogFile.close()
            if g_ErrorOccurred:
                sLogFileFinalName = "{0}.ERROR.log".format(sLogFilePrefix)
            else:
                sLogFileFinalName = "{0}.ok.log".format(sLogFilePrefix)
            if g_ErrorOccurred or mode==mode.upper():
                os.rename("{0}.running.log".format(sLogFilePrefix), sLogFileFinalName)
        jobID+=1

def sig_handler(signum, frame):
    global g_Working
    global g_ErrorOccurred
    global g_dataBuffer

    logPrint("\nsigHander: break received, signaling stop to threads...")
    g_Working = False
    g_ErrorOccurred = True
    while True:
        try:
            dummy=g_dataBuffer.get(block=True, timeout=1)
        except queue.Empty:
            break

# MAIN
def Main():
    global g_readP
    global g_writeP
    global g_fullstop
    global g_logFileName

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    if len(sys.argv) < 4:
        g_logFileName=os.getenv('LOG_FILE','')
    else:
        g_logFileName=sys.argv[3]

    if len(sys.argv) < 3:
        q_filename=os.getenv('JOB_FILE','jobs.csv')
    else:
        q_filename=sys.argv[2]

    if len(sys.argv) < 2:
        c_filename=os.getenv('CONNECTIONS_FILE','connections.csv')
    else:
        c_filename=sys.argv[1]

    loadConnections(c_filename)
    loadQueries(q_filename)

    preCheck()
    copyData()

if __name__ == '__main__':
    Main()