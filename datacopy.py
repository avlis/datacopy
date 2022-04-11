#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pylint: disable=broad-except,global-statement,invalid-name,import-outside-toplevel,line-too-long,too-many-lines,c-extension-no-member

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
    -- TEST_QUERIES (dry run, default no)
    -- QUEUE_SIZE (default 256)
    -- QUEUE_FB4NEWR (queue free before new read, when reuse_writers=yes, default 1/3 off queue)
    -- REUSE_WRITERS (default no)
    -- STOP_JOBS_ON_ERROR (default yes)
    -- DUMPFILE_SEP (default '|')
    -- STATS_IN_JSON (default no)

"""

import sys
import os
import signal
from timeit import default_timer as timer
from time import sleep
from datetime import datetime

import queue

import multiprocessing as mp

import re
import csv

import pandas as pd

mp.set_start_method('fork')

# Constants

E_READ = 1
E_WRITE = 2

L_INFO = 1
L_DEBUG = 2
L_STATS = 4
L_DUMPCOLS = 9
L_DUMPDATA = 10
L_OPEN = 16
L_CLOSE = 32
L_END = 255

D_COD = 'C'
D_EOD = '\x04'

csv_delimiter_decoder = {
    "tab":      '\t',
    "coma":     ',',
    "pipe":     '|',
    "colon":    ':',
    "hash":     '#'
}

csv_quoting_decoder = {
    "ALL":          csv.QUOTE_ALL,
    "MINIMAL":      csv.QUOTE_MINIMAL,
    "NONE":         csv.QUOTE_NONE,
    "NONNUMERIC":   csv.QUOTE_NONNUMERIC
}

# GLOBAL VARS

expected_conns_columns_db = ("name","driver","server","database","user","password")
expected_conns_columns_csv = ("name","driver","paths","delimiter","quoting")
expected_query_columns = ("source","dest","mode","query","table")

check_bd_version_cmd = {
    "pyodbc": "SELECT @@version",
    "cx_Oracle": "SELECT * FROM V$VERSION",
    "psycopg2":"SELECT version()",
    "mysql":"SELECT version()",
    "mariadb":"SELECT version()",
    "csv":"",
    "":""
}

g_connections = {}
g_queries = {}

g_defaultFetchSize:int = 1000

g_logFileName:str = ''
g_readP = {}
g_writeP = {}

queueSize = int(os.getenv('QUEUE_SIZE','256'))
g_usedQueueBeforeNew = int(queueSize/int(os.getenv('QUEUE_FB4NEWR','3')))

g_ReuseWriters:bool = bool(os.getenv('REUSE_WRITERS','no') == 'yes')

g_testQueries:bool = bool(os.getenv('TEST_QUERIES','no') == 'yes')

g_stopJobsOnError:bool = bool(os.getenv('STOP_JOBS_ON_ERROR','yes') == 'yes')

g_DEBUG:bool = bool(os.getenv('DEBUG','no') == 'yes')

g_DUMPFILE_SEP:str = os.getenv('DUMPFILE_SEP','|')

if os.getenv('STATS_IN_JSON','no') == 'yes':
    g_statsFormat = '{{"timestamp":"{0}","event":"{1}","jobid":"{2}","recs":{3},"secs":{4:.2f},"threads":{5}}}'
else:
    g_statsFormat = "{0}\t{1}\t{2}\t{3}\t{4:.2f}\t{5}"

#### SHARED OBJECTS

g_dataBuffer = mp.Manager().Queue(queueSize)
g_eventStream = mp.Manager().Queue()
g_logStream = mp.Manager().Queue()

g_seqnbr = mp.Value('i', 0)
g_Working = mp.Value('b', True)
g_ErrorOccurred =  mp.Value ('b',False)


def logPrint(p_ErrorMessage:str, p_logLevel:int=1):
    ''' sends message to the queue that manages logging'''
    global g_logStream

    global L_INFO
    global L_DEBUG

    if p_logLevel  in (L_INFO, L_DEBUG):
        sMsg = "{0}: {1}".format(str(datetime.now()), p_ErrorMessage)
    else:
        sMsg=p_ErrorMessage
    g_logStream.put((p_logLevel, sMsg))

def statsPrint(p_type:str, p_jobid:int, p_recs:int, p_secs:float, p_threads:int):
    '''sends stat messages to the log queue'''
    global L_STATS
    global g_statsFormat

    sMsg = g_statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), p_type, p_jobid, p_recs, p_secs, p_threads )
    g_logStream.put((L_STATS, sMsg))

def cx_Oracle_OutputTypeHandler(cursor, name, defaultType, size, precision, scale): # pylint: disable=unused-argument
    '''oracle custom stuff'''
    import cx_Oracle
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize = cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize = cursor.arraysize)

def loadConnections(p_filename:str):
    '''
    load connections file into memory
    '''
    global g_connections

    conns = {}
    try:
        c=pd.read_csv(p_filename, delimiter = '\t')
    except Exception as error:
        logPrint("error Loading [{0}]: [{1}]".format(p_filename, error))
        closeLogFile(1)

    for i in range(len(c)):
        cName = c["name"][i]
        if cName=="" or cName[0] == "#":
            continue
        if c["driver"][i] == "csv":
            for ecol in expected_conns_columns_csv:
                if ecol not in c:
                    logPrint("loadConnections[{0}]: Missing column on connections file: [{1}]".format(cName, ecol))
                    closeLogFile(1)
            nc = {"driver": c["driver"][i], "paths": c["paths"][i], "delimiter":c["delimiter"][i], "quoting":c["quoting"][i]}
        else:
            for ecol in expected_conns_columns_db:
                if ecol not in c:
                    logPrint("loadConnections[{0}]: Missing column on connections file: [{1}]".format(cName, ecol))
                    closeLogFile(1)
            nc = {"driver": c["driver"][i], "server": c["server"][i], "database":c["database"][i], "user":c["user"][i], "password":c["password"][i]}

        conns[cName] = nc

    g_connections = conns

def initConnections(p_name:str, p_readOnly:bool, p_qtd:int, p_preQuery:str = '', p_tableName = '', p_mode = 'w'):
    '''creates connection objects to sources or destinations'''
    global g_connections

    global L_DEBUG

    nc = {}

    if p_name in g_connections:
        c = g_connections[p_name]

    logPrint("initConnections[{0}]: trying to connect...".format(p_name), L_DEBUG)
    if c["driver"] == "pyodbc":
        try:
            import pyodbc
            for x in range(p_qtd):
                nc[x]=pyodbc.connect(driver="{ODBC Driver 18 for SQL Server}", server=c["server"], database=c["database"], user=c["user"], password=c["password"],encoding = "UTF-8", nencoding = "UTF-8", readOnly = p_readOnly )
        except (Exception, pyodbc.DatabaseError) as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error))
            g_ErrorOccurred.value=True
            closeLogFile(2)

    if c["driver"] == "cx_Oracle":
        try:
            import cx_Oracle
            for x in range(p_qtd):
                nc[x]=cx_Oracle.connect(c["user"], c["password"], "{0}/{1}".format(c["server"], c["database"]), encoding = "UTF-8", nencoding = "UTF-8" )
                nc[x].outputtypehandler = cx_Oracle_OutputTypeHandler
        except Exception as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error))
            g_ErrorOccurred.value=True
            closeLogFile(2)

    if c["driver"] == "psycopg2":
        try:
            from psycopg2 import pool as pgpools
            tpool = pgpools.ThreadedConnectionPool(1, p_qtd, host=c["server"], database=c["database"], user=c["user"], password = c["password"])
            for x in range(p_qtd):
                nc[x] = tpool.getconn()
                nc[x].readonly = p_readOnly
        except Exception as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error))
            g_ErrorOccurred.value=True
            closeLogFile(2)

    if c["driver"] == "mysql":
        try:
            import mysql.connector
            for x in range(p_qtd):
                nc[x]=mysql.connector.connect(host=c["server"], database=c["database"], user=c["user"], password = c["password"])
        except Exception as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error))
            g_ErrorOccurred.value=True
            closeLogFile(2)

    if c["driver"] == "mariadb":
        try:
            import mariadb
            for x in range(p_qtd):
                nc[x]=mariadb.connect(host=c["server"], database=c["database"], user=c["user"], password = c["password"])
        except Exception as error:
            logPrint("initConnections({0}): DB error [{1}]".format(p_name,error))
            g_ErrorOccurred.value=True
            closeLogFile(2)

    if c["driver"] == "csv":
        if "paths" in c:
            _paths=c["paths"].split('|')
        else:
            _paths=('.')

        for _path in _paths:
            if not os.path.isdir(_path):
                logPrint("initConnections({0}): directory does not exist [{1}]".format(p_name, _path))
                g_ErrorOccurred.value=True
                closeLogFile(2)

        sFileName = ''
        logPrint("initConnections({0}): dumping CSV files to {1}".format(p_name, _paths), L_DEBUG)
        try:
            _delim=csv_delimiter_decoder[c["delimiter"]]
        except Exception:
            _delim=","
        try:
            _quote=csv_quoting_decoder[c["quoting"]]
        except Exception:
            _quote=csv.QUOTE_MINIMAL

        csv.register_dialect(p_name, delimiter = _delim, quoting = _quote)
        logPrint("initConnections({0}): registering csv dialect with delim=[{1}], quoting=[{2}]".format(p_name, _delim, _quote), L_DEBUG)
        try:
            if p_qtd > 1:
                ipath=0
                for x in range(p_qtd):
                    sFileName = os.path.join(_paths[ipath], '{0}_{1}.csv'.format(p_tableName, x+1))
                    if ipath<len(_paths)-1:
                        ipath += 1
                    else:
                        ipath = 0
                    logPrint("initConnections({0}): opening file=[{1}], mode=[{2}]".format(p_name, sFileName, p_mode), L_DEBUG)
                    nc[x] = csv.writer(open(sFileName, p_mode), dialect = p_name)
            else:
                sFileName = os.path.join(_paths[0], '{0}.csv'.format(p_tableName))
                logPrint("initConnections({0}): opening file=[{1}], mode=[{2}]".format(p_name, sFileName, p_mode), L_DEBUG)
                nc[0] = csv.writer(open(sFileName, p_mode), dialect = p_name)
        except Exception as error:
            logPrint("initConnections({0}): CSV error [{1}] opening file [{2}]".format(p_name, error, sFileName))



    try:
        sGetVersion = check_bd_version_cmd[c["driver"]]
        if sGetVersion != '':
            cur = nc[0].cursor()
            logPrint("initConnections({0}): Testing connection, getting version with [{1}]...".format(p_name, sGetVersion), L_DEBUG)
            cur.execute(sGetVersion)
            db_version = cur.fetchone()
            logPrint("initConnections({0}): ok, connected to DB version: {1}".format(p_name, db_version), L_DEBUG)
            logPrint("initConnections({0}): connected".format(p_name))
            cur.close()
    except Exception as error:
        logPrint("initConnections({0}): error [{1}]".format(p_name, error))
        g_ErrorOccurred.value=True
        closeLogFile(2)

    if p_preQuery != '':
        for i in nc:
            pc = nc[i].cursor()
            try:
                logPrint("initConnections({0}): executing pre_query [{1}]".format(p_name, p_preQuery), L_DEBUG)
                pc.execute(p_preQuery)
            except Exception as error:
                logPrint("initConnections({0}): error executing pre_query [{1}] [{2}]".format(p_name, p_preQuery, error))
                g_ErrorOccurred.value=True
                closeLogFile(2)
            pc.close()

    return nc

def getConnectionParameter(p_name:str, p_otion:str):
    '''gets connection option'''
    global g_connections

    if p_name in g_connections:
        c = g_connections[p_name]
        if p_otion in c:
            return c[p_otion]
        else:
            return None
    else:
        return None

def initCursor(p_conn, p_jobID:int, p_fetchSize:int, p_UseServerSideCursors:bool):
    '''prepares the object that will send commands to databases'''
    # postgres: try not to fetch all rows to memory, using server side cursors

    global L_DEBUG

    try:
        if p_UseServerSideCursors:
            logPrint('trying to get server side cursor...', L_DEBUG)
            newCursor = p_conn.cursor(name='jobid-{0}'.format(p_jobID))
        else:
            logPrint('trying to get client side cursor...', L_DEBUG)
            newCursor = p_conn.cursor()
    except Exception as error:
        logPrint('server side cursor did not work, getting a normal cursor: [{0}]'.format(error), L_DEBUG)
        newCursor = p_conn.cursor()
    try:
        #only works on postgres...
        newCursor.itersize = p_fetchSize
    except Exception as error:
        logPrint('could not set itersize: [{0}]'.format(error), L_DEBUG)

    return newCursor

def loadQueries(p_filename:str):
    '''loads job file into memory'''
    global g_queries
    try:
        g_queriesRaw=pd.read_csv(p_filename, delimiter = '\t')
        g_queriesRaw.index = g_queriesRaw.index+1
        g_queries = g_queriesRaw[ g_queriesRaw.source.str.contains("^[A-Z,a-z,0-9]") ].reset_index()
    except Exception as error:
        logPrint("error Loading [{0}]: [{1}]".format(p_filename, error))
        closeLogFile(3)

def preCheck():
    '''checks that config files are consistent'''
    global g_connections
    global g_queries

    logPrint("checking sources and destinations...")
    for ecol in expected_query_columns:
        if ecol not in g_queries:
            logPrint("Missing column on queries file: [{0}]".format(ecol))
            closeLogFile(4)

    for i in range(0,len(g_queries)):
        source = g_queries["source"][i]
        if source=="" or source[0] == "#":
            continue
        dest = g_queries["dest"][i]
        query = g_queries["query"][i]

        if source not in g_connections:
            logPrint("ERROR: data source [{0}] not declared on connections.csv. giving up.".format(source))
            closeLogFile(4)

        if getConnectionParameter(source, "driver") == "csv":
            logPrint("ERROR: csv driver requested as source on [{0}], but it's only available as destination yet. giving up.".format(source))
            closeLogFile(4)

        if dest not in g_connections:
            logPrint("ERROR: data destination [{0}] not declared on connections.csv. giving up.".format(dest))
            closeLogFile(4)

        if query[0] == '@':
            if not os.path.isfile(query[1:]):
                logPrint("ERROR: query file [{0}] does not exist! giving up.".format(query[1:]))
                closeLogFile(4)

        if "pre_query_src" in g_queries:
            preQuerySrc = g_queries["pre_query_src"][i]
            if preQuerySrc[0] == '@':
                if not os.path.isfile(preQuerySrc[1:]):
                    logPrint("ERROR: pre query file [{0}] does not exist! giving up.".format(preQuerySrc[1:]))
                    closeLogFile(4)

        if "pre_query_dst" in g_queries:
            preQueryDst = g_queries["pre_query_dst"][i]
            if preQueryDst[0] == '@':
                if not os.path.isfile(preQueryDst[1:]):
                    logPrint("ERROR: pre query file [{0}] does not exist! giving up.".format(preQueryDst[1:]))
                    closeLogFile(4)

        if "regexes" in g_queries:
            regex = g_queries["regexes"][i]
            if regex[0] == '@':
                if not os.path.isfile(regex[1:]):
                    logPrint("ERROR: regex file [{0}] does not exist! giving up.".format(regex[1:]))
                    closeLogFile(4)


def openLogFile(p_dest:str, p_table:str):
    '''setups the log file'''
    global g_logFileName
    global g_logStream

    global L_OPEN

    sLogFilePrefix = ''
    if g_logFileName == '':
        sLogFilePrefix = "{0}.{1}".format(p_dest, p_table)
    else:
        sLogFilePrefix = "{0}".format(g_logFileName)

    g_logStream.put( (L_OPEN, sLogFilePrefix) )


def closeLogFile(p_exitCode = None):
    '''makes sure the log file is properly handled.'''
    global g_logFileName

    global L_CLOSE
    global L_END

    #give some time to other threads to say whatever they need to say to logs...
    if p_exitCode is None:
        sleep(3)

    g_logStream.put( (L_CLOSE, '') )

    g_logStream.put( (L_END, mp.current_process().name) )
    loopTimeout = 3
    while g_logStream.qsize() > 0 and loopTimeout >0:
        sleep(1)
        loopTimeout -= 1

    if p_exitCode is not None:
        sys.exit(p_exitCode)

def encodeSpecialChars(p_in):
    '''convert special chars to escaped representation'''
    buff=[]
    for line in p_in:
        newLine=[]
        for col in line:
            if isinstance(col, str):
                newData=[]
                for b in col:
                    i=ord(b)
                    if i<32:
                        newData.append(r'\{0}'.format(hex(i)))
                    else:
                        newData.append(b)
                newLine.append(''.join(newData))
            else:
                newLine.append(col)
        buff.append(tuple(newLine))
    return tuple(buff)


def writeLogFile():
    '''processes messages on the log queue and sends them to file, stdout, stderr acordingly'''

    # WARNING: Log writing failures does not stop processing!

    global g_logStream
    global g_DEBUG
    global g_ErrorOccurred
    global g_DUMPFILE_SEP
    global g_statsFormat

    global L_INFO
    global L_DEBUG
    global L_STATS
    global L_DUMPCOLS
    global L_DUMPDATA
    global L_OPEN
    global L_CLOSE
    global L_END

    #ignore control-c on this thread
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    logFile = None
    statsFile = None
    dumpColNames = None
    dumpFile = None
    sLogFilePrefix = ''

    bKeepGoing=True
    while bKeepGoing:
        try:
            (logLevel, sMsg) = g_logStream.get( block=True, timeout = 1 )
        except queue.Empty:
            continue
        except Exception:
            continue

        #print("logwriter: received message [{0}][{1}]".format(logLevel, sMsg), file=sys.stderr, flush=True)
        if logLevel == L_INFO:
            print(sMsg, file=sys.stdout, flush=True)
            if logFile:
                try:
                    print(sMsg, file=logFile, flush=True)
                except Exception:
                    pass
            continue

        if logLevel == L_DEBUG and g_DEBUG:
            print(sMsg, file=sys.stderr, flush=True)
            continue

        if logLevel == L_STATS:
            if logFile:
                try:
                    print(sMsg, file=statsFile, flush=True)
                except Exception:
                    pass
            continue

        if logLevel == L_DUMPCOLS:
            dumpColNames = sMsg
            continue

        if logLevel == L_DUMPDATA:
            if g_DEBUG:
                try:
                    dumpFile = open( "{0}.DUMP".format(sLogFilePrefix), 'w')
                    dumper=csv.writer(dumpFile, delimiter = g_DUMPFILE_SEP, quoting = csv.QUOTE_MINIMAL)
                    dumper.writerow(dumpColNames)
                    dumper.writerows(encodeSpecialChars(sMsg))
                    dumpFile.close()
                except Exception:
                    pass
            continue

        if logLevel == L_OPEN:
            rStart = timer()
            try:
                print("writeLogFile: opening [{0}]".format(sMsg), file=sys.stderr, flush=True)
                sLogFilePrefix = sMsg
                logFile = open( "{0}.running.log".format(sMsg), 'a')
            except Exception as error:
                print('could not open log file [{0}]: [{1}]'.format(sMsg, error), file=sys.stderr, flush=True)
            try:
                statsFile = open( "{0}.stats".format(sMsg), 'a')
                print(g_statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), 'jobStart', 0, 0, 0, 0 ), file=statsFile, flush=True)
            except Exception as error:
                print('could not open stats file [{0}]: [{1}]'.format(sMsg, error), file=sys.stderr, flush=True)
            continue

        if logLevel == L_CLOSE:
            if logFile:
                print(g_statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), 'totalTime', 0, 0, timer()-rStart, 0 ), file=statsFile, flush=True)
                logFile.close()
                logFile=None
                if g_ErrorOccurred.value:
                    sLogFileFinalName = "{0}.ERROR.log".format(sLogFilePrefix)
                else:
                    sLogFileFinalName = "{0}.ok.log".format(sLogFilePrefix)
                try:
                    os.rename("{0}.running.log".format(sLogFilePrefix), sLogFileFinalName)
                except Exception:
                    pass
            continue

        if logLevel == L_END:
            bKeepGoing = False

    print('writeLogFile exiting...', file=sys.stderr, flush=True)


def readData(p_index, p_connection, p_cursor, p_fetchSize, p_query, p_closeStream, p_nbrParallelWriters):
    '''gets data from sources'''
    global g_Working
    global g_ErrorOccurred

    global g_dataBuffer
    global g_eventStream

    global g_seqnbr

    global L_DEBUG

    if g_testQueries:
        logPrint("readData({0}): test queries only mode, exiting".format(p_index))
        g_dataBuffer.put( (g_seqnbr.value, D_EOD, None) )
        g_eventStream.put((E_READ,p_index, False,float(0)))
        return

    logPrint("\nreadData({0}): Started".format(p_index), L_DEBUG)
    if p_query:
        try:
            tStart = timer()
            p_cursor.execute(p_query)
            statsPrint('execQuery', p_index, 0, timer() - tStart, 0)
        except Exception as error:
            logPrint("ReadData({0}): DB Error: [{1}]".format(p_index, error))
            g_ErrorOccurred.value = True

    if not g_ErrorOccurred.value:
        while g_Working.value:
            try:
                rStart = timer()
                bData = p_cursor.fetchmany(p_fetchSize)
            except Exception as error:
                logPrint("ReadData({0}): DB Error: [{1}]".format(p_index, error))
                g_ErrorOccurred.value = True
                break
            if not bData:
                break

            # not using p_cursor.rowcount because it is not consistent across drivers...
            iRowCount = len(bData)
            if iRowCount>0:
                g_eventStream.put( (E_READ, p_index, iRowCount, (timer()-rStart)) )

            g_dataBuffer.put( (g_seqnbr.value, D_COD, bData), block = True )
            #logPrint("pushed g_seqnbr {0} (data)".format(g_seqnbr.value), L_DEBUG)
            g_seqnbr.value += 1

    if p_closeStream:
        logPrint("\nreadData({0}:{1}): signaling write threads of the end of data.".format(p_index,g_seqnbr.value), L_DEBUG)
        for x in range(p_nbrParallelWriters): # pylint: disable=unused-variable
            g_dataBuffer.put( (g_seqnbr.value, D_EOD, None), block = True )
            #logPrint("pushed g_seqnbr {0} (end)".format(g_seqnbr.value), L_DEBUG)
            g_seqnbr.value += 1
    else:
        logPrint("\nreadData({0}:{1}): end of data, but keeeping the stream open".format(p_index, g_seqnbr.value), L_DEBUG)

    g_eventStream.put((E_READ, p_index, False, float(0)))

    try:
        p_cursor.close()
    except Exception:
        pass
    try:
        p_connection.close()
    except Exception:
        pass

    logPrint("\nreadData({0}): Ended".format(p_index), L_DEBUG)

def writeData(p_index:int, p_thread:int, p_connection, p_cursor, p_iQuery:str = ''):
    '''writes data to destinations'''
    global g_Working
    global g_ErrorOccurred

    global g_dataBuffer
    global g_eventStream

    global L_DEBUG
    global L_DUMPDATA

    seqnbr = -1

    FOD = 'X'

    logPrint("\nwriteData({0}:{1}): Started".format(p_index, p_thread), L_DEBUG)
    while g_Working.value:
        try:
            seqnbr, FOD, bData = g_dataBuffer.get( block=True, timeout = 1 )
            #logPrint("writer[{0}:{1}]: pulled g_seqnbr {2}, queue size {3}".format(p_index, p_thread , seqnbr, g_dataBuffer.qsize()), L_DEBUG)
        except queue.Empty:
            continue
        if FOD == D_EOD:
            logPrint("\nwriteData({0}:{1}:{2}): 'no more data' message received".format(p_index, p_thread, seqnbr), L_DEBUG)
            break
        iStart = timer()
        try:
            p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except Exception as error:
            logPrint("writeData({0}:{1}): DB Error: [{2}]".format(p_index, p_thread, error))
            logPrint(bData, L_DUMPDATA)
            if not p_connection.closed:
                p_connection.rollback()
            g_ErrorOccurred.value = True
            break
        g_eventStream.put( (E_WRITE, p_index, p_cursor.rowcount, (timer()-iStart)) )
    try:
        p_cursor.close()
    except Exception:
        pass
    try:
        p_connection.close()
    except Exception:
        pass
    g_eventStream.put( (E_WRITE, p_index, False, p_thread ) )
    logPrint("\nwriteData({0}:{1}): Ended".format(p_index, p_thread), L_DEBUG)

def writeDataCSV(p_index:int, p_thread:int, p_stream, p_Header:str, p_encodeSpecial:bool = False):
    '''write data to csv file'''
    global g_Working
    global g_ErrorOccurred

    global g_dataBuffer
    global g_eventStream

    global L_DEBUG
    global L_DUMPDATA

    seqnbr = -1

    FOD = 'X'

    logPrint("\nwriteDataCSV({0}:{1}): Started".format(p_index, p_thread), L_DEBUG)
    if p_Header != '':
        p_stream.writerow(p_Header.split(','))

    while g_Working.value:
        try:
            seqnbr, FOD, bData = g_dataBuffer.get( block=True, timeout = 1 )
            #logPrint("writer[{0}:{1}]: pulled g_seqnbr {2}, queue size {3}".format(p_index, p_thread , seqnbr, g_dataBuffer.qsize()), L_DEBUG)
        except queue.Empty:
            continue
        if FOD == D_EOD:
            logPrint("\nwriteDataCSV({0}:{1}:{2}): 'no more data' message received".format(p_index, p_thread, seqnbr), L_DEBUG)
            break
        iStart = timer()
        try:
            if p_encodeSpecial:
                p_stream.writerows(encodeSpecialChars(bData))
            else:
                p_stream.writerows(bData)
        except Exception as error:
            logPrint("writeDataCSV({0}:{1}): Error: [{2}]".format(p_index, p_thread, error))
            logPrint(bData, L_DUMPDATA)
            g_ErrorOccurred.value = True
            break
        g_eventStream.put( (E_WRITE, p_index, len(bData), (timer()-iStart)) )

    g_eventStream.put( (E_WRITE, p_index, False, p_thread ) )
    logPrint("\nwriteDataCSV({0}:{1}): Ended".format(p_index, p_thread), L_DEBUG)

def prepQuery(p_index):
    '''prepares the job step'''
    global g_queries
    global g_defaultFetchSize
    global g_testQueries
    global g_ReuseWriters

    global L_DEBUG

    bCloseStream = None

    qIndex = g_queries["index"][p_index]
    source = g_queries["source"][p_index]
    dest = g_queries["dest"][p_index]
    mode = g_queries["mode"][p_index]
    query = g_queries["query"][p_index]
    preQuerySrc = ''
    preQueryDst = ''
    bCSVEncodeSpecial = False


    if query[0] == '@':
        with open(query[1:], 'r') as file:
            query = file.read()

    if "pre_query_src" in g_queries:
        preQuerySrc = g_queries["pre_query_src"][p_index]
        if preQuerySrc[0]  == '@':
            with open(preQuerySrc[1:], 'r') as file:
                preQuerySrc = file.read()

    if "pre_query_dst" in g_queries:
        preQueryDst = g_queries["pre_query_dst"][p_index]
        if preQueryDst[0]  == '@':
            with open(preQueryDst[1:], 'r') as file:
                preQueryDst = file.read()

    if "regexes" in g_queries:
        regexes = g_queries["regexes"][p_index]
        if regexes[0] == '@':
            with open(regexes[1:], 'r') as file:
                regexes = file.read().split('\n')
        else:
            if len(regexes)>0:
                regexes = [regexes.replace('/','\t')]
            else:
                regexes = None

        for regex in regexes:
            r = regex.split('\t')
            #result = re.sub(r"(\d.*?)\s(\d.*?)", r"\g<1> \g<2>", string1)
            if len(r) == 2:
                query = re.sub( r[0], r[1], query )
                preQuerySrc = re.sub( r[0], r[1], preQuerySrc )
                preQueryDst = re.sub( r[0], r[1], preQueryDst )

    table = g_queries["table"][p_index]

    if "fetch_size" in g_queries:
        qFetchSize = int(g_queries["fetch_size"][p_index])
        if qFetchSize == 0:
            fetchSize = g_defaultFetchSize
        else:
            fetchSize = qFetchSize
    else:
        fetchSize = g_defaultFetchSize

    if "parallel_writers" in g_queries and not g_testQueries:
        qParallelWriters = int(g_queries["parallel_writers"][p_index])
        if qParallelWriters == 0:
            nbrParallelWriters = 1
        else:
            nbrParallelWriters = qParallelWriters
    else:
        nbrParallelWriters = 1

    if "csv_encode_special" in g_queries:
        bCSVEncodeSpecial = bool(g_queries["csv_encode_special"][p_index] == 'yes')


    if g_ReuseWriters:
        if p_index<len(g_queries)-1 and g_queries["dest"][p_index+1]==dest and g_queries["table"][p_index+1] == table:
            bCloseStream = False
        else:
            bCloseStream = True
    else:
        bCloseStream = True

    logPrint("prepQuery({0}): source=[{1}], dest=[{2}], table=[{3}] closeStream=[{4}], CSVEncodeSpecial=[{5}]".format(qIndex, source, dest, table, bCloseStream, bCSVEncodeSpecial), L_DEBUG)
    return (source, dest, mode, preQuerySrc, preQueryDst, query, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial)

def copyData():
    ''' main job loop'''
    global g_Working
    global g_defaultFetchSize
    global g_ErrorOccurred
    global g_stopJobsOnError

    global g_logFileName

    global g_eventStream

    global g_connections
    global g_queries

    global g_readP
    global g_writeP

    global g_dataBuffer

    global L_DEBUG

    cPutConn = {}
    cPutData = {}
    cGetConn = {}
    cGetData = {}

    bCloseStream = True

    iWriters = 0

    jobID = 0

    sWriteFileMode = 'w'
    sCSVHeader = ''

    while jobID < len(g_queries) and g_Working.value:
        logPrint("entering jobID {0}".format(jobID), L_DEBUG)
        prettyJobID = g_queries["index"][jobID]

        try:
            (source, dest, mode, preQuerySrc, preQueryDst, query, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial) = prepQuery(jobID)
        except Exception as error:
            g_ErrorOccurred.value = True
            logPrint("copyData::OuterPrepQuery({0}): ERROR: [{1}]".format(prettyJobID, error))

        openLogFile(dest, table)

        try:
            cGetConn[jobID] = initConnections(source, True, 1, preQuerySrc)[0]

            sColNames = ''
            sColsPlaceholders = ''
            bUseServerSideCursors = False

            isSelect = re.search('(^|[ \t\n]+)SELECT[ \t\n]+', query.upper())
            if not isSelect:
                sSourceTableName = query
                bUseServerSideCursors = True

                logPrint("copyData({0}): prefetching table definition...".format(prettyJobID))
                tdCursor = cGetConn[jobID].cursor()
                tdCursor.execute("SELECT * FROM {0} WHERE 1=0".format(sSourceTableName))
                if "ignore_cols" in g_queries:
                    tIgnoreCols = (g_queries["ignore_cols"][jobID]).split(',')
                else:
                    tIgnoreCols = ()
                for col in tdCursor.description:
                    if col[0] not in tIgnoreCols:
                        sColNames = '{0}"{1}",'.format(sColNames,col[0])
                        sColsPlaceholders = sColsPlaceholders + "%s,"
                sColNames = sColNames[:-1]
                sColsPlaceholders = sColsPlaceholders[:-1]

                tdCursor.close()
                query="SELECT {0} FROM {1}".format(sColNames,sSourceTableName)

            logPrint("copyData({0}): running source query: [{1}]".format(prettyJobID, query))

            tStart = timer()
            cGetData[jobID] = initCursor(cGetConn[jobID], jobID, fetchSize, bUseServerSideCursors)

            cGetData[jobID].execute(query)
            logPrint("copyData({0}): source query took {1:.2f} seconds to reply.".format(prettyJobID, (timer() - tStart)))
            statsPrint('execQuery', prettyJobID, 0, timer() - tStart, 0)

            if sColNames == '':
                for col in cGetData[jobID].description:
                    sColNames = sColNames + '"{0}",'.format(col[0])
                    sColsPlaceholders = sColsPlaceholders + "%s,"

                sColNames = sColNames[:-1]
                sColsPlaceholders = sColsPlaceholders[:-1]

            iQuery = ''
            if "insert_cols" in g_queries:
                sOverrideCols = str(g_queries["insert_cols"][jobID])
                if sOverrideCols != '':
                    if sOverrideCols == '@':
                        iQuery = "INSERT INTO {0} VALUES ({1})".format(table,sColsPlaceholders)
                        sIcolType = "from destination"
                    elif sOverrideCols == '@l':
                        iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.lower(),sColsPlaceholders)
                        sIcolType = "from source, lowercase"
                    elif sOverrideCols == '@u':
                        iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.upper(),sColsPlaceholders)
                        sIcolType = "from source, upercase"
                    elif sOverrideCols != 'nan':
                        iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sOverrideCols,sColsPlaceholders)
                        sIcolType = "overridden"
            if iQuery == '':
                iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames,sColsPlaceholders)
                sIcolType = "from source"

            logPrint("copyData({0}): starting reading from [{1}] to [{2}].[{3}], with query:\n***\n{4}\n***".format(prettyJobID, source, dest, table, query))
            g_readP[jobID]=mp.Process(target=readData, args = (prettyJobID, cGetConn[jobID], cGetData[jobID], fetchSize, None, bCloseStream, nbrParallelWriters))
            iTotalDataLinesRead = 0
            iTotalReadSecs = .001

            g_readP[jobID].start()

            sColNamesNoQuotes = sColNames.replace('"','')
            logPrint(sColNamesNoQuotes.split(','), L_DUMPCOLS)

            if not g_testQueries:
                # cleaning up

                if getConnectionParameter(dest, 'driver') == 'csv':
                    if  mode.upper() in ('T','D'):
                        sWriteFileMode='w'
                        sCSVHeader = sColNamesNoQuotes
                        logPrint("copyData({0}): creating new CSV files, with cols [{1}]".format(prettyJobID, sCSVHeader))
                    else:
                        sWriteFileMode='a'
                        sCSVHeader = ''
                        logPrint("copyData({0}): appending to existing CSV files, assuming cols [{1}]".format(prettyJobID, sColNamesNoQuotes))
                else:
                    cConn = initConnections(dest, False, 1, '', table, 'r')[0]
                    cCleanData = cConn.cursor()
                    if mode.upper() == 'T':
                        logPrint("copyData({0}): cleaning up table (truncate) [{1}].[{2}]".format(prettyJobID, dest,table))
                        cCleanData.execute("truncate table {0}".format(table))
                    if mode.upper() == 'D':
                        logPrint("copyData({0}): cleaning up table (delete) [{1}].[{2}]".format(prettyJobID, dest, table))
                        cCleanData.execute("delete from {0}".format(table))
                    cConn.commit()
                    cCleanData.close()
                    cConn.close()

                    logPrint("copyData({0}): insert query (cols = {1}): [{2}]".format(prettyJobID, sIcolType, iQuery))


            logPrint("copyData({0}): number of writers for this job: [{1}]".format(prettyJobID, nbrParallelWriters))

            iRunningWriters = 0
            iTotalDataLinesWritten = 0
            iTotalWrittenSecs = .001
            newWriteConns = initConnections(dest, False, nbrParallelWriters, preQueryDst, table, sWriteFileMode)

            for x in range(nbrParallelWriters):
                cPutConn[iWriters] = newWriteConns[x]
                if newWriteConns[x].__class__.__name__ == 'writer':
                    cPutData[iWriters] = None
                    g_writeP[iWriters] = (mp.Process(target=writeDataCSV, args = (prettyJobID, x+1, cPutConn[iWriters], sCSVHeader, bCSVEncodeSpecial) ))
                    g_writeP[iWriters].start()
                else:
                    cPutData[iWriters] = cPutConn[iWriters].cursor()
                    g_writeP[iWriters] = (mp.Process(target=writeData, args = (prettyJobID, x+1, cPutConn[iWriters], cPutData[iWriters], iQuery) ))
                    g_writeP[iWriters].start()
                iWriters += 1
                iRunningWriters += 1

            bWait4Buffers = False
            bFinishedRead = False

            logPrint("copyData({0}): entering insert loop...".format(prettyJobID))

            while ( g_Working.value and (not g_ErrorOccurred.value) and iRunningWriters > 0 ) or g_eventStream.qsize()>0 :
                try:
                    eType, threadID, recs, secs = g_eventStream.get(block=True,timeout = 1) # pylint: disable=unused-variable
                    #logPrint("\nstreamevent: [{0},{1},{2},{3}]".format(eType,threadID, recs, secs), L_DEBUG)

                    if eType == E_READ:
                        if not recs:
                            logPrint("readData({0}): {1:,} rows read in {2:.2f} seconds ({3:.2f}/sec).".format(prettyJobID, iTotalDataLinesRead, iTotalReadSecs, (iTotalDataLinesRead/iTotalReadSecs)))
                            statsPrint('read', prettyJobID, iTotalDataLinesRead, iTotalReadSecs, 1)
                            if not bFinishedRead:
                                if not bCloseStream:
                                    bWait4Buffers = True
                                else:
                                    logPrint("no more jobs for reused writers, moving on", L_DEBUG)
                        else:
                            iTotalDataLinesRead += recs
                            iTotalReadSecs += secs
                    else: # E_WRITE
                        if not recs:
                            iRunningWriters -= 1
                        else:
                            iTotalDataLinesWritten += recs
                            iTotalWrittenSecs += secs

                    if bWait4Buffers:
                        if g_dataBuffer.qsize()<g_usedQueueBeforeNew:
                            logPrint("buffers free, moving to next query", L_DEBUG)
                            bWait4Buffers = False

                            if jobID<len(g_queries)-1:
                                jobID += 1
                                prettyJobID = g_queries["index"][jobID]
                                try:
                                    (source, dest, mode, preQuerySrc, preQueryDst, query, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial) = prepQuery(jobID)
                                except Exception as error:
                                    g_ErrorOccurred.value = True
                                    logPrint("copyData::InnerPrepQuery({0}): ERROR: [{1}]".format(prettyJobID, error))
                                cGetConn[jobID] = initConnections(source, True, 1, preQuerySrc)[0]
                                cGetData[jobID] = initCursor(cGetConn[jobID], jobID, fetchSize, True)
                                logPrint("copyData({0}): starting reading from [{1}] to [{2}].[{3}], with query:\n***\n{4}\n***".format(prettyJobID, source, dest, table,query))
                                g_readP[jobID]=mp.Process(target=readData, args = (prettyJobID, cGetConn[jobID], cGetData[jobID], fetchSize, query, bCloseStream, nbrParallelWriters))
                                iTotalDataLinesRead = 0
                                iTotalReadSecs = .001
                                g_readP[jobID].start()
                            else:
                                logPrint("no more jobs, moving on", L_DEBUG)
                                jobID += 1
                                bFinishedRead = True

                except queue.Empty:
                    pass

                print("\r{0:,} records read ({1:.2f}/sec), {2:,} records written ({3:.2f}/sec), data queue len: {4}       ".format(iTotalDataLinesRead, (iTotalDataLinesRead/iTotalReadSecs), iTotalDataLinesWritten, (iTotalDataLinesWritten/iTotalWrittenSecs), g_dataBuffer.qsize()), file=sys.stdout, end='', flush = True)

            if g_ErrorOccurred.value:
                #clean up any remaining data
                while True:
                    try:
                        dummy=g_dataBuffer.get(block = True, timeout = 1 )
                    except queue.Empty:
                        break
                for x in range(nbrParallelWriters):
                    g_dataBuffer.put( (-3, D_EOD, None) )

            print("\n", file=sys.stdout, flush = True)
            logPrint("copyData({0}): {1:,} rows copied in {2:.2f} seconds ({3:.2f}/sec).".format(prettyJobID, iTotalDataLinesWritten, iTotalWrittenSecs, (iTotalDataLinesWritten/iTotalWrittenSecs)))
            statsPrint('write', prettyJobID, iTotalDataLinesWritten, iTotalWrittenSecs, nbrParallelWriters)

        except Exception as error:
            g_ErrorOccurred.value = True
            logPrint("copyData({0}): ERROR: [{1}]".format(prettyJobID, error))
        finally:
            #if a control-c occurred, also rename file
            if mode == mode.upper() or not g_Working.value or (g_stopJobsOnError and g_ErrorOccurred.value):
                closeLogFile()

        if g_stopJobsOnError and g_ErrorOccurred.value:
            break
        else:
            g_ErrorOccurred.value = False

        jobID += 1

    for i in g_readP:
        g_readP[i].terminate()
    for i in g_writeP:
        g_writeP[i].terminate()

def sig_handler(signum, frame):
    '''handles signals'''
    global g_Working
    global g_ErrorOccurred

    p = mp.current_process()
    if p.name == "MainProcess":
        logPrint("sigHander: Error: break signal received ({0},{1}), signaling stop to threads...".format(signum, frame))
        g_ErrorOccurred.value = True
        g_Working.value = False

# MAIN
def Main():
    '''entry point'''

    global g_logFileName

    signal.signal(signal.SIGINT, sig_handler)

    if len(sys.argv) < 4:
        g_logFileName = os.getenv('LOG_FILE','')
    else:
        g_logFileName = sys.argv[3]

    if len(sys.argv) < 3:
        q_filename = os.getenv('JOB_FILE','jobs.csv')
    else:
        q_filename = sys.argv[2]

    if len(sys.argv) < 2:
        c_filename = os.getenv('CONNECTIONS_FILE','connections.csv')
    else:
        c_filename = sys.argv[1]

    logProcessor=mp.Process(target=writeLogFile)
    logProcessor.start()

    loadConnections(c_filename)
    loadQueries(q_filename)

    preCheck()
    copyData()
    print ("exited copydata!")
    if g_ErrorOccurred.value:
        closeLogFile(6)

    else:
        closeLogFile(0)

if __name__ == '__main__':
    Main()
