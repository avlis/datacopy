'''data handlers for relational databases (DB-API)'''
from timeit import default_timer as timer
from multiprocessing import Queue
from queue import Empty as queueEmpty
from setproctitle import setproctitle

import modules.shared as shared
import modules.utils as utils
import modules.logging as logging
from modules.logging import logLevel as logLevel
from .common import playNice

def readData(p_jobID:int, p_connection, p_cursor, p_fetchSize:int, p_query:str, p_outQueue:Queue, p_finalDataReader:bool=True):
    '''gets data from sources'''

    playNice()

    utils.block_signals()

    logging.logPrint(f'Started, with cursor=[{id(p_cursor)}], fetchSize=[{p_fetchSize}], finalReader={p_finalDataReader}', logLevel.DEBUG, p_jobID=p_jobID)
    tStart = timer()

    jobName = shared.getJobName(p_jobID)
    processTitlePrefix:str =f'datacopy: readData{"" if p_finalDataReader else "[keys]"} '

    errorOccurred = False

    try:
        setproctitle(f'{processTitlePrefix}(query) [{jobName}]')

        shared.eventQueue.put( (
            shared.E_QUERY_START if p_finalDataReader else shared.E_KEYS_QUERY_START,
            p_jobID,  None, None)
        )

        p_cursor.execute(p_query)

        shared.eventQueue.put( (
            shared.E_QUERY_END if p_finalDataReader else shared.E_KEYS_QUERY_END,
            p_jobID,  None, (timer() - tStart))
        )
    except Exception as e:
        errorOccurred = True
        setproctitle(f'{processTitlePrefix}(error@query) [{jobName}]')
        logging.processError(p_e=e, p_message=f'executing query, conn=[{p_connection}]', p_jobID=p_jobID, p_dontSendToStats=True)
        shared.eventQueue.put( (shared.E_QUERY_ERROR, p_jobID, None, (timer() - tStart)) )

    setproctitle(f'{processTitlePrefix}(reading) [{jobName}]')
    if p_finalDataReader:
        if shared.Working.value and not errorOccurred:
            #first read outside the loop, to get the col description without penalising the main loop with ifs
            bData = False

            rStart = timer()
            try:
                bData = p_cursor.fetchmany(p_fetchSize)
            except Exception as e:
                errorOccurred = True
                setproctitle(f'{processTitlePrefix}(error@1) [{jobName}]')
                logging.processError(p_e=e, p_message='reading1', p_jobID=p_jobID, p_dontSendToStats=True)
                shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )

            if not errorOccurred:
                shared.eventQueue.put( (
                    shared.E_READ_START,
                    p_jobID, p_cursor.description, p_fetchSize )
                )
                if not shared.TEST_QUERIES:
                    p_outQueue.put( bData, block = True)
                    shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData), (timer()-rStart)) ) #type: ignore
    else:
        shared.eventQueue.put( (
            shared.E_KEYS_READ_START,
            p_jobID, p_cursor.description, p_fetchSize )
        )

    if not shared.TEST_QUERIES:
        while shared.Working.value and not errorOccurred:
            #don't use None here, some drivers mess with it
            bData = False
            try:
                rStart = timer()
                bData = p_cursor.fetchmany(p_fetchSize)
            except Exception as e:
                errorOccurred = True
                setproctitle(f'{processTitlePrefix}(error@2) [{jobName}]')
                logging.processError(p_e=e, p_message='readingLoop', p_dontSendToStats=True)
                shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )
                break
            if not bData:
                break

            shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData), (timer()-rStart)) )
            p_outQueue.put( bData, block = True )

        logging.logPrint('exited read loop.', logLevel.DEBUG, p_jobID=p_jobID)
    else:
        logging.logPrint('testing queries mode, stopping read.', logLevel.DEBUG, p_jobID=p_jobID)
        pass #do not remove as on production mode we comment the previous line

    setproctitle(f'{processTitlePrefix}(closing cursor) [{jobName}]')
    try:
        p_cursor.close()
    except Exception:
        pass

    setproctitle(f'{processTitlePrefix}(closing connection) [{jobName}]')
    try:
        p_connection.close()
    except Exception:
        pass

    shared.eventQueue.put( (
        shared.E_READ_END if p_finalDataReader else shared.E_KEYS_READ_END,
        p_jobID, None, None)
    )

    setproctitle(f'{processTitlePrefix}(flushing) [{jobName}]')
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID)

def readData2(p_jobID:int, p_threadID:int, p_connection2, p_cursor2, p_query2:str, p_fetchSize:int):
    '''gets data from sources, sublooping for keys'''

    playNice()

    utils.block_signals()

    logging.logPrint(f'Started, with cursor2=[{id(p_cursor2)}], fetchSize=[{p_fetchSize}]', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)

    jobName = shared.getJobName(p_jobID)

    bColsNotSentYet = True

    errorOccurred = False

    setproctitle(f'datacopy: readData2 (reading) [{jobName}]#{p_threadID}')
    while shared.Working.value and not errorOccurred:
        try:
            bData = shared.dataKeysQueue.get(timeout=1)
        except queueEmpty:
            if shared.stopWhenKeysEmpty.value:
                break
            continue

        logging.logPrint(f'[{len(bData)}] rows received from readData Level 1', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
        for keys in bData:
            if shared.Working.value == False or errorOccurred:
                break

            logging.logPrint(f'executing query 2 with keys=[{keys}]', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
            qStart = timer()
            try:
                shared.eventQueue.put( (shared.E_DETAIL_QUERY_START, p_jobID, None, None) )
                p_cursor2.execute(p_query2, keys)
                shared.eventQueue.put( (shared.E_DETAIL_QUERY_END, p_jobID, None, (timer() - qStart)) )
            except Exception as e:
                errorOccurred = True
                logging.processError(p_e=e, p_message=f'(execute2: keys=[{keys}], query2=[{p_query2}], conn2=[{p_connection2}]', p_jobID=p_jobID, p_threadID=p_threadID)
                shared.eventQueue.put( (shared.E_QUERY_ERROR, p_jobID, None, (timer() - qStart)) )

            while shared.Working.value and not errorOccurred:
                bData2 = False
                rStart = timer()
                try:
                    bData2 = p_cursor2.fetchmany(p_fetchSize)
                    if bColsNotSentYet:
                        shared.eventQueue.put( (shared.E_READ_START, p_jobID, p_cursor2.description, p_fetchSize ) )
                        if shared.TEST_QUERIES:
                            logging.logPrint('Testing queries mode, stopping read.', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                            break
                        bColsNotSentYet = False
                except Exception as e:
                    errorOccurred = True
                    logging.processError(p_e=e, p_jobID=p_jobID, p_dontSendToStats=True)
                    shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - rStart)) )
                    break

                if not bData2:
                    logging.logPrint(f'query 2 returned no rows', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                    break
                logging.logPrint(f'query 2 returned [{len(bData2)}] rows', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                if len(bData2) > 0:
                    shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData2), (timer()-rStart)) )
                    shared.dataQueue.put( bData2, block = True )
                else:
                    continue

            if shared.TEST_QUERIES:
                    break

    try:
        p_cursor2.close()
    except Exception:
        pass
    try:
        p_connection2.close()
    except Exception:
        pass

    shared.eventQueue.put( (shared.E_READ_END, p_jobID, p_threadID, None) )
    setproctitle(f'datacopy: readData2 (flushing) [{jobName}]#{p_threadID}')
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)

def writeData(p_jobID:int, p_threadID:int, p_connection, p_cursor, p_iQuery:str = ''):
    '''writes data to destinations'''

    playNice()

    utils.block_signals()

    jobName = shared.getJobName(p_jobID)

    setproctitle(f'datacopy: writeData [{jobName}]')

    logging.logPrint('Started', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    shared.eventQueue.put( (shared.E_WRITE_START, p_jobID, None, None) )
    while shared.Working.value:
        try:
            bData = shared.dataQueue.get( block=True, timeout = 1 )
        except queueEmpty:
            if shared.stopWhenEmpty.value:
                logging.logPrint('end of data detected', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                setproctitle(f'datacopy: writeData [{jobName}] stopping')
                break
            continue
        iStart = timer()
        try:
            p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except Exception as e:
            setproctitle(f'datacopy: writeData [{jobName}], error occurred')
            logging.logPrint(bData, logLevel.DUMP_DATA)
            logging.processError(p_e=e, p_dontSendToStats=True, p_jobID=p_jobID, p_threadID=p_threadID)
            shared.eventQueue.put( (shared.E_WRITE_ERROR, p_jobID, p_threadID, None ) )
            break

        #sometimes... things don't work as expected... like with pyodbc...
        wr = p_cursor.rowcount
        if wr == -1:
            wr = len(bData) # let's hope that all rows were writen...
        shared.eventQueue.put( (shared.E_WRITE, p_jobID, wr, (timer() - iStart)) )

    setproctitle(f'datacopy: writeData (rollback@cursor) [{jobName}]')
    try:
        p_cursor.rollback()
    except Exception:
        pass

    setproctitle(f'datacopy: writeData (rollback@connection) [{jobName}]')
    try:
        p_connection.rollback()
    except Exception:
        pass

    setproctitle(f'datacopy: writeData (closing cursor) [{jobName}]')
    try:
        p_cursor.close()
    except Exception:
        pass

    setproctitle(f'datacopy: writeData (closing connection) [{jobName}]')
    try:
        p_connection.close()
    except Exception:
        pass

    shared.eventQueue.put( (shared.E_WRITE_END, p_jobID, None, None) )
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    setproctitle(f'datacopy: writeData [{jobName}], ended')

def calcColumnsFromDestination(p_jobID:int, p_connection, p_table:str, p_objSep:str):
    '''retrieves column metadata from destination table'''
    tdCursor = p_connection.cursor()
    fetchColsFromDestSql=f'SELECT * FROM {p_objSep}{p_table}{p_objSep}  WHERE 1=0'
    logging.logPrint(f'retrieving cols for @d, executing [{fetchColsFromDestSql}]', logLevel.DEBUG, p_jobID=p_jobID)
    tdCursor.execute(fetchColsFromDestSql)
    workingCols = tdCursor.description
    
    if hasattr(p_connection, 'rollback'):
        try:
            p_connection.rollback() #somehow, this select blocks truncates on postgres, if not rolled back?...
        except Exception:
            pass
    
    tdCursor.close()
    return workingCols
def cleanDestinationTable(p_jobID:int, p_connection, p_table:str, p_mode:str, p_objSep:str):
    '''cleans up destination table (TRUNCATE or DELETE)'''
    cCleanData = p_connection.cursor()
    from timeit import default_timer as timer

    match p_mode.upper():
        case 'T':
            logging.logPrint(f'cleaning up table (truncate) [{p_table}]', p_jobID=p_jobID)
            cStart = timer()
            cleanDestSQL=f'truncate table {p_objSep}{p_table}{p_objSep}'
            try:
                logging.statsPrint('truncateStart', p_jobID, 0, 0, 0)
                cCleanData.execute(cleanDestSQL)
                p_connection.commit()
                logging.statsPrint('truncateEnd', p_jobID, 0, timer() - cStart, 0)
            except Exception as e:
                logging.statsPrint('truncateError', p_jobID, 0, timer() - cStart, 0)
                logging.processError(p_e=e, p_message=f'truncating table [{p_table}] with sql=[{cleanDestSQL}]', p_jobID=p_jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                return False
        case 'D':
            logging.logPrint(f'cleaning up table (delete) [{p_table}]', p_jobID=p_jobID)
            cStart = timer()
            deletedRows=-1
            cleanDestSQL=f'delete from {p_objSep}{p_table}{p_objSep}'
            try:
                logging.statsPrint('deleteStart', p_jobID, 0, 0, 0)
                deletedRows=cCleanData.execute(cleanDestSQL)
                p_connection.commit()
                logging.statsPrint('deleteEnd', p_jobID, deletedRows, timer() - cStart, 0)
            except Exception as e:
                logging.statsPrint('deleteError', p_jobID, 0, timer() - cStart, 0)
                logging.processError(p_e=e, p_message=f'deleting table: [{p_table}] with sql=[{cleanDestSQL}]', p_jobID=p_jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                return False
    
    cCleanData.close()
    return True
