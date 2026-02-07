'''readers and writers'''
import os
import traceback

from timeit import default_timer as timer

from typing import Optional

from multiprocessing import Queue
from queue import Empty as queueEmpty

from setproctitle import setproctitle

import modules.shared as shared
import modules.utils as utils
import modules.logging as logging
from modules.logging import logLevel as logLevel

def playNice():
    try:
        os.setpriority(os.PRIO_PROCESS, 0, shared.parallelProcessesNiceness)
    except:
        pass


def executeStatement(p_jobID:int, p_connection, p_cursor, p_target:str, p_query:str):
    '''executes commands on sources or destinations'''

    playNice()

    utils.block_signals()

    logging.logPrint(f'Started, with cursor=[{id(p_cursor)}], target=[{p_target}]', logLevel.DEBUG, p_jobID=p_jobID)
    tStart = timer()

    jobName = shared.getJobName(p_jobID)
    processTitlePrefix:str =f'datacopy: executeStatement[{p_target}]'

    try:
        setproctitle(f'{processTitlePrefix}(query) [{jobName}]')

        shared.eventQueue.put( (shared.E_CMD_START, p_jobID,  None, None) )

        p_cursor.execute(p_query)

    except Exception as e:
        setproctitle(f'{processTitlePrefix}(error@query) [{jobName}]')
        logging.processError(p_e=e, p_message=f'executing query, conn=[{p_connection}]', p_jobID=p_jobID, p_dontSendToStats=True)
        shared.eventQueue.put( (shared.E_CMD_ERROR, p_jobID, None, (timer() - tStart)) )

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

    shared.eventQueue.put( (shared.E_CMD_END, p_jobID,  None, (timer() - tStart)) )

    setproctitle(f'{processTitlePrefix}(finished) [{jobName}]')
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID)


def readData(p_jobID:int, p_connection, p_cursor, p_fetchSize:int, p_query:str, p_outQueue:Queue, p_finalDataReader:bool=True):
    '''gets data from sources'''

    playNice()

    utils.block_signals()

    logging.logPrint(f'Started, with cursor=[{id(p_cursor)}], fetchSize=[{p_fetchSize}], finalReader={p_finalDataReader}', logLevel.DEBUG, p_jobID=p_jobID)
    tStart = timer()

    jobName = shared.getJobName(p_jobID)
    processTitlePrefix:str =f'datacopy: readData{'' if p_finalDataReader else '[keys]'} '

    errorOccurred = False

    if p_query:
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

    setproctitle(f'{processTitlePrefix}(abort@cursor) [{jobName}]')
    try:
        p_cursor.abort()
    except Exception:
        pass

    setproctitle(f'{processTitlePrefix}(abort@connection) [{jobName}]')
    try:
        p_connection.abort()
    except Exception:
        pass

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

def readDataCSV(p_jobID:int, p_conn, p_fetchSize:int, p_outQueue:Queue, p_finalDataReader:bool=True, p_columns:Optional[list]=None):

    playNice()

    jobName = shared.getJobName(p_jobID)
    logging.logPrint(f'Started, columns requested=[{p_columns}], fetchSize=[{p_fetchSize}], finalReader={p_finalDataReader}', logLevel.DEBUG, p_jobID=p_jobID)

    f_file, f_stream = p_conn

    processTitlePrefix:str =f'datacopy: readDataCSV{'' if p_finalDataReader else '[keys]'} '
    setproctitle(f'{processTitlePrefix}[{jobName}::{f_file.name}]')

    tStart = timer()

    errorOccurred = False

    column_names:list[tuple[str]] = []
    column_indexes:list[int] = []
    filterData:bool = False

    i:int = 0
    raw_column_names:list[str] = []

    try:
        raw_column_names:list[str] = next(f_stream)

        if p_columns is not None:
            column_names:list[tuple[str]] = [(name,) for name in p_columns]
            column_indexes:list[int] = [raw_column_names.index(col) for col in p_columns if col in raw_column_names]
            filterData:bool = True
        else:
            column_names:list[tuple[str]] = [(name,) for name in raw_column_names]

        shared.eventQueue.put( (
            shared.E_READ_START if p_finalDataReader else shared.E_KEYS_READ_START,
            p_jobID, column_names, p_fetchSize )
        )


    except StopIteration as e:
        logging.processError(p_e=e, p_message='File does not have headers', p_jobID=p_jobID, p_stop=True)
        shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )
        errorOccurred = True

    except Exception as e:
        logging.processError(p_e=e, p_message='readingHeaders', p_stack=traceback.format_exc(), p_jobID=p_jobID, p_dontSendToStats=True)
        shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )
        errorOccurred = True

    if not errorOccurred:
        logging.logPrint(f'filterData: [{filterData}], raw_column_names: [{raw_column_names}], ', logLevel.DEBUG, p_jobID=p_jobID)

        data_packet:list[tuple] = []

        # duplicated code instead of doing an if for every row.
        # the only difference should be the row filtering atter the for loop
        if filterData:
            try:
                for i, row in enumerate(f_stream):
                    row = [row[idx] for idx in column_indexes]
                    data_packet.append(tuple(row))

                    if (i+1) % p_fetchSize == 0:
                        if shared.Working.value:
                            p_outQueue.put(data_packet, block=True)
                            data_packet:list[tuple] = []
                            shared.eventQueue.put( (shared.E_READ, p_jobID, p_fetchSize, (timer()-tStart)) )
                        else:
                            break
                remainingLines = len(data_packet)
                if remainingLines>0:
                    p_outQueue.put(data_packet, block=True)
                    shared.eventQueue.put( (shared.E_READ, p_jobID, remainingLines, (timer()-tStart)) )
            except Exception as e:
                errorOccurred = True
                setproctitle(f'{processTitlePrefix}(error@1) [{jobName}]')
                logging.processError(p_e=e, p_message='readingLoopfiltered', p_jobID=p_jobID, p_dontSendToStats=True)
                shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )
        else:
            try:
                for i, row in enumerate(f_stream):
                    data_packet.append(tuple(row))
                    # no filtering here
                    if (i+1) % p_fetchSize == 0:
                        if shared.Working.value:
                            p_outQueue.put(data_packet, block=True)
                            data_packet:list[tuple] = []
                            shared.eventQueue.put( (shared.E_READ, p_jobID, p_fetchSize, (timer()-tStart)) )
                        else:
                            break
                remainingLines = len(data_packet)
                if remainingLines>0:
                    p_outQueue.put(data_packet, block=True)
                    shared.eventQueue.put( (shared.E_READ, p_jobID, remainingLines, (timer()-tStart)) )
            except Exception as e:
                errorOccurred = True
                setproctitle(f'{processTitlePrefix}(error@1) [{jobName}]')
                logging.processError(p_e=e, p_message='readingLoopUnfiltered', p_jobID=p_jobID, p_dontSendToStats=True)
                shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )

        logging.logPrint(f'[{i+1}] rows read from CSV', logLevel.DEBUG, p_jobID=p_jobID)

        try:
            f_stream.close()
        except Exception:
            pass

        try:
            f_file.close()
        except Exception:
            pass

    shared.eventQueue.put( (
        shared.E_READ_END if p_finalDataReader else shared.E_KEYS_READ_END
        , p_jobID, None, None)
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

def writeDataCSV(p_jobID:int, p_threadID:int, p_conn, p_Header:str, p_encodeSpecial:bool = False):
    '''write data to csv file'''

    playNice()

    utils.block_signals()

    jobName = shared.getJobName(p_jobID)

    #p_conn is returned by initConnections as (file, stream) for CSVs
    f_file, f_stream = p_conn

    setproctitle(f'datacopy: writeDataCSV [{jobName}::{f_file.name}]')

    logging.logPrint('Started', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    shared.eventQueue.put( (shared.E_WRITE_START, p_jobID, None, None) )
    if len(p_Header) > 0:
        f_stream.writerow(p_Header.split(','))

    while shared.Working.value:
        try:
            bData = shared.dataQueue.get( block=True, timeout = 1 )
        except queueEmpty:
            if shared.stopWhenEmpty.value:
                logging.logPrint(f'end of data detected', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                break
            continue
        iStart = timer()
        try:
            if p_encodeSpecial:
                f_stream.writerows(utils.encodeSpecialChars(bData))
            else:
                f_stream.writerows(bData)
        except Exception as e:
            logging.logPrint(bData, logLevel.DUMP_DATA)
            logging.processError(p_e=e, p_dontSendToStats=True, p_jobID=p_jobID, p_threadID=p_threadID)
            shared.eventQueue.put( (shared.E_WRITE_ERROR, p_jobID, None, None) )

            try:
                f_file.flush()
                f_file.close()
            except Exception:
                pass
            break
        shared.eventQueue.put( (shared.E_WRITE, p_jobID, len(bData), (timer()-iStart)) )
    #make sure the stream is flushedß
    try:
        f_file.flush()
        f_file.close()
    except Exception as e:
        logging.processError(p_e=e, p_dontSendToStats=True, p_jobID=p_jobID, p_threadID=p_threadID)
        shared.eventQueue.put( (shared.E_WRITE_ERROR, p_jobID, None, None) )

    shared.eventQueue.put( (shared.E_WRITE_END, p_jobID, None, None) )
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
