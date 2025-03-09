'''readers and writers'''

#pylint: disable=invalid-name, broad-except, line-too-long

from timeit import default_timer as timer
from time import sleep

import queue

import setproctitle

import modules.shared as shared
import modules.logging as logging
from modules.logging import logLevel as logLevel

def readData(p_jobID:int, p_connection, p_cursor, p_fetchSize:int, p_query:str):
    '''gets data from sources'''

    shared.block_signals()

    logging.logPrint(f'Started, with cursor=[{id(p_cursor)}]', logLevel.DEBUG, p_jobID=p_jobID)
    tStart = timer()

    jobName = logging.getJobName(p_jobID)

    errorOccurred = False

    if p_query:
        try:
            setproctitle.setproctitle(f'datacopy: readData (query) [{jobName}]')
            shared.eventQueue.put( (shared.E_QUERY_START, p_jobID,  None, None) )
            p_cursor.execute(p_query)
            shared.eventQueue.put( (shared.E_QUERY_END, p_jobID,  None, (timer() - tStart)) )
        except Exception as e:
            errorOccurred = True
            setproctitle.setproctitle(f'datacopy: readData (error@query) [{jobName}]')
            logging.processError(p_e=e, p_message=f'executing query, conn=[{p_connection}]', p_jobID=p_jobID, p_dontSendToStats=True)
            shared.eventQueue.put( (shared.E_QUERY_ERROR, p_jobID, None, (timer() - tStart)) )

    setproctitle.setproctitle(f'datacopy: readData (reading) [{jobName}]')
    if shared.Working.value and not errorOccurred:
        #first read outside the loop, to get the col description without penalising the loop with ifs
        bData = False
        rStart = timer()
        try:
            bData = p_cursor.fetchmany(p_fetchSize)
        except Exception as e:
            errorOccurred = True
            setproctitle.setproctitle(f'datacopy: readData (error@1) [{jobName}]')
            logging.processError(p_e=e, p_message='reading1', p_jobID=p_jobID, p_dontSendToStats=True)
            shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )

        if bData:
            shared.eventQueue.put( (shared.E_READ_START, p_jobID, p_cursor.description, None ) )
            shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData), (timer()-rStart)) )

            shared.dataQueue.put( bData, block = True)

    if not shared.TEST_QUERIES:
        while shared.Working.value and not errorOccurred:
            #don't use None here, some drivers mess with it
            bData = False
            try:
                rStart = timer()
                bData = p_cursor.fetchmany(p_fetchSize)
            except Exception as e:
                errorOccurred = True
                setproctitle.setproctitle(f'datacopy: readData (error@2) [{jobName}]')
                logging.processError(p_e=e, p_message='reading', p_dontSendToStats=True)
                shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )
                break
            if not bData:
                break

            shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData), (timer()-rStart)) )
            shared.dataQueue.put( bData, block = True )

        logging.logPrint('exited read loop.', logLevel.DEBUG, p_jobID=p_jobID)
    else:
        logging.logPrint('testing queries mode, stopping read.', logLevel.DEBUG, p_jobID=p_jobID)
        pass #do not remove as on production we delete the previous line

    setproctitle.setproctitle(f'datacopy: readData (abort@cursor) [{jobName}]')
    try:
        p_cursor.abort()
    except Exception:
        pass

    setproctitle.setproctitle(f'datacopy: readData (abort@connection) [{jobName}]')
    try:
        p_connection.abort()
    except Exception:
        pass

    setproctitle.setproctitle(f'datacopy: readData (closing cursor) [{jobName}]')
    try:
        p_cursor.close()
    except Exception:
        pass

    setproctitle.setproctitle(f'datacopy: readData (closing connection) [{jobName}]')
    try:
        p_connection.close()
    except Exception:
        pass

    if shared.parallelReaders > 1:
        #make sure that the next reader is started before we run the runningreaders to 0
        # and the innerloop exits and we close writers
        shared.eventQueue.put( (shared.E_NOOP, p_jobID, None, None) )
        sleep(.15)

    shared.eventQueue.put( (shared.E_READ_END, p_jobID, None, None) )
    setproctitle.setproctitle(f'datacopy: readData (flushing) [{jobName}]')
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID)

def readData2(p_jobID:int, p_connection, p_connection2, p_cursor, p_cursor2, p_fetchSize:int, p_query:str, p_query2:str):
    '''gets data from sources, sublooping for keys'''

    shared.block_signals()

    logging.logPrint(f'Started, with cursor2=[{id(p_cursor2)}]', logLevel.DEBUG, p_jobID=p_jobID)

    jobName = logging.getJobName(p_jobID)

    tStart = timer()
    bColsNotSentYet = True

    errorOccurred = False

    if p_query:
        try:
            setproctitle.setproctitle(f'datacopy: readData2 (query) [{jobName}]')
            shared.eventQueue.put( (shared.E_QUERY_START, p_jobID, None, None) )
            p_cursor.execute(p_query)
            shared.eventQueue.put( (shared.E_QUERY_END, p_jobID, None, (timer() - tStart)) )
        except Exception as e:
            errorOccurred = True
            setproctitle.setproctitle(f'datacopy: readData2 (error@query) [{jobName}]')
            logging.processError(p_e=e, p_message=f'executing query, conn=[{p_connection}]', p_jobID=p_jobID, p_dontSendToStats=True)
            shared.eventQueue.put( (shared.E_QUERY_ERROR, p_jobID, None, (timer() - tStart)) )

    setproctitle.setproctitle(f'datacopy: readData2 (reading) [{jobName}]')
    while shared.Working.value and not errorOccurred:
        bData = False
        try:
            rStart = timer()
            bData = p_cursor.fetchmany(p_fetchSize)
        except Exception as e:
            errorOccurred = True
            setproctitle.setproctitle(f'datacopy: readData2 (error@1) [{jobName}]')
            logging.processError(p_e=e, p_message='reading1', p_jobID=p_jobID, p_dontSendToStats=True)
            shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )
            break
        if not bData:
            break

        logging.logPrint(f'[{len(bData)}] rows retrieved from query 1', logLevel.DEBUG, p_jobID=p_jobID)
        for keys in bData:
            if shared.Working.value == False or errorOccurred:
                break

            logging.logPrint(f'executing query 2 with keys=[{keys}]', logLevel.DEBUG, p_jobID=p_jobID)

            try:
                p_cursor2.execute(p_query2, keys)
            except Exception as e:
                errorOccurred = True
                logging.processError(p_e=e, p_message=f'(execute2: query=[{p_query2}], keys=[{keys}], conn=[{p_connection}], conn2=[{p_connection2}]', p_jobID=p_jobID)
                shared.eventQueue.put( (shared.E_QUERY_ERROR, p_jobID, None, (timer() - tStart)) )

            while shared.Working.value and not errorOccurred:
                bData2 = False
                try:
                    bData2 = p_cursor2.fetchmany(p_fetchSize)
                    if bColsNotSentYet:
                        shared.eventQueue.put( (shared.E_READ_START, p_jobID, p_cursor2.description, None ) )
                        if shared.TEST_QUERIES:
                            logging.logPrint('Testing queries mode, stopping read.', logLevel.DEBUG, p_jobID=p_jobID)
                            break
                        bColsNotSentYet = False
                except Exception as e:
                    errorOccurred = True
                    logging.processError(p_e=e, p_message=f'reading', p_jobID=p_jobID, p_dontSendToStats=True)
                    shared.eventQueue.put( (shared.E_READ_ERROR, p_jobID, None, (timer() - tStart)) )

                    break
                if not bData2:
                    logging.logPrint(f'query 2 returned no rows', logLevel.DEBUG, p_jobID=p_jobID)
                    break
                logging.logPrint(f'query 2 returned [{len(bData2)}] rows', logLevel.DEBUG, p_jobID=p_jobID)
                if len(bData2) > 0:
                    shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData2), (timer()-rStart)) )
                    shared.dataQueue.put( bData2, block = True )
            else:
                continue

            if shared.TEST_QUERIES:
                    break



    try:
        p_cursor.close()
    except Exception:
        pass
    try:
        p_connection.close()
    except Exception:
        pass
    try:
        p_cursor2.close()
    except Exception:
        pass
    try:
        p_connection2.close()
    except Exception:
        pass

    shared.eventQueue.put( (shared.E_READ_END, p_jobID, None, None) )
    setproctitle.setproctitle(f'datacopy: readData2 (flushing) [{jobName}]')
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID)

def writeData(p_jobID:int, p_threadID:int, p_connection, p_cursor, p_iQuery:str = ''):
    '''writes data to destinations'''

    shared.block_signals()

    jobName = logging.getJobName(p_jobID)

    setproctitle.setproctitle(f'datacopy: writeData [{jobName}]')

    logging.logPrint('Started', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    shared.eventQueue.put( (shared.E_WRITE_START, p_jobID, None, None) )
    while shared.Working.value:
        try:
            bData = shared.dataQueue.get( block=True, timeout = 1 )
        except queue.Empty:
            if shared.stopWhenEmpty.value:
                logging.logPrint('end of data detected', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                setproctitle.setproctitle(f'datacopy: writeData [{jobName}], stopping')
                break
            continue
        iStart = timer()
        try:
            p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except Exception as e:
            setproctitle.setproctitle(f'datacopy: writeData [{jobName}], error occurred')
            logging.logPrint(bData, logLevel.DUMP_DATA)
            logging.processError(p_e=e, p_dontSendToStats=True, p_jobID=p_jobID, p_threadID=p_threadID)
            shared.eventQueue.put( (shared.E_WRITE_ERROR, p_jobID, p_threadID, None ) )
            break

        #sometimes... things don't work as expected... like with pyodbc...
        wr = p_cursor.rowcount
        if wr == -1:
            wr = len(bData) # let's hope that all rows were writen...
        shared.eventQueue.put( (shared.E_WRITE, p_jobID, wr, (timer() - iStart)) )

    setproctitle.setproctitle(f'datacopy: writeData (rollback@cursor) [{jobName}]')
    try:
        p_cursor.rollback()
    except Exception:
        pass

    setproctitle.setproctitle(f'datacopy: writeData (rollback@connection) [{jobName}]')
    try:
        p_connection.rollback()
    except Exception:
        pass

    setproctitle.setproctitle(f'datacopy: writeData (closing cursor) [{jobName}]')
    try:
        p_cursor.close()
    except Exception:
        pass

    setproctitle.setproctitle(f'datacopy: writeData (closing connection) [{jobName}]')
    try:
        p_connection.close()
    except Exception:
        pass

    shared.eventQueue.put( (shared.E_WRITE_END, p_jobID, None, None) )
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    setproctitle.setproctitle(f'datacopy: writeData [{jobName}], ended')

def writeDataCSV(p_jobID:int, p_threadID:int, p_conn, p_Header:str, p_encodeSpecial:bool = False):
    '''write data to csv file'''

    shared.block_signals()

    jobName = logging.getJobName(p_jobID)

    #p_conn is returned by initConnections as (file, stream) for CSVs
    f_file, f_stream = p_conn

    setproctitle.setproctitle(f'datacopy: writeDataCSV [{jobName}::{f_file.name}]')

    logging.logPrint('Started', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    shared.eventQueue.put( (shared.E_WRITE_START, p_jobID, None, None) )
    if len(p_Header) > 0:
        f_stream.writerow(p_Header.split(','))

    while shared.Working.value:
        try:
            bData = shared.dataQueue.get( block=True, timeout = 1 )
        except queue.Empty:
            if shared.stopWhenEmpty.value:
                logging.logPrint(f'end of data detected', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                break
            continue
        iStart = timer()
        try:
            if p_encodeSpecial:
                f_stream.writerows(shared.encodeSpecialChars(bData))
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
