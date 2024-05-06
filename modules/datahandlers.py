'''readers and writers'''

#pylint: disable=invalid-name, broad-except, line-too-long

from timeit import default_timer as timer
from time import sleep

import queue

import setproctitle

import modules.shared as shared
import modules.logging as logging

def readData(p_jobID:int, p_jobName:str, p_connection, p_cursor, p_fetchSize:int, p_query:str):
    '''gets data from sources'''

    logging.logPrint(f'\nreadData({p_jobName}): Started, with cursor=[{id(p_cursor)}]', shared.L_DEBUG)
    tStart = timer()

    if p_query:
        try:
            setproctitle.setproctitle(f'datacopy: readData(query) [{p_jobName}]')
            shared.eventStream.put( (shared.E_QUERY_START, p_jobID, p_jobName, 0, 0) )
            p_cursor.execute(p_query)
            shared.eventStream.put( (shared.E_QUERY_END, p_jobID, p_jobName, 0, (timer() - tStart)) )
        except Exception as error:
            logging.logPrint(f'readData({p_jobName}): DB Error at execute: [{error}], query=[{p_query}], conn=[{p_connection}]')
            logging.statsPrint('execError', p_jobName, 0, (timer() - tStart), 0)
            shared.ErrorOccurred.value = True

    setproctitle.setproctitle(f'datacopy: readData [{p_jobName}]')
    if not shared.ErrorOccurred.value:
        #first read outside the loop, to get the col description without penalising the loop with ifs
        bData = False
        try:
            rStart = timer()
            bData = p_cursor.fetchmany(p_fetchSize)
        except Exception as error:
            logging.logPrint(f'readData({p_jobName}): DB Error: [{error}]')
            logging.statsPrint('readError', p_jobName, 0, (timer() - tStart), 1)
            shared.ErrorOccurred.value = True

        if bData:
            shared.eventStream.put( (shared.E_READ_START, p_jobID, p_jobName, p_cursor.description, 0 ) )
            shared.eventStream.put( (shared.E_READ, p_jobID, p_jobName, len(bData), (timer()-rStart)) )

            shared.dataBuffer.put( (shared.seqnbr.value, shared.D_COD, bData), block = True )
            #logging.logPrint(f'pushed shared.seqnbr {shared.seqnbr.value} (data)', shared.L_DEBUG)
            shared.seqnbr.value += 1

    if not shared.testQueries:
        while shared.Working.value and not shared.ErrorOccurred.value:
            bData = False
            try:
                rStart = timer()
                bData = p_cursor.fetchmany(p_fetchSize)
            except Exception as error:
                logging.logPrint(f'readData({p_jobName}): DB Error: [{error}]')
                logging.statsPrint('readError', p_jobName, 0, (timer() - tStart), 2)
                shared.ErrorOccurred.value = True
                break
            if not bData:
                break

            shared.eventStream.put( (shared.E_READ, p_jobID, p_jobName, len(bData), (timer()-rStart)) )

            shared.dataBuffer.put( (shared.seqnbr.value, shared.D_COD, bData), block = True )
            #logging.logPrint(f'pushed shared.seqnbr {shared.seqnbr.value} (data)', shared.L_DEBUG)
            shared.seqnbr.value += 1
    else:
        logging.logPrint(f'\nreadData({p_jobName}): testing queries mode, stopping read.', shared.L_DEBUG)

    try:
        p_cursor.close()
    except Exception:
        pass
    try:
        p_connection.close()
    except Exception:
        pass


    if shared.parallelReaders > 1:
        #make sure that the next reader is started before we run the runningreaders to 0
        # and the innerloop exits and we close writers
        shared.eventStream.put( (shared.E_NOOP, p_jobID, p_jobName, None, None) )
        sleep(.15)

    shared.eventStream.put( (shared.E_READ_END, p_jobID, p_jobName, None, None) )
    logging.logPrint(f'\nreadData({p_jobName}): Ended', shared.L_DEBUG)

def readData2(p_jobID:int, p_jobName:str, p_connection, p_connection2, p_cursor, p_cursor2, p_fetchSize:int, p_query:str, p_query2:str):
    '''gets data from sources, sublooping for keys'''

    logging.logPrint(f'\nreadData2({p_jobName}): Started, with cursor2=[{id(p_cursor2)}]', shared.L_DEBUG)

    tStart = timer()
    bColsNotSentYet = True

    if p_query:
        try:
            setproctitle.setproctitle(f'datacopy: readData2(query) [{p_jobName}]')
            shared.eventStream.put( (shared.E_QUERY_START, p_jobID, p_jobName, 0, 0) )
            p_cursor.execute(p_query)
            shared.eventStream.put( (shared.E_QUERY_END, p_jobID, p_jobName, 0, (timer() - tStart)) )
        except Exception as error:
            logging.logPrint(f'readData2({p_jobName}): DB Error at execute: [{error}], query=[{p_query}], conn=[{p_connection}]')
            logging.statsPrint('execError', p_jobName, 0, (timer() - tStart), 1)
            shared.ErrorOccurred.value = True

    setproctitle.setproctitle(f'datacopy: readData2 [{p_jobName}]')
    while shared.Working.value and not shared.ErrorOccurred.value:
        bData = False
        try:
            rStart = timer()
            bData = p_cursor.fetchmany(p_fetchSize)
        except Exception as error:
            logging.logPrint(f'readData2({p_jobName}): DB Error: [{error}]')
            logging.statsPrint('readError', p_jobName, 0, (timer() - tStart), 3)
            shared.ErrorOccurred.value = True
            break
        if not bData:
            break

        new_bData = []

        for l in bData:

            keys=list(l[:-1])
            rowid=int(l[-1])

            try:
                p_cursor2.execute(p_query2, keys)
            except Exception as error:
                logging.logPrint(f'readData2({p_jobName}): DB Error at execute2: [{error}], query=[{p_query2}], keys=[{keys}], rowid=[{rowid}], conn=[{p_connection}], conn2=[{p_connection2}]')
                logging.statsPrint('execError', p_jobName, 0, (timer() - tStart), 2)
                shared.ErrorOccurred.value = True

            while shared.Working.value:
                bData2 = False
                try:
                    bData2 = p_cursor2.fetchmany(p_fetchSize)
                    if bColsNotSentYet:
                        shared.eventStream.put( (shared.E_READ_START, p_jobID, p_jobName, p_cursor2.description, 0 ) )
                        if shared.testQueries:
                            logging.logPrint(f'\nreadData({p_jobName}): testing queries mode, stopping read.', shared.L_DEBUG)
                            break
                        bColsNotSentYet = False
                except Exception as error:
                    logging.logPrint(f'readData2({p_jobName}): DB Error at fetch: [{error}]')
                    logging.statsPrint('readError', p_jobName, 0, (timer() - tStart), 4)
                    shared.ErrorOccurred.value = True
                    break
                if not bData2:
                    break

                for li in bData2:
                    nl=li+(rowid,)
                    new_bData=new_bData+[(nl),]
            else:
                continue
            break

        if shared.testQueries:
            break
        if len(new_bData) > 0:
            shared.eventStream.put( (shared.E_READ, p_jobID, p_jobName, len(new_bData), (timer()-rStart)) )
            shared.dataBuffer.put( (shared.seqnbr.value, shared.D_COD, new_bData), block = True )
            #logging.logPrint(f'pushed shared.seqnbr {shared.seqnbr.value} (data)', shared.L_DEBUG)
            shared.seqnbr.value += 1

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

    shared.eventStream.put( (shared.E_READ_END, p_jobID, p_jobName, None, None) )
    logging.logPrint(f'\nreadData2({p_jobName}): Ended', shared.L_DEBUG)

def writeData(p_jobID:int, p_jobName:str, p_thread:int, p_connection, p_cursor, p_iQuery:str = ''):
    '''writes data to destinations'''

    seqnbr = -1
    FOD = 'X'

    setproctitle.setproctitle(f'datacopy: writeData [{p_jobName}]')

    logging.logPrint(f'\nwriteData({p_jobName}:{p_thread}): Started', shared.L_DEBUG)
    shared.eventStream.put( (shared.E_WRITE_START, p_jobID, p_jobName, None, None) )
    while shared.Working.value and not shared.ErrorOccurred.value:
        try:
            seqnbr, FOD, bData = shared.dataBuffer.get( block=True, timeout = 1 )
            #logging.logPrint(f'writer[{p_jobName}:{p_thread}]: pulled shared.seqnbr {seqnbr}, queue size {shared.dataBuffer.qsize()}', shared.L_DEBUG)
        except queue.Empty:
            continue
        if FOD == shared.D_EOD:
            logging.logPrint(f"\nwriteData({p_jobName}:{p_thread}:{seqnbr}): 'no more data' message received", shared.L_DEBUG)
            break
        iStart = timer()
        try:
            p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint(f'writeData({p_jobName}:{p_thread}): DB Error: [{error}], packet seq [{seqnbr}]')
            logging.logPrint(bData, shared.L_DUMPDATA)
            logging.statsPrint('writeError', p_jobName, 0, (timer() - iStart), p_thread)
            try:
                p_connection.rollback()
            except Exception:
                pass
            break
        #sometimes... things don't work as expected... like with pyodbc...
        wr = p_cursor.rowcount
        if wr == -1:
            wr = len(bData) # let's hope that all rows were writen...
        shared.eventStream.put( (shared.E_WRITE, p_jobID, p_jobName, wr, (timer() - iStart)) )

    try:
        p_cursor.close()
    except Exception:
        pass
    try:
        p_connection.close()
    except Exception:
        pass

    shared.eventStream.put( (shared.E_WRITE_END, p_jobID, p_jobName, None, None) )
    logging.logPrint(f'\nwriteData({p_jobName}:{p_thread}): Ended', shared.L_DEBUG)

def writeDataCSV(p_jobID:int, p_jobName:str, p_thread:int, p_conn, p_Header:str, p_encodeSpecial:bool = False):
    '''write data to csv file'''

    setproctitle.setproctitle(f'datacopy: writeDataCSV [{p_jobName}]')

    seqnbr = -1
    FOD = 'X'

    #p_conn is returned by initConnections as (file, stream)
    f_file, f_stream = p_conn

    logging.logPrint(f'\nwriteDataCSV({p_jobName}:{p_thread}): Started', shared.L_DEBUG)
    shared.eventStream.put( (shared.E_WRITE_START, p_jobID, p_jobName, None, None) )
    if p_Header != '':
        f_stream.writerow(p_Header.split(','))

    while shared.Working.value and not shared.ErrorOccurred.value:
        try:
            seqnbr, FOD, bData = shared.dataBuffer.get( block=True, timeout = 1 )
            #logging.logPrint('writer[{0}:{1}]: pulled shared.seqnbr {2}, queue size {3}'.format(p_jobName, p_thread , seqnbr, shared.dataBuffer.qsize()), shared.L_DEBUG)
        except queue.Empty:
            continue
        if FOD == shared.D_EOD:
            logging.logPrint(f"\nwriteDataCSV({p_jobName}:{p_thread}:{seqnbr}): 'no more data' message received", shared.L_DEBUG)
            break
        iStart = timer()
        try:
            if p_encodeSpecial:
                f_stream.writerows(shared.encodeSpecialChars(bData))
            else:
                f_stream.writerows(bData)
        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint(f'writeDataCSV({p_jobName}:{p_thread}): Error: [{error}]')
            logging.logPrint(bData, shared.L_DUMPDATA)
            logging.statsPrint('writeError', p_jobName, 0, (timer() - iStart), p_thread)
            try:
                f_file.flush()
                f_file.close()
            except Exception:
                pass
            break
        shared.eventStream.put( (shared.E_WRITE, p_jobID, p_jobName, len(bData), (timer()-iStart)) )
    #make sure the stream is flushedß
    try:
        f_file.flush()
        f_file.close()
    except Exception as error:
        shared.ErrorOccurred.value = True
        logging.logPrint(f'writeDataCSV({p_jobName}:{p_thread}): Error: [{error}]')
        logging.statsPrint('writeError', p_jobName, 0, (timer() - iStart), p_thread)

    shared.eventStream.put( (shared.E_WRITE_END, p_jobID, p_jobName, None, None) )
    logging.logPrint(f'\nwriteDataCSV({p_jobName}:{p_thread}): Ended', shared.L_DEBUG)
