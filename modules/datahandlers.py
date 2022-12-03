'''readers and writers'''

#pylint: disable=invalid-name,broad-except

from timeit import default_timer as timer
import queue

import modules.shared as shared
import modules.logging as logging


def readData(p_jobID:int, p_jobName:str, p_connection, p_cursor, p_fetchSize:int, p_query:str):
    '''gets data from sources'''

    logging.logPrint("\nreadData({0}): Started, with cursor=[{1}]".format(p_jobName, id(p_cursor)), shared.L_DEBUG)
    tStart = timer()

    if p_query:
        try:
            p_cursor.execute(p_query)
            logging.statsPrint('execQuery', p_jobName, 0, (timer() - tStart), 0)
        except Exception as error:
            logging.logPrint("readData({0}): DB Error at execute: [{1}], query=[{2}], conn=[{3}]".format(p_jobName, error, p_query, p_connection))
            logging.statsPrint('execError', p_jobName, 0, (timer() - tStart), 0)
            shared.ErrorOccurred.value = True

    if not shared.ErrorOccurred.value:
        #first read outside the loop, to get the col description without penalising the loop with ifs
        try:
            rStart = timer()
            bData = p_cursor.fetchmany(p_fetchSize)
        except Exception as error:
            logging.logPrint("readData({0}): DB Error: [{1}]".format(p_jobName, error))
            logging.statsPrint('readError', p_jobName, 0, (timer() - tStart), 1)
            shared.ErrorOccurred.value = True

        if bData:
            shared.eventStream.put( (shared.E_READ_START, p_jobID, p_jobName, p_cursor.description, 0 ) )
            shared.eventStream.put( (shared.E_READ, p_jobID, p_jobName, len(bData), (timer()-rStart)) )

            shared.dataBuffer.put( (shared.seqnbr.value, shared.D_COD, bData), block = True )
            #logging.logPrint("pushed shared.seqnbr {0} (data)".format(shared.seqnbr.value), shared.L_DEBUG)
            shared.seqnbr.value += 1

    if not shared.testQueries:
        while shared.Working.value and not shared.ErrorOccurred.value:
            try:
                rStart = timer()
                bData = p_cursor.fetchmany(p_fetchSize)
            except Exception as error:
                logging.logPrint("readData({0}): DB Error: [{1}]".format(p_jobName, error))
                logging.statsPrint('readError', p_jobName, 0, (timer() - tStart), 2)
                shared.ErrorOccurred.value = True
                break
            if not bData:
                break

            shared.eventStream.put( (shared.E_READ, p_jobID, p_jobName, len(bData), (timer()-rStart)) )

            shared.dataBuffer.put( (shared.seqnbr.value, shared.D_COD, bData), block = True )
            #logging.logPrint("pushed shared.seqnbr {0} (data)".format(shared.seqnbr.value), shared.L_DEBUG)
            shared.seqnbr.value += 1
    else:
        logging.logPrint("\nreadData({0}): testing queries mode, stopping read.".format(p_jobName), shared.L_DEBUG)

    shared.eventStream.put( (shared.E_READ_END, p_jobID, p_jobName, None, None) )

    logging.logPrint("\nreadData({0}): Ended".format(p_jobName), shared.L_DEBUG)

def readData2(p_jobID:int, p_jobName:str, p_connection, p_connection2, p_cursor, p_cursor2, p_fetchSize:int, p_query:str, p_query2:str):
    '''gets data from sources, sublooping for keys'''

    logging.logPrint("\nreadData2({0}): Started, with cursor2=[{1}]".format(p_jobName, id(p_cursor2)), shared.L_DEBUG)

    tStart = timer()
    bColsNotSentYet = True

    if p_query:
        try:
            p_cursor.execute(p_query)
            logging.statsPrint('execQuery', p_jobName, 0, timer() - tStart, 0)
        except Exception as error:
            logging.logPrint("readData2({0}): DB Error at execute: [{1}], query=[{2}], conn=[{3}]".format(p_jobName, error, p_query, p_connection))
            logging.statsPrint('execError', p_jobName, 0, (timer() - tStart), 1)
            shared.ErrorOccurred.value = True

    while shared.Working.value and not shared.ErrorOccurred.value:
        try:
            rStart = timer()
            bData = p_cursor.fetchmany(p_fetchSize)
        except Exception as error:
            logging.logPrint("readData2({0}): DB Error: [{1}]".format(p_jobName, error))
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
                #logging.statsPrint('execQuery', p_jobName, 0, timer() - tStart, 0)
            except Exception as error:
                logging.logPrint("readData2({0}): DB Error at execute2: [{1}], query=[{2}], keys=[{3}], rowid=[{4}], conn=[{5}], conn2=[{6}]".format(p_jobName, error, p_query2, keys, rowid, p_connection, p_connection2))
                logging.statsPrint('execError', p_jobName, 0, (timer() - tStart), 2)
                shared.ErrorOccurred.value = True

            while shared.Working.value:
                try:
                    bData2 = p_cursor2.fetchmany(p_fetchSize)
                    if bColsNotSentYet:
                        shared.eventStream.put( (shared.E_READ_START, p_jobID, p_jobName, p_cursor2.description, 0 ) )
                        if shared.testQueries:
                            logging.logPrint("\nreadData({0}): testing queries mode, stopping read.".format(p_jobName), shared.L_DEBUG)
                            break
                        bColsNotSentYet = False
                except Exception as error:
                    logging.logPrint("readData2({0}): DB Error at fetch: [{1}]".format(p_jobName, error))
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
            #logging.logPrint("pushed shared.seqnbr {0} (data)".format(shared.seqnbr.value), shared.L_DEBUG)
            shared.seqnbr.value += 1

    shared.eventStream.put( (shared.E_READ_END, p_jobID, p_jobName, None, None) )

    logging.logPrint("\nreadData2({0}): Ended".format(p_jobName), shared.L_DEBUG)

def writeData(p_jobID:int, p_jobName:str, p_thread:int, p_connection, p_cursor, p_iQuery:str = ''):
    '''writes data to destinations'''

    seqnbr = -1
    FOD = 'X'

    logging.logPrint("\nwriteData({0}:{1}): Started".format(p_jobName, p_thread), shared.L_DEBUG)
    shared.eventStream.put( (shared.E_WRITE_START, p_jobID, p_jobName, None, None) )
    while shared.Working.value and not shared.ErrorOccurred.value:
        try:
            seqnbr, FOD, bData = shared.dataBuffer.get( block=True, timeout = 1 )
            #logging.logPrint("writer[{0}:{1}]: pulled shared.seqnbr {2}, queue size {3}".format(p_jobName, p_thread , seqnbr, shared.dataBuffer.qsize()), shared.L_DEBUG)
        except queue.Empty:
            continue
        if FOD == shared.D_EOD:
            logging.logPrint("\nwriteData({0}:{1}:{2}): 'no more data' message received".format(p_jobName, p_thread, seqnbr), shared.L_DEBUG)
            break
        iStart = timer()
        try:
            p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint("writeData({0}:{1}): DB Error: [{2}]".format(p_jobName, p_thread, error))
            logging.logPrint(bData, shared.L_DUMPDATA)
            logging.statsPrint('writeError', p_jobName, 0, (timer() - iStart), p_thread)
            if not p_connection.closed:
                p_connection.rollback()
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
    logging.logPrint("\nwriteData({0}:{1}): Ended".format(p_jobName, p_thread), shared.L_DEBUG)

def writeDataCSV(p_jobID:int, p_jobName:str, p_thread:int, p_stream, p_Header:str, p_encodeSpecial:bool = False):
    '''write data to csv file'''

    seqnbr = -1
    FOD = 'X'

    logging.logPrint("\nwriteDataCSV({0}:{1}): Started".format(p_jobName, p_thread), shared.L_DEBUG)
    shared.eventStream.put( (shared.E_WRITE_START, p_jobID, p_jobName, None, None) )
    if p_Header != '':
        p_stream.writerow(p_Header.split(','))

    while shared.Working.value and not shared.ErrorOccurred.value:
        try:
            seqnbr, FOD, bData = shared.dataBuffer.get( block=True, timeout = 1 )
            #logging.logPrint("writer[{0}:{1}]: pulled shared.seqnbr {2}, queue size {3}".format(p_jobName, p_thread , seqnbr, shared.dataBuffer.qsize()), shared.L_DEBUG)
        except queue.Empty:
            continue
        if FOD == shared.D_EOD:
            logging.logPrint("\nwriteDataCSV({0}:{1}:{2}): 'no more data' message received".format(p_jobName, p_thread, seqnbr), shared.L_DEBUG)
            break
        iStart = timer()
        try:
            if p_encodeSpecial:
                p_stream.writerows(shared.encodeSpecialChars(bData))
            else:
                p_stream.writerows(bData)
        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint("writeDataCSV({0}:{1}): Error: [{2}]".format(p_jobName, p_thread, error))
            logging.logPrint(bData, shared.L_DUMPDATA)
            logging.statsPrint('writeError', p_jobName, 0, (timer() - iStart), p_thread)
            break
        shared.eventStream.put( (shared.E_WRITE, p_jobID, p_jobName, len(bData), (timer()-iStart)) )

    shared.eventStream.put( (shared.E_WRITE_END, p_jobID, p_jobName, None, None) )
    logging.logPrint("\nwriteDataCSV({0}:{1}): Ended".format(p_jobName, p_thread), shared.L_DEBUG)
