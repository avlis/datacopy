'''data handlers for CSV files'''
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
from .common import playNice

def readDataCSV(p_jobID:int, p_conn, p_fetchSize:int, p_outQueue:Queue, p_finalDataReader:bool=True, p_columns:Optional[list]=None):
    '''gets data from CSV files'''

    playNice()

    jobName = shared.getJobName(p_jobID)
    logging.logPrint(f'Started, columns requested=[{p_columns}], fetchSize=[{p_fetchSize}], finalReader={p_finalDataReader}', logLevel.DEBUG, p_jobID=p_jobID)

    f_file, f_stream = p_conn

    processTitlePrefix:str =f'datacopy: readDataCSV{"" if p_finalDataReader else "[keys]"} '
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
            # (name, type_code, display_size, internal_size, precision, scale, null_ok)
            column_names:list[tuple] = [(name, 'STRING', None, None, None, None, True) for name in p_columns]
            column_indexes:list[int] = [raw_column_names.index(col) for col in p_columns if col in raw_column_names]
            filterData:bool = True
        else:
            column_names:list[tuple] = [(name, 'STRING', None, None, None, None, True) for name in raw_column_names]

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
    #make sure the stream is flushed
    try:
        f_file.flush()
        f_file.close()
    except Exception as e:
        logging.processError(p_e=e, p_dontSendToStats=True, p_jobID=p_jobID, p_threadID=p_threadID)
        shared.eventQueue.put( (shared.E_WRITE_ERROR, p_jobID, None, None) )

    shared.eventQueue.put( (shared.E_WRITE_END, p_jobID, None, None) )
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    setproctitle(f'datacopy: writeData [{jobName}], ended')
