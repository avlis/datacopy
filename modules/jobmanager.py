'''job sequencer control'''

import sys

import re
from timeit import default_timer as timer
import multiprocessing as mp

from queue import Empty as queueEmpty

from setproctitle import setproctitle
import traceback

import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.shared as shared
import modules.utils as utils
import modules.jobs as jobs
import modules.connections as connections
import modules.datahandlers.common as datahandlers_common
import modules.datahandlers.relational as datahandlers_relational
import modules.datahandlers.bigquery as datahandlers_bigquery
import modules.datahandlers.csv as datahandlers_csv
from modules.jobmanager_context import StreamContext
from modules.jobmanager_events.dispatcher import dispatch

def jobManager():
    ''' main jobs handling loop'''

    utils.block_signals()

    ctx = StreamContext()
    try:
        setproctitle(f'datacopy: jobManager thread')
        logging.logPrint(f'entering jobs loop, max readers allowed: [{shared.parallelReaders}]')

        #################################
        ### outer loop, for multi-stream (i.e., multiple destination tables) cases.
        #################################

        while ctx.jobID < len(shared.jobs)+1 and ctx.bKeepGoing:
            logging.logPrint(f'outer loop, jobID=[{ctx.jobID}]', logLevel.DEBUG, p_jobID=ctx.jobID)

            thisJob = jobs.Job(ctx.jobID)
            ctx.reset_job_state(thisJob)

            logging.statsPrint('streamStart', ctx.jobID, shared.parallelReaders, thisJob.nbrParallelWriters, 0)

            shared.idleSecsObserved.value = 0

            if not shared.TEST_QUERIES:
                # cleaning up destination before inserts
                if connections.getConnectionParameter(thisJob.dest, 'driver') == 'csv':
                    if thisJob.mode.upper() in ('T','D'):
                        ctx.sWriteFileMode = 'w'
                        logging.logPrint('creating new CSV file(s)', p_jobID=ctx.jobID)
                    else:
                        ctx.sWriteFileMode = 'a'
                        logging.logPrint('appending to existing CSV file(s)', p_jobID=ctx.jobID)
                else:
                    siObjSep = connections.getConnectionParameter(thisJob.dest, 'insert_object_delimiter')
                    match thisJob.mode.upper():
                        case 'T' | 'D':
                            newConns = connections.initConnections(thisJob.dest, False, 1, thisJob.table, 'w')
                            if newConns is not None:
                                cConn = newConns[0]
                                if thisJob.destDriver == 'bigquery':
                                    res = datahandlers_bigquery.cleanDestinationTable(ctx.jobID, cConn, thisJob.table, thisJob.mode, siObjSep)
                                else:
                                    res = datahandlers_relational.cleanDestinationTable(ctx.jobID, cConn, thisJob.table, thisJob.mode, siObjSep)
                                cConn.close()
                                if not res: break
                            else:
                                break
                        case 'A':
                            getMaxDest = thisJob.getMaxDest if len(thisJob.getMaxDest) > 0 else thisJob.dest
                            newConns = connections.initConnections(getMaxDest, False, 1, thisJob.table, 'w')
                            if newConns is not None:
                                cConn = newConns[0]
                                cGetMaxID = cConn.cursor()
                                logging.logPrint(f'figuring out max value for [{thisJob.appendKeyColumn}] on [{getMaxDest}] with [{thisJob.getMaxQuery}]', p_jobID=ctx.jobID)
                                cStart = timer()
                                try:
                                    logging.statsPrint('getMaxAtDestinationStart', ctx.jobID, 0, 0, 0)
                                    cGetMaxID.execute(thisJob.getMaxQuery)
                                    ctx.oMaxAlreadyInsertedData = cGetMaxID.fetchone()[0]
                                    logging.statsPrint('getMaxAtDestinationEnd', ctx.jobID, ctx.oMaxAlreadyInsertedData, timer() - cStart, 0)
                                    logging.logPrint(f'max value is [{ctx.oMaxAlreadyInsertedData}]', p_jobID=ctx.jobID)
                                except Exception as e:
                                    logging.statsPrint('getMaxAtDestinationError', ctx.jobID, 0, timer() - cStart, 0)
                                    logging.processError(p_e=e, p_message=f'getting max value:', p_jobID=ctx.jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                                cGetMaxID.close()
                                cConn.close()
                            else:
                                break
                                break

                            cGetMaxID.close()
                            cConn.close()

            logging.logPrint('entering stream loop...', p_jobID=ctx.jobID)
            shared.eventQueue.put( (shared.E_BOOT, ctx.jobID, None, None ) )

            #################################
            ### inner loop (for each job on to the same stream, ie, target table. event based.)
            #################################

            fStart = timer()
            bInnerLoop = True
            
            while bInnerLoop and ctx.bKeepGoing:
                try:
                    eType, eJobID, recs, secs = shared.eventQueue.get(timeout=1)
                    dispatch(eType, eJobID, recs, secs, ctx)
                    ctx.iIdleTimeout = 0
                    shared.idleSecsObserved.value = 0

                except queueEmpty:
                    if not ctx.bStopRequested:
                        ctx.iIdleTimeout += 1
                        shared.idleSecsObserved.value += 1

                        if shared.idleTimeoutSecs > 0 and ctx.iIdleTimeout > shared.idleTimeoutSecs:
                            logging.statsPrint('IdleTimeoutError', ctx.jobID, 0, shared.idleTimeoutSecs, 0)
                            logging.processError(p_message=f'idle timeout secs [{shared.idleTimeoutSecs}] reached.', p_dontSendToStats=True, p_stop=True, p_exitCode=5)

                            # force close cursors
                            allObjectsToClose={**shared.GetData, **shared.GetData2, **shared.PutData, **shared.GetConn, **shared.GetConn2, **shared.PutConn}
                            for k,v in allObjectsToClose.items():
                                try:
                                    v[k].close()
                                    v[k] = None
                                except:
                                    pass

                        if ( ctx.iIdleTimeout > 3 and ctx.iActiveJobsOnThisStream == 0 and ctx.iRunningWriters == 0 ):
                            bInnerLoop = False
                    else:
                        # handle stop request
                        while shared.dataKeysQueue.qsize() > 0:
                            try:
                                shared.dataKeysQueue.get(block = True, timeout = 1 )
                                ctx.dumpedPackets += 1
                            except queueEmpty:
                                break
                        while not ctx.bReadyToStop:
                            try:
                                shared.dataQueue.get(block = True, timeout = 1 )
                                ctx.dumpedPackets += 1
                            except queueEmpty:
                                ctx.emptyQueueTimeout -= 1
                                if ctx.emptyQueueTimeout == 0 or ctx.iRunningReaders == 0:
                                    ctx.bReadyToStop = True
                                break

                        if ctx.bReadyToStop:
                            if shared.ErrorOccurred.value:
                                logging.statsPrint('dumpDataOnError', ctx.jobID, ctx.dumpedPackets, 0, 0)
                                logging.logPrint(f'dumped {ctx.dumpedPackets} packets from dataQueue', logLevel.DEBUG, p_jobID=ctx.jobID)
                            ctx.bKeepGoing = False

                # common part of event processing:
                iCurrentQueueSize = shared.dataQueue.qsize()
                if iCurrentQueueSize > shared.maxQueueLenObserved:
                    shared.maxQueueLenObserved = iCurrentQueueSize
                if iCurrentQueueSize == shared.queueSize:
                    shared.maxQueueLenObservedEvents +=1

                if ctx.tParallelReadersNextCheck < timer():
                    ctx.tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval
                    if ctx.bKeepGoing and (not ctx.bEndOfJobs and not thisJob.bCloseStream and ctx.iActiveJobsOnThisStream < shared.parallelReaders and iCurrentQueueSize < shared.usedQueueBeforeNew):
                        if ctx.jobID < len(shared.jobs):
                            ctx.jobID += 1
                            shared.eventQueue.put( (shared.E_BOOT, ctx.jobID, None, None ) )
                        else:
                            ctx.jobID += 1
                            ctx.bEndOfJobs = True
                    else:
                        if ctx.iActiveJobsOnThisStream == 0 and thisJob.bCloseStream:
                            if shared.dataQueue.qsize() == 0 and shared.eventQueue.qsize() == 0 and shared.stopWhenEmpty.value == False:
                                with shared.stopWhenEmpty.get_lock():
                                    shared.stopWhenEmpty.value = True

                if shared.Working.value:
                    if shared.SCREEN_STATS:
                        if ctx.iRunningStatements > 0:
                            statsLine=f'\r executing statement of Job [{ctx.jobName}], timeout timer: {ctx.iIdleTimeout:,}, idle time: {shared.idleSecsObserved.value:,}        '
                        else:
                            statsLine=f'\r{ctx.iTotalDataLinesRead:,} recs read ({(ctx.iTotalDataLinesRead/ctx.fTotalReadSecs):,.2f}/sec, {ctx.iReadingReaders}r,{ctx.iRunningQueries}q), {ctx.iTotalDataLinesWritten:,} recs written ({(ctx.iTotalDataLinesWritten/ctx.fTotalWrittenSecs):,.2f}/sec, {ctx.iRunningWriters}w), queue len: {iCurrentQueueSize:,}, max queue: {shared.maxQueueLenObserved:,}, timeout timer: {ctx.iIdleTimeout:,}, idle time: {shared.idleSecsObserved.value:,}, activeJobs: {ctx.iActiveJobsOnThisStream}        '
                        if shared.SCREEN_STATS_TO_STDOUT:
                            print(statsLine, file=sys.stdout, end='', flush = True)
                        else:
                            print(statsLine, file=sys.stderr, end='', flush = True)
                    logging.logPrint(f'reads:{ctx.iTotalDataLinesRead:,} ({(ctx.iTotalDataLinesRead/ctx.fTotalReadSecs):,.2f}/s, {ctx.iReadingReaders}r,{ctx.iRunningQueries}q); writes:{ctx.iTotalDataLinesWritten:,} ({(ctx.iTotalDataLinesWritten/ctx.fTotalWrittenSecs):,.2f}/s, {ctx.iRunningWriters}w); ql:{iCurrentQueueSize:,}, mq:{shared.maxQueueLenObserved:,}; i:{ctx.iIdleTimeout:,}, it:{shared.idleSecsObserved.value:,}, Working={shared.Working.value}, ActiveJobs={ctx.iActiveJobsOnThisStream}', logLevel.STATSONPROCNAME)

            fEnd = timer()
            fTimeTaken = fEnd - fStart
            print('\n\n', file=sys.stdout, flush = True)

            if ctx.iTotalDataLinesWritten > 0:
                logging.statsPrint('queueStats', ctx.jobID, shared.maxQueueLenObserved, shared.maxQueueLenObservedEvents, 0)
                logging.logPrint(f'{ctx.iTotalDataLinesWritten:,} rows copied in {utils.seconds_to_readable(fTimeTaken)} ({(ctx.iTotalDataLinesWritten/fTimeTaken):,.2f}/sec).')
                logging.statsPrint('writeDataEnd', ctx.jobID, ctx.iTotalDataLinesWritten, ctx.fTotalWrittenSecs, shared.dataQueue.qsize())
            else:
                logging.logPrint(f'statement(s) executed in {utils.seconds_to_readable(fTimeTaken)}')

            logging.statsPrint('streamEnd', ctx.jobID, shared.idleSecsObserved.value, fTimeTaken, 0)
            shared.maxQueueLenObserved = 0
            shared.maxQueueLenObservedEvents = 0
            logging.logPrint(f'end of inner loop, with jobID=[{ctx.jobID}]', logLevel.DEBUG)
            ctx.jobID += 1

        logging.logPrint(f'end of outer loop, with jobID=[{ctx.jobID}]', logLevel.DEBUG)
        setproctitle(f'datacopy: jobManager thread, ended')
        with shared.Working.get_lock():
            shared.Working.value = False

    except Exception as e:
        logging.processError(p_e=e, p_message=f'({ctx.jobName}): unexpected exception', p_stack=traceback.format_exc(), p_stop=True, p_exitCode=5)
