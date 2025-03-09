'''job sequencer control'''

#pylint: disable=invalid-name, broad-except, bare-except, line-too-long

import sys

import re
from timeit import default_timer as timer
import multiprocessing as mp

import queue

import setproctitle
import traceback

import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.shared as shared
import modules.jobs as jobs
import modules.connections as connections
import modules.datahandlers as datahandlers

def jobManager():
    ''' main jobs handling loop'''

    shared.block_signals()

    try:
        setproctitle.setproctitle(f'datacopy: jobManager thread')

        iWriters = 0
        iRunningReaders = 0
        iRunningQueries = 0

        tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval

        jobID = 0

        iDataLinesRead = {}
        iReadSecs = {}

        iIdleTimeout = 0

        logging.logPrint(f'entering jobs loop, max readers allowed: [{shared.parallelReaders}]')

        bKeepGoing:bool = True

        #################################
        ### outer loop, for multi-stream (i.e., multiple destination tables) cases.
        #################################

        while jobID < len(shared.jobs) and bKeepGoing:
            logging.logPrint(f'outer loop, jobID=[{jobID}]', logLevel.DEBUG, p_jobID=jobID)

            sWriteFileMode = 'w'
            sCSVHeader = ''

            iTotalDataLinesRead = 0
            iTotalReadSecs = .001

            iRunningWriters = 0
            iTotalDataLinesWritten = 0
            iTotalWrittenSecs = .001

            iDataLinesRead[jobID] = 0
            iReadSecs[jobID] = .001

            writersNotStartedYet = True

            oMaxAlreadyInsertedData = None

            bEndOfJobs = False

            try:
                (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial, appendKeyColumn, getMaxQuery, getMaxDest) = jobs.calcJob(jobID)
            except Exception as e:
                logging.processError(p_e=e, p_stack=traceback.format_exc(), p_message=f"OutercalcJob({shared.jobs['index'][jobID]})")

            jobName = logging.getJobName(jobID)

            logging.statsPrint('streamStart', jobID, 0, 0, shared.parallelReaders)

            shared.idleSecsObserved.value = 0

            iStart = timer()

            if not shared.TEST_QUERIES:
                # cleaning up destination before inserts

                if connections.getConnectionParameter(dest, 'driver') == 'csv':
                    if  mode.upper() in ('T','D'):
                        sWriteFileMode='w'
                        logging.logPrint('creating new CSV file(s)', p_jobID=jobID)
                    else:
                        sWriteFileMode='a'

                        logging.logPrint('appending to existing CSV file(s)', p_jobID=jobID)
                else:
                    siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')
                    match mode.upper():
                        case 'T' | 'D':
                            cConn = connections.initConnections(dest, False, 1, table, 'w')[0]
                            cCleanData = cConn.cursor()
                            if len(preQueryDst) > 0:
                                try:
                                    logging.logPrint(f'preparing cursor to clean destination, executing preQueryDst=[{preQueryDst}]', logLevel.DEBUG, p_jobID=jobID)
                                    cCleanData.execute(preQueryDst)
                                except Exception as e:
                                    logging.processError(p_e=e, p_message=f'preparing cursor to clean destination, preQueryDst=[{preQueryDst}]', p_jobID=jobID,p_dontSendToStats=True)

                            match mode.upper():
                                case 'T':
                                    logging.logPrint(f'cleaning up table (truncate) [{dest}].[{table}]', p_jobID=jobID)
                                    cStart = timer()
                                    try:
                                        logging.statsPrint('truncateStart', jobID, 0, 0, 0)
                                        cleanDestSQL=f'truncate table {siObjSep}{table}{siObjSep}'
                                        cCleanData.execute(cleanDestSQL)
                                        cConn.commit()
                                        logging.statsPrint('truncateEnd', jobID, 0, timer() - cStart, 0)
                                    except Exception as e:
                                        logging.statsPrint('truncateError', jobID, 0, timer() - cStart, 0)
                                        logging.processError(p_e=e, p_message=f'truncating table [{dest}].[{table}] with sql=[{cleanDestSQL}]', p_jobID=jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                                        return
                                case 'D':
                                    logging.logPrint(f'cleaning up table (delete) [{dest}].[{table}]', p_jobID=jobID)
                                    cStart = timer()
                                    deletedRows=-1
                                    try:
                                        logging.statsPrint('deleteStart', jobID, 0, 0, 0)
                                        cleanDestSQL=f'delete from {siObjSep}{table}{siObjSep}'
                                        deletedRows=cCleanData.execute(cleanDestSQL)
                                        cConn.commit()
                                        logging.statsPrint('deleteEnd', jobID, deletedRows, timer() - cStart, 0)
                                    except Exception as e:
                                        logging.statsPrint('deleteError', jobID, 0, timer() - cStart, 0)
                                        logging.processError(p_e=e, p_message=f'deleting table: [{dest}].[{table}] with sql=[{cleanDestSQL}]', p_jobID=jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                                        return
                            cCleanData.close()
                            cConn.close()
                        case 'A':
                            if len(getMaxDest) == 0:
                                getMaxDest = dest

                            cConn = connections.initConnections(getMaxDest, False, 1, table, 'w')[0]
                            cGetMaxID = cConn.cursor()

                            logging.logPrint(f'figuring out max value for [{appendKeyColumn}] on [{getMaxDest}] with [{getMaxQuery}]', p_jobID=jobID)
                            cStart = timer()
                            try:
                                logging.statsPrint('getMaxAtDestinationStart', jobID, 0, 0, 0)
                                cGetMaxID.execute(getMaxQuery)
                                oMaxAlreadyInsertedData = cGetMaxID.fetchone()[0]

                                logging.statsPrint('getMaxAtDestinationEnd', jobID, oMaxAlreadyInsertedData, timer() - cStart, 0)
                                logging.logPrint(f'max value  is [{oMaxAlreadyInsertedData}]', p_jobID=jobID)
                            except Exception as e:
                                logging.statsPrint('getMaxAtDestinationError', jobID, 0, timer() - cStart, 0)
                                logging.processError(p_e=e, p_message=f'getting max value:', p_jobID=jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                                return

                            cGetMaxID.close()
                            cConn.close()

            if 'insert_cols' in shared.jobs:
                sOverrideCols = str(shared.jobs['insert_cols'][jobID])
            else:
                sOverrideCols = ''

            if 'ignore_cols' in shared.jobs:
                tIgnoreCols = (shared.jobs['ignore_cols'][jobID]).split(',')
            else:
                tIgnoreCols = ()

            logging.logPrint('entering insert loop...', p_jobID=jobID)
            shared.eventQueue.put( (shared.E_BOOT_READER, jobID, None, None ) )

            #################################
            ### inner loop (for each job on to the same stream, ie, target table. event based.)
            #################################

            bStopRequested:bool = False
            bReadyToStop:bool = False
            dumpedPackets = 0
            emptyQueueTimeout = 5

            while bKeepGoing:
                try:
                    eType, eJobID, recs, secs = shared.eventQueue.get(block = True, timeout = 1)
                    iIdleTimeout = 0
                    if eType not in (shared.E_READ, shared.E_WRITE):
                        try:
                            eventName = shared.eventsDecoder[eType]
                        except:
                            eventName = 'unkown!'
                        logging.logPrint(f'event [{eventName}] received, jobID=[{eJobID}], recs=[{recs}], secs=[{secs}]', logLevel.DEBUG, p_jobID=eJobID)

                    match eType:
                        case shared.E_READ:
                            iDataLinesRead[eJobID] += recs
                            iTotalDataLinesRead += recs
                            iReadSecs[eJobID] += secs
                            iTotalReadSecs += secs

                        case shared.E_WRITE:
                            iTotalDataLinesWritten += recs
                            iTotalWrittenSecs += secs

                        case shared.E_BOOT_READER:

                            #don't start any new things if something happended meanwhile
                            if not shared.Working.value:
                                continue

                            iDataLinesRead[eJobID] = 0
                            iReadSecs[eJobID] = .001

                            try:
                                (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWritersIgnored, bCloseStream, bCSVEncodeSpecial, appendKeyColumn, getMaxQuery, getMaxDest) = jobs.calcJob(eJobID) #pylint: disable=unused-variable
                            except Exception as e:
                                logging.processError(p_e=e, p_message=f"InnercalcJob({shared.jobs['index'][eJobID]})", p_jobID=eJobID, p_stop=True, p_exitCode=5)

                            jobName = logging.getJobName(eJobID)

                            isSelect = re.search('(^|[ \t\n]+)SELECT[ \t\n]+', query.upper())
                            siObjSep = connections.getConnectionParameter(source, 'insert_object_delimiter')

                            if mode.upper() == 'A' and oMaxAlreadyInsertedData:
                                if shared.identify_type(oMaxAlreadyInsertedData) in ('integer', 'float'):
                                    sMaxAlreadyInsertedData = f'{oMaxAlreadyInsertedData}'
                                else:
                                    sMaxAlreadyInsertedData = f"'{oMaxAlreadyInsertedData}'"

                                if isSelect:
                                    query = re.sub('#MAX_KEY_VALUE#', sMaxAlreadyInsertedData, query)
                                else:
                                    query = f'SELECT * FROM {siObjSep}{query}{siObjSep} WHERE {siObjSep}{appendKeyColumn}{siObjSep} > {sMaxAlreadyInsertedData}'
                            else:
                                if not isSelect:
                                    # it means query is just a table name, expand to select
                                    query = f'SELECT * FROM {siObjSep}{query}{siObjSep}'

                            shared.GetConn[eJobID] = connections.initConnections(source, True, 1)[0]
                            shared.GetData[eJobID] = connections.initCursor(shared.GetConn[eJobID], eJobID, source, fetchSize)
                            if len(preQuerySrc) > 0:
                                logging.logPrint(f'({source}): preparing source for selects, sending preQuerySrc=[{preQuerySrc}]', logLevel.DEBUG, p_jobID=eJobID)
                                try:
                                    shared.GetData[eJobID].execute(preQuerySrc)
                                except Exception as e:
                                    logging.processError(p_e=e, p_message=f'({source}): preparing source for selects, sending preQuerySrc=[{preQuerySrc}]', p_jobID=eJobID, p_dontSendToStats=True)
                            if len(source2) > 0:
                                shared.GetConn2[eJobID] = connections.initConnections(source2, True, 1)[0]
                                shared.GetData2[eJobID] = connections.initCursor(shared.GetConn2[eJobID], eJobID, source2, fetchSize)
                            logging.logPrint(f'starting reading from [{source}] to [{dest}].[{table}], with query:\n***\n{query}\n***', p_jobID=eJobID)
                            if len(query2) > 0:
                                logging.logPrint(f'and from [{source2}] with query:\n***\n{query2}\n***', p_jobID=eJobID)
                                shared.readP[eJobID]=mp.Process(target=datahandlers.readData2, args = (eJobID, shared.GetConn[eJobID], shared.GetConn2[eJobID], shared.GetData[eJobID], shared.GetData2[eJobID], fetchSize, query, query2))
                            else:
                                shared.readP[eJobID]=mp.Process(target=datahandlers.readData, args = (eJobID, shared.GetConn[eJobID], shared.GetData[eJobID], fetchSize, query))
                            shared.readP[eJobID].start()
                            iRunningReaders += 1
                            shared.runningReaders.value = iRunningReaders

                        case shared.E_QUERY_START:
                            iRunningQueries += 1
                            logging.statsPrint('execQueryStart', eJobID, 0, 0, iRunningQueries)

                        case shared.E_QUERY_ERROR:
                            iRunningQueries -= 1
                            logging.statsPrint('execQueryError', eJobID, 0, secs, iRunningQueries)
                            logging.processError(p_message='QUERY ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_stop=True, p_exitCode=6)

                        case shared.E_QUERY_END:
                            iRunningQueries -= 1
                            logging.statsPrint('execQueryEnd', eJobID, 0, secs, iRunningQueries)

                        case shared.E_READ_START:

                            #don't start any new things if something happended meanwhile
                            if writersNotStartedYet and shared.Working.value:
                                logging.statsPrint('readDataStart', eJobID, 0, 0, iRunningReaders)
                                iRunningWriters = 0
                                shared.runningWriters.value = 0
                                iTotalDataLinesWritten = 0
                                iTotalWrittenSecs = .001                            # only start writers after a sucessful read
                                logging.logPrint(f'writersNotStartedYet, processing cols to prepare insert statement: [{recs}]', logLevel.DEBUG, p_jobID=eJobID)
                                sColNames = ''
                                sColsPlaceholders = ''

                                workingCols = None

                                match sOverrideCols:
                                    case '' | '@' | '@l' | '@u':
                                        workingCols = recs
                                    case '@d':
                                        # from destination:
                                        cConn = connections.initConnections(dest, False, 1, table, 'r')[0]
                                        tdCursor = cConn.cursor()
                                        if len(preQueryDst) > 0:
                                            try:
                                                logging.logPrint(f'retrieving cols for @d, executing preQueryDst=[{preQueryDst}]', logLevel.DEBUG, p_jobID=eJobID)
                                                tdCursor.execute(preQueryDst)
                                            except Exception as e:
                                                logging.processError(p_e=e, p_message=f'retrieving cols for @d, preQueryDst=[{preQueryDst}]', p_jobID=eJobID,p_dontSendToStats=True)
                                        siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')
                                        fetchColsFromDestSql=f'SELECT * FROM {siObjSep}{table}{siObjSep}  WHERE 1=0'
                                        logging.logPrint(f'retrieving cols for @d, executing preQueryDst=[{fetchColsFromDestSql}]', logLevel.DEBUG, p_jobID=eJobID)
                                        tdCursor.execute(fetchColsFromDestSql)
                                        workingCols = tdCursor.description
                                        cConn.rollback() #somehow, this select blocks truncates on postgres, if not rolled back?...
                                        tdCursor.close()
                                    case _:
                                        workingCols = []
                                        for col in sOverrideCols.split(','):
                                            workingCols.append( (col,'dummy') )

                                sIP = connections.getConnectionParameter(dest, 'insert_placeholder')
                                siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')

                                for col in workingCols:
                                    if col[0] not in tIgnoreCols:
                                        sColNames = f'{sColNames}{siObjSep}{col[0]}{siObjSep},'
                                        sColsPlaceholders = f'{sColsPlaceholders}{sIP},'
                                sColNames = sColNames[:-1]
                                sColsPlaceholders = sColsPlaceholders[:-1]

                                iQuery = ''
                                match sOverrideCols:
                                    case '@d':
                                        iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sColNames}) VALUES ({sColsPlaceholders})'
                                        sIcolType = 'from destination'
                                    case '@l':
                                        iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sColNames.lower()}) VALUES ({sColsPlaceholders})'
                                        sIcolType = 'from source, lowercase'
                                    case '@u':
                                        iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sColNames.upper()}) VALUES ({sColsPlaceholders})'
                                        sIcolType = 'from source, upercase'
                                    case _:
                                        if len(sOverrideCols)>0 and sOverrideCols[0] != '@':
                                            iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sOverrideCols}) VALUES ({sColsPlaceholders})'
                                            sIcolType = 'overridden'
                                        else:
                                            iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sColNames}) VALUES ({sColsPlaceholders})'
                                            sIcolType = 'from source'

                                sColNamesNoQuotes = sColNames.replace(f'{siObjSep}','')

                                logging.logPrint(sColNamesNoQuotes.split(','), logLevel.DUMP_COLS)
                                if connections.getConnectionParameter(dest, 'driver') == 'csv':
                                    logging.logPrint(f'cols for CSV file(s): [{sColNamesNoQuotes}]', p_jobID=eJobID)
                                    if  mode.upper() in ('T','D'):
                                        sCSVHeader = sColNamesNoQuotes
                                    else:
                                        sCSVHeader = ''

                                else:
                                    logging.logPrint(f'insert query (cols {sIcolType}): [{iQuery}]', p_jobID=eJobID)

                                if not shared.TEST_QUERIES:
                                    logging.logPrint(f'number of writers for this job: [{nbrParallelWriters}]', p_jobID=eJobID)

                                    newWriteConns = connections.initConnections(dest, False, nbrParallelWriters, table, sWriteFileMode)
                                    if newWriteConns is None:
                                        logging.processError(p_message='InitConnections returned None, giving up', p_stop=True)
                                    else:
                                        shared.stopWhenEmpty.value = False
                                        for x in range(nbrParallelWriters):
                                            shared.PutConn[iWriters] = newWriteConns[x]
                                            if isinstance(newWriteConns[x], tuple):
                                                shared.PutData[iWriters] = None
                                                shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeDataCSV, args = (eJobID, iWriters, shared.PutConn[iWriters], sCSVHeader, bCSVEncodeSpecial) ))
                                                shared.writeP[iWriters].start()
                                            else:
                                                shared.PutData[iWriters] = shared.PutConn[iWriters].cursor()
                                                if len(preQueryDst) > 0:
                                                    try:
                                                        logging.logPrint(f'preparing cursor #{iWriters} for inserts, executing preQueryDst=[{preQueryDst}]', logLevel.DEBUG, p_jobID=eJobID)
                                                        shared.PutData[iWriters].execute(preQueryDst)
                                                    except Exception as e:
                                                        logging.processError(p_e=e, p_message=f'preparing cursor #{iWriters} for inserts, preQueryDst=[{preQueryDst}]', p_jobID=eJobID,p_dontSendToStats=True)
                                                shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeData, args = (eJobID, iWriters, shared.PutConn[iWriters], shared.PutData[iWriters], iQuery) ))
                                                shared.writeP[iWriters].start()
                                            iWriters += 1
                                            iRunningWriters += 1
                                            shared.runningWriters.value = iRunningWriters

                                        writersNotStartedYet = False
                                        logging.statsPrint('writeDataStart', eJobID, 0, 0, iRunningWriters)

                        case shared.E_READ_ERROR:
                            logging.statsPrint('readDataError', eJobID, iDataLinesRead[eJobID], iReadSecs[eJobID], iRunningReaders)
                            logging.processError(p_message='READ ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_stop=True, p_exitCode=6)

                        case shared.E_READ_END:
                            iRunningReaders -= 1
                            shared.runningReaders.value = iRunningReaders
                            logging.statsPrint('readDataEnd', eJobID, iDataLinesRead[eJobID], iReadSecs[eJobID], iRunningReaders)
                            try:
                                shared.readP[eJobID].join(timeout=1)
                            except:
                                pass

                            if iRunningReaders == 0 and bCloseStream:
                                logging.logPrint('signaling the end of data for this stream.', p_jobID=eJobID)
                                shared.stopWhenEmpty.value = True

                        case shared.E_WRITE_START:
                            pass

                        case shared.E_WRITE_ERROR:
                            logging.statsPrint('writeDataError', eJobID, iTotalDataLinesWritten, -1, iRunningWriters)
                            logging.processError(p_message='WRITE ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_threadID=recs, p_stop=True, p_exitCode=7)

                        case shared.E_WRITE_END:
                            iRunningWriters -= 1
                            shared.runningWriters.value = iRunningWriters
                            try:
                                shared.writeP[eJobID].join(timeout=1)
                            except:
                                pass

                        case shared.E_STOP:
                            setproctitle.setproctitle(f'datacopy: jobManager thread, stop received')
                            bStopRequested = True

                        case shared.E_NOOP:
                            #no operation. just to force the common part of event processing.
                            pass

                        case _:
                            logging.logPrint(f'unknown event in insert loop ({eType}), should not happen!', p_jobID=eJobID)

                #nothing happended the last second, check idle timeout
                except queue.Empty:
                    if not bStopRequested:
                        iIdleTimeout += 1
                        shared.idleSecsObserved.value += 1

                        if shared.idleTimeoutSecs > 0 and iIdleTimeout > shared.idleTimeoutSecs:
                            logging.statsPrint('IdleTimeoutError', jobID, 0, shared.idleTimeoutSecs, 0)
                            logging.processError(p_message=f'idle timeout secs [{shared.idleTimeoutSecs}] reached.', p_dontSendToStats=True, p_stop=True, p_exitCode=5)

                            #try to close all cursors and connections to force the drivers to give up
                            allObjectsToClose={**shared.GetData, **shared.GetData2, **shared.PutData, **shared.GetConn, **shared.GetConn2, **shared.PutConn}
                            for k,v in allObjectsToClose.items():
                                try:
                                    v[k].close()
                                    v[k] = None
                                except Exception as e:
                                    logging.logPrint(f'ignored error on forcing close on timeout, [{k}][{v[k]}]: [{e}]', logLevel.DEBUG)
                                    pass #do not remove as on production we delete the previous line

                        if ( iIdleTimeout > 3 and iRunningWriters == 0 and iRunningReaders == 0 ):
                            #exit inner loop
                            break
                    else:
                        #apply the brakes...
                        while not bReadyToStop:
                            try:
                                dummy = shared.dataQueue.get(block = True, timeout = 1 )
                                dummy = None
                                dumpedPackets += 1
                            except queue.Empty:
                                emptyQueueTimeout -= 1
                                if emptyQueueTimeout == 0:
                                    logging.logPrint(f'stopping, timing out', logLevel.DEBUG, p_jobID=eJobID)
                                    bReadyToStop = True
                                if shared.runningReaders.value == 0:
                                    logging.logPrint('stopping, no more running readers detected, early exit', logLevel.DEBUG, p_jobID=eJobID)
                                    bReadyToStop = True
                                break

                        if bReadyToStop:
                            if shared.ErrorOccurred.value:
                                logging.statsPrint('dumpDataOnError', eJobID, dumpedPackets, 0, 0)
                                logging.logPrint(f'dumped {dumpedPackets} packets from dataQueue', logLevel.DEBUG, p_jobID=eJobID)
                            bKeepGoing = False

                #common part of event processing:
                iCurrentQueueSize = shared.dataQueue.qsize()

                if iCurrentQueueSize > shared.maxQueueLenObserved:
                    shared.maxQueueLenObserved = iCurrentQueueSize

                if iCurrentQueueSize == shared.queueSize:
                    shared.maxQueueLenObservedEvents +=1

                if tParallelReadersNextCheck < timer():
                    tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval
                    if  bKeepGoing and (not bEndOfJobs and not bCloseStream and iRunningReaders < shared.parallelReaders and iCurrentQueueSize<shared.usedQueueBeforeNew):
                        if jobID<len(shared.jobs)-1:
                            jobID += 1
                            jobName = logging.getJobName(jobID)
                            shared.eventQueue.put( (shared.E_BOOT_READER, jobID, None, None ) )
                        else:
                            logging.logPrint('no more jobs, stopping launches', logLevel.DEBUG, p_jobID=jobID)
                            jobID += 1
                            bEndOfJobs = True

                if shared.Working.value:
                    if shared.SCREEN_STATS:
                        statsLine=f'\r{iTotalDataLinesRead:,} recs read ({(iTotalDataLinesRead/iTotalReadSecs):,.2f}/sec x {iRunningReaders}r,{iRunningQueries}q), {iTotalDataLinesWritten:,} recs written ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/sec x {iRunningWriters}), queue len: {iCurrentQueueSize:,}, max queue: {shared.maxQueueLenObserved:,}, timeout timer: {iIdleTimeout:,}, idle time: {shared.idleSecsObserved.value:,}        '
                        if shared.SCREEN_STATS_TO_STDOUT:
                            print(statsLine, file=sys.stdout, end='', flush = True)
                        else:
                            print(statsLine, file=sys.stderr, end='', flush = True)
                    logging.logPrint(f'reads:{iTotalDataLinesRead:,} ({(iTotalDataLinesRead/iTotalReadSecs):,.2f}/s x {iRunningReaders}r,{iRunningQueries}q); writes:{iTotalDataLinesWritten:,} ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/s x {iRunningWriters}); ql:{iCurrentQueueSize:,}, mq:{shared.maxQueueLenObserved:,}; i:{iIdleTimeout:,}, it:{shared.idleSecsObserved.value:,}, Working=[{shared.Working.value}', logLevel.STATSONPROCNAME)

            logging.statsPrint('queueStats', jobID, shared.maxQueueLenObserved, shared.maxQueueLenObservedEvents, fetchSize)
            print('\n\n', file=sys.stdout, flush = True)
            iEnd = timer()
            iTimeTaken = iEnd - iStart
            logging.logPrint(f'{iTotalDataLinesWritten:,} rows copied in {iTimeTaken:,.2f} seconds ({(iTotalDataLinesWritten/iTimeTaken):,.2f}/sec).')
            logging.statsPrint('writeDataEnd', jobID, iTotalDataLinesWritten, iTotalWrittenSecs, nbrParallelWriters)
            logging.statsPrint('streamEnd', jobID, shared.idleSecsObserved.value, iTimeTaken, 0)
            shared.maxQueueLenObserved = 0
            shared.maxQueueLenObservedEvents = 0
            logging.logPrint(f'end of inner loop, with jobID=[{jobID}]', logLevel.DEBUG)
            jobID += 1

        logging.logPrint(f'end of outer loop, with jobID=[{jobID}]', logLevel.DEBUG)
        setproctitle.setproctitle(f'datacopy: jobManager thread, ended')
        with shared.Working.get_lock():
            shared.Working.value = False

    except Exception as e:
        logging.processError(p_e=e, p_message=f'({jobName}): unexpected exception', p_stack=traceback.format_exc(), p_stop=True, p_exitCode=5)
