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
import modules.datahandlers as datahandlers

def jobManager():
    ''' main jobs handling loop'''

    utils.block_signals()

    jobName:str='<unknown>'
    try:
        setproctitle(f'datacopy: jobManager thread')

        iWriters:int = 0
        iRunningReaders:int = 0
        iReadingReaders:int = 0
        iRunningQueries:int = 0

        tParallelReadersNextCheck:float = 0

        jobID = 1

        iDataLinesRead:dict[int, int] = {}
        iReadSecs:dict[int, float] = {}
        iDetailsQueriesSecs:dict[int, float] = {}

        iIdleTimeout:int = 0

        logging.logPrint(f'entering jobs loop, max readers allowed: [{shared.parallelReaders}]')

        bKeepGoing:bool = True

        #################################
        ### outer loop, for multi-stream (i.e., multiple destination tables) cases.
        #################################


        while jobID < len(shared.jobs)+1 and bKeepGoing:
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

            thisJob = jobs.Job(jobID)
            jobName = thisJob.jobName

            logging.statsPrint('streamStart', jobID, shared.parallelReaders, thisJob.nbrParallelWriters, 0)

            shared.idleSecsObserved.value = 0

            iStart = timer()

            if not shared.TEST_QUERIES:
                # cleaning up destination before inserts

                if connections.getConnectionParameter(thisJob.dest, 'driver') == 'csv':
                    if  thisJob.mode.upper() in ('T','D'):
                        sWriteFileMode='w'
                        logging.logPrint('creating new CSV file(s)', p_jobID=jobID)
                    else:
                        sWriteFileMode='a'
                        logging.logPrint('appending to existing CSV file(s)', p_jobID=jobID)
                else:
                    siObjSep = connections.getConnectionParameter(thisJob.dest, 'insert_object_delimiter')
                    match thisJob.mode.upper():
                        case 'T' | 'D':
                            cConn = connections.initConnections(thisJob.dest, False, 1, thisJob.table, 'w')[0]
                            cCleanData = cConn.cursor()
                            match thisJob.mode.upper():
                                case 'T':
                                    logging.logPrint(f'cleaning up table (truncate) [{thisJob.dest}].[{thisJob.table}]', p_jobID=jobID)
                                    cStart = timer()
                                    cleanDestSQL=f'truncate table {siObjSep}{thisJob.table}{siObjSep}'
                                    try:
                                        logging.statsPrint('truncateStart', jobID, 0, 0, 0)
                                        cCleanData.execute(cleanDestSQL)
                                        cConn.commit()
                                        logging.statsPrint('truncateEnd', jobID, 0, timer() - cStart, 0)
                                    except Exception as e:
                                        logging.statsPrint('truncateError', jobID, 0, timer() - cStart, 0)
                                        logging.processError(p_e=e, p_message=f'truncating table [{thisJob.dest}].[{thisJob.table}] with sql=[{cleanDestSQL}]', p_jobID=jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                                        return
                                case 'D':
                                    logging.logPrint(f'cleaning up table (delete) [{thisJob.dest}].[{thisJob.table}]', p_jobID=jobID)
                                    cStart = timer()
                                    deletedRows=-1
                                    cleanDestSQL=f'delete from {siObjSep}{thisJob.table}{siObjSep}'
                                    try:
                                        logging.statsPrint('deleteStart', jobID, 0, 0, 0)
                                        deletedRows=cCleanData.execute(cleanDestSQL)
                                        cConn.commit()
                                        logging.statsPrint('deleteEnd', jobID, deletedRows, timer() - cStart, 0)
                                    except Exception as e:
                                        logging.statsPrint('deleteError', jobID, 0, timer() - cStart, 0)
                                        logging.processError(p_e=e, p_message=f'deleting table: [{thisJob.dest}].[{thisJob.table}] with sql=[{cleanDestSQL}]', p_jobID=jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                                        return
                            cCleanData.close()
                            cConn.close()
                        case 'A':
                            if len(thisJob.getMaxDest) == 0:
                                getMaxDest = thisJob.dest
                            else:
                                getMaxDest = thisJob.getMaxDest

                            cConn = connections.initConnections(getMaxDest, False, 1, thisJob.table, 'w')[0]
                            cGetMaxID = cConn.cursor()

                            logging.logPrint(f'figuring out max value for [{thisJob.appendKeyColumn}] on [{thisJob.getMaxDest}] with [{thisJob.getMaxQuery}]', p_jobID=jobID)
                            cStart = timer()
                            try:
                                logging.statsPrint('getMaxAtDestinationStart', jobID, 0, 0, 0)
                                cGetMaxID.execute(thisJob.getMaxQuery)
                                oMaxAlreadyInsertedData = cGetMaxID.fetchone()[0]

                                logging.statsPrint('getMaxAtDestinationEnd', jobID, oMaxAlreadyInsertedData, timer() - cStart, 0)
                                logging.logPrint(f'max value  is [{oMaxAlreadyInsertedData}]', p_jobID=jobID)
                            except Exception as e:
                                logging.statsPrint('getMaxAtDestinationError', jobID, 0, timer() - cStart, 0)
                                logging.processError(p_e=e, p_message=f'getting max value:', p_jobID=jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                                return

                            cGetMaxID.close()
                            cConn.close()


            logging.logPrint('entering insert loop...', p_jobID=jobID)
            shared.eventQueue.put( (shared.E_BOOT_READER, jobID, None, None ) )

            #################################
            ### inner loop (for each job on to the same stream, ie, target table. event based.)
            #################################

            bStopRequested:bool = False
            bReadyToStop:bool = False
            dumpedPackets:int = 0
            emptyQueueTimeout:int = 5
            bFirstDetailQueryStarted = False
            iActiveJobsOnThisStream:int = 1 #starts at one because the first boot reader event is triggered above

            while bKeepGoing:
                eJobID = 0
                try:
                    eType, eJobID, recs, secs = shared.eventQueue.get(block = True, timeout = 1)
                    iIdleTimeout = 0
                    if shared.DEBUG_READWRITES or (eType not in (shared.E_READ, shared.E_WRITE)):   #DISABLE_IN_PROD
                        try:                                                                        #DISABLE_IN_PROD
                            eventName = shared.eventsDecoder[eType]                            #DISABLE_IN_PROD
                        except:                                                                     #DISABLE_IN_PROD
                            eventName = 'unkown!'                                                   #DISABLE_IN_PROD
                        logging.logPrint(f'event [{eventName}] received, jobID=[{eJobID}], recs=[{recs}], secs=[{secs}], ActiveJobs=[{iActiveJobsOnThisStream}]', logLevel.DEBUG, p_jobID=eJobID)

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

                            #don't start any new things if something bad happended meanwhile
                            if not shared.Working.value:
                                continue

                            iDataLinesRead[eJobID] = 0
                            iReadSecs[eJobID] = .001

                            thisJob = jobs.Job(eJobID)
                            jobName = thisJob.jobName

                            isSelect = re.search('(^|[ \t\n]+)SELECT[ \t\n]+', thisJob.query.upper())
                            siObjSep = connections.getConnectionParameter(thisJob.source, 'insert_object_delimiter')

                            if thisJob.mode.upper() == 'A' and oMaxAlreadyInsertedData:
                                if utils.identify_type(oMaxAlreadyInsertedData) in ('integer', 'float'):
                                    sMaxAlreadyInsertedData = f'{oMaxAlreadyInsertedData}'
                                else:
                                    sMaxAlreadyInsertedData = f"'{oMaxAlreadyInsertedData}'"

                                if isSelect:
                                    thisJob.query = re.sub('#MAX_KEY_VALUE#', sMaxAlreadyInsertedData, thisJob.query)
                                else:
                                    thisJob.query = f'SELECT * FROM {siObjSep}{thisJob.query}{siObjSep} WHERE {siObjSep}{thisJob.appendKeyColumn}{siObjSep} > {sMaxAlreadyInsertedData}'
                            else:
                                if not isSelect:
                                    # it means query is just a table name, expand to select
                                    if thisJob.sourceDriver != 'csv':
                                        thisJob.query = f'SELECT * FROM {siObjSep}{thisJob.query}{siObjSep}'


                            # dual query case:
                            if len(thisJob.key_source) > 0:
                                logging.logPrint(f'reading keys from [{thisJob.key_source}] with query:\n***\n{thisJob.key_query}\n***', p_jobID=eJobID)
                                logging.logPrint(f'and reading data from [{thisJob.source}] with query:\n***\n{thisJob.query}\n***', p_jobID=eJobID)

                                r1JobID:int = eJobID * -1
                                r1Query:str = thisJob.key_query
                                r1SourceDriver:str = thisJob.key_sourceDriver
                                r1FetchSize:int = thisJob.key_fetchSize
                                if thisJob.key_sourceDriver == 'csv':
                                    shared.GetConn[r1JobID] = connections.initConnections(thisJob.key_source, True, 1, p_tableName=thisJob.key_query, p_mode='r')[0]
                                else:
                                    shared.GetConn[r1JobID] = connections.initConnections(thisJob.key_source, True, 1)[0]
                                    shared.GetData[r1JobID] = connections.initCursor(p_conn=shared.GetConn[r1JobID], p_jobID=eJobID, p_source=thisJob.key_source, p_fetchSize=thisJob.fetchSize)

                                for i in range(1, shared.parallelReaders+1):
                                    thisThreadID=eJobID*1000+i
                                    shared.GetConn[thisThreadID] = connections.initConnections(thisJob.source, True, 1)[0]
                                    shared.GetData[thisThreadID] = connections.initCursor(p_conn=shared.GetConn[thisThreadID], p_jobID=eJobID, p_source=thisJob.source, p_fetchSize=thisJob.fetchSize)

                                    r2=mp.Process(target=datahandlers.readData2, args = (eJobID, thisThreadID, shared.GetConn[thisThreadID], shared.GetData[thisThreadID], thisJob.query, thisJob.fetchSize))
                                    shared.readP[thisThreadID]=r2
                                    iRunningReaders += 1

                                    r2.start()


                                outQueue:mp.Queue = shared.dataKeysQueue
                                r1FinalDataReader = False
                                iDetailsQueriesSecs[eJobID] = 0.001
                            else:
                                r1JobID = eJobID
                                r1Query:str=thisJob.query
                                r1SourceDriver:str = thisJob.key_source
                                r1FetchSize=thisJob.fetchSize
                                shared.GetConn[r1JobID] = connections.initConnections(p_name=thisJob.source, p_readOnly=True, p_qtd=1, p_tableName=thisJob.query, p_mode='r')[0]
                                outQueue:mp.Queue = shared.dataQueue
                                r1FinalDataReader = True

                                if r1SourceDriver != 'csv':
                                    shared.GetData[r1JobID] = connections.initCursor(p_conn=shared.GetConn[r1JobID], p_jobID=eJobID, p_source=thisJob.source, p_fetchSize=thisJob.fetchSize)
                                    logging.logPrint(f'reading data from [{thisJob.source}] with query:\n***\n{thisJob.query}\n***', p_jobID=eJobID)
                                else:
                                    logging.logPrint(f'reading data from file [{shared.GetConn[r1JobID][0].name}]', p_jobID=eJobID)
                                    iRunningReaders += 1

                            iDataLinesRead[r1JobID] = 0
                            iReadSecs[r1JobID] = .001

                            if r1SourceDriver == 'csv':
                                shared.readP[r1JobID]=mp.Process(target=datahandlers.readDataCSV, args = (r1JobID, shared.GetConn[r1JobID], r1FetchSize, outQueue, r1FinalDataReader))
                            else:
                                shared.readP[r1JobID]=mp.Process(target=datahandlers.readData, args = (r1JobID, shared.GetConn[r1JobID], shared.GetData[r1JobID], r1FetchSize, r1Query, outQueue, r1FinalDataReader))
                            shared.readP[r1JobID].start()

                        case shared.E_QUERY_START:
                            iRunningQueries += 1
                            logging.statsPrint('execQueryStart', eJobID, iActiveJobsOnThisStream, 0, iRunningQueries)

                        case shared.E_KEYS_QUERY_START:
                            iRunningQueries += 1
                            logging.statsPrint('execKeysQueryStart', eJobID, 0, 0, iRunningQueries)

                        case shared.E_DETAIL_QUERY_START:
                            iRunningQueries += 1
                            if bFirstDetailQueryStarted == False:
                                bFirstDetailQueryStarted = True
                                logging.statsPrint('execDetailQueriesStart', eJobID, 0, 0, iRunningQueries)

                        case shared.E_QUERY_ERROR:
                            iRunningQueries -= 1
                            logging.statsPrint('execQueryError', eJobID, 0, secs, iRunningQueries)
                            logging.processError(p_message='QUERY ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_stop=True, p_exitCode=6)

                        case shared.E_QUERY_END:
                            iRunningQueries -= 1
                            logging.statsPrint('execQueryEnd', eJobID, iActiveJobsOnThisStream, secs, iRunningQueries)

                        case shared.E_KEYS_QUERY_END:
                            iRunningQueries -= 1
                            logging.statsPrint('execKeysQueryEnd', eJobID, 0, secs, iRunningQueries)

                        case shared.E_DETAIL_QUERY_END:
                            iRunningQueries -= 1
                            iDetailsQueriesSecs[eJobID] += secs

                        case shared.E_READ_START:

                            iReadingReaders += 1

                            #don't start any new things if something happended meanwhile
                            if writersNotStartedYet and shared.Working.value:
                                logging.statsPrint('readDataStart', p_jobID=eJobID, p_recs=secs, p_secs=0, p_threads=iRunningReaders)
                                iRunningWriters = 0
                                iTotalDataLinesWritten = 0
                                iTotalWrittenSecs = .001
                                logging.logPrint(f'writersNotStartedYet, processing cols to prepare insert statement: [{recs}]', logLevel.DEBUG, p_jobID=eJobID)
                                sColNames = ''
                                sColsPlaceholders = ''

                                workingCols = None

                                match thisJob.overrideCols:
                                    case '' | '@' | '@l' | '@u':
                                        workingCols = recs
                                    case '@d':
                                        # from destination:
                                        cConn = connections.initConnections(thisJob.dest, False, 1, thisJob.table, 'r')[0]
                                        tdCursor = cConn.cursor()
                                        siObjSep = connections.getConnectionParameter(thisJob.dest, 'insert_object_delimiter')
                                        fetchColsFromDestSql=f'SELECT * FROM {siObjSep}{thisJob.table}{siObjSep}  WHERE 1=0'
                                        logging.logPrint(f'retrieving cols for @d, executing [{fetchColsFromDestSql}]', logLevel.DEBUG, p_jobID=eJobID)
                                        tdCursor.execute(fetchColsFromDestSql)
                                        workingCols = tdCursor.description
                                        cConn.rollback() #somehow, this select blocks truncates on postgres, if not rolled back?...
                                        tdCursor.close()
                                    case _:
                                        workingCols = []
                                        for col in thisJob.overrideCols.split(','):
                                            workingCols.append( (col,'dummy') )

                                sIP = connections.getConnectionParameter(thisJob.dest, 'insert_placeholder')
                                siObjSep = connections.getConnectionParameter(thisJob.dest, 'insert_object_delimiter')

                                for col in workingCols:
                                    if col[0] not in thisJob.ignoreCols:
                                        sColNames = f'{sColNames}{siObjSep}{col[0]}{siObjSep},'
                                        sColsPlaceholders = f'{sColsPlaceholders}{sIP},'
                                sColNames = sColNames[:-1]
                                sColsPlaceholders = sColsPlaceholders[:-1]

                                iQuery = ''
                                match thisJob.overrideCols:
                                    case '@d':
                                        iQuery = f'INSERT INTO {siObjSep}{thisJob.table}{siObjSep}({sColNames}) VALUES ({sColsPlaceholders})'
                                        sIcolType = 'from destination'
                                    case '@l':
                                        iQuery = f'INSERT INTO {siObjSep}{thisJob.table}{siObjSep}({sColNames.lower()}) VALUES ({sColsPlaceholders})'
                                        sIcolType = 'from source, lowercase'
                                    case '@u':
                                        iQuery = f'INSERT INTO {siObjSep}{thisJob.table}{siObjSep}({sColNames.upper()}) VALUES ({sColsPlaceholders})'
                                        sIcolType = 'from source, upercase'
                                    case _:
                                        if len(thisJob.overrideCols)>0 and thisJob.overrideCols[0] != '@':
                                            iQuery = f'INSERT INTO {siObjSep}{thisJob.table}{siObjSep}({thisJob.overrideCols}) VALUES ({sColsPlaceholders})'
                                            sIcolType = 'overridden'
                                        else:
                                            iQuery = f'INSERT INTO {siObjSep}{thisJob.table}{siObjSep}({sColNames}) VALUES ({sColsPlaceholders})'
                                            sIcolType = 'from source'

                                sColNamesNoQuotes:str = sColNames.replace(f'{siObjSep}','')

                                logging.logPrint(sColNamesNoQuotes.split(','), logLevel.DUMP_COLS)
                                if connections.getConnectionParameter(thisJob.dest, 'driver') == 'csv':
                                    logging.logPrint(f'cols for CSV file(s): [{sColNamesNoQuotes}]', p_jobID=eJobID)
                                    if  thisJob.mode.upper() in ('T','D'):
                                        sCSVHeader:str = sColNamesNoQuotes
                                    else:
                                        sCSVHeader:str = ''

                                else:
                                    logging.logPrint(f'insert query (cols {sIcolType}): [{iQuery}]', p_jobID=eJobID)

                                if not shared.TEST_QUERIES:
                                    logging.logPrint(f'number of writers for this job: [{thisJob.nbrParallelWriters}]', p_jobID=eJobID)

                                    newWriteConns = connections.initConnections(thisJob.dest, False, thisJob.nbrParallelWriters, thisJob.table, sWriteFileMode)
                                    if newWriteConns is None:
                                        logging.processError(p_message='InitConnections returned None, giving up', p_stop=True)
                                    else:
                                        with shared.stopWhenEmpty.get_lock():
                                            shared.stopWhenEmpty.value = False
                                        for x in range(thisJob.nbrParallelWriters):
                                            shared.PutConn[iWriters] = newWriteConns[x]
                                            if isinstance(newWriteConns[x], tuple):
                                                shared.PutData[iWriters] = None
                                                shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeDataCSV, args = (eJobID, iWriters, shared.PutConn[iWriters], sCSVHeader, thisJob.bCSVEncodeSpecial) ))
                                                shared.writeP[iWriters].start()
                                            else:
                                                shared.PutData[iWriters] = shared.PutConn[iWriters].cursor()
                                                if len(thisJob.preQueryDst) > 0:
                                                    try:
                                                        logging.logPrint(f'preparing cursor #{iWriters} for inserts, executing preQueryDst=[{thisJob.preQueryDst}]', logLevel.DEBUG, p_jobID=eJobID)
                                                        shared.PutData[iWriters].execute(thisJob.preQueryDst)
                                                    except Exception as e:
                                                        logging.processError(p_e=e, p_message=f'preparing cursor #{iWriters} for inserts, preQueryDst=[{thisJob.preQueryDst}]', p_jobID=eJobID,p_dontSendToStats=True)
                                                shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeData, args = (eJobID, iWriters, shared.PutConn[iWriters], shared.PutData[iWriters], iQuery) ))
                                                shared.writeP[iWriters].start()
                                            iWriters += 1
                                            iRunningWriters += 1

                                        writersNotStartedYet = False
                                        logging.statsPrint('writeDataStart', eJobID, 0, 0, thisJob.nbrParallelWriters)

                        case shared.E_READ_ERROR:
                            logging.statsPrint('readDataError', eJobID, iDataLinesRead[eJobID], iReadSecs[eJobID], iRunningReaders)
                            logging.processError(p_message='READ ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_stop=True, p_exitCode=6)

                        case shared.E_READ_END:
                            iRunningReaders -= 1
                            iReadingReaders -= 1

                            # readData2 stuffs the threaID in recs
                            if recs is None:
                                iActiveJobsOnThisStream -=1
                                logging.statsPrint('readDataEnd', eJobID, iDataLinesRead[eJobID], iReadSecs[eJobID], iRunningReaders)
                                try:
                                    shared.readP[eJobID].join(timeout=1)
                                except:
                                    pass
                            else:
                                #only print stats on last thead end
                                if iRunningReaders == 0:
                                    iActiveJobsOnThisStream -= 1
                                    logging.statsPrint('readDataEnd', eJobID, iDataLinesRead[eJobID], iReadSecs[eJobID], iRunningReaders)
                                try:
                                    shared.readP[recs].join(timeout=1)
                                except:
                                    pass

                        case shared.E_KEYS_READ_START:
                            with shared.stopWhenKeysEmpty.get_lock():
                                shared.stopWhenKeysEmpty.value = False
                            logging.statsPrint('keysReadStart', p_jobID=eJobID, p_recs=secs, p_secs=0, p_threads=iRunningReaders)

                        case shared.E_KEYS_READ_END:
                            with shared.stopWhenKeysEmpty.get_lock():
                                shared.stopWhenKeysEmpty.value = True
                            logging.statsPrint('keysReadEnd', eJobID, iDataLinesRead[eJobID], iReadSecs[eJobID], iRunningWriters)

                        case shared.E_WRITE_START:
                            pass

                        case shared.E_WRITE_ERROR:
                            logging.statsPrint('writeDataError', eJobID, iTotalDataLinesWritten, -1, iRunningWriters)
                            logging.processError(p_message='WRITE ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_threadID=recs, p_stop=True, p_exitCode=7)

                        case shared.E_WRITE_END:
                            iRunningWriters -= 1
                            try:
                                shared.writeP[eJobID].join(timeout=1)
                            except:
                                pass

                        case shared.E_STOP:
                            setproctitle(f'datacopy: jobManager thread, stop received')
                            bStopRequested = True

                        case shared.E_NOOP:
                            #no operation. just to force the common part of event processing.
                            pass

                        case _:
                            logging.logPrint(f'unknown event in insert loop ({eType}), should not happen!', p_jobID=eJobID)

                #nothing happended the last second, check idle timeout
                except queueEmpty:
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
                                    pass #do not remove as on production mode we comment the previous line

                        if ( iIdleTimeout > 3 and iActiveJobsOnThisStream == 0 ):
                            #exit inner loop
                            break
                    else:
                        #apply the brakes...

                        while shared.dataKeysQueue.qsize() > 0:
                            try:
                                _ = shared.dataKeysQueue.get(block = True, timeout = 1 )
                                _ = None
                                dumpedPackets += 1
                            except queueEmpty:
                                break
                        while not bReadyToStop:
                            try:
                                _ = shared.dataQueue.get(block = True, timeout = 1 )
                                _ = None
                                dumpedPackets += 1
                            except queueEmpty:
                                emptyQueueTimeout -= 1
                                if emptyQueueTimeout == 0:
                                    logging.logPrint(f'stopping, timing out', logLevel.DEBUG, p_jobID=eJobID)
                                    bReadyToStop = True
                                if iRunningReaders == 0:
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
                    if  bKeepGoing and (not bEndOfJobs and not thisJob.bCloseStream and iActiveJobsOnThisStream < shared.parallelReaders and iCurrentQueueSize<shared.usedQueueBeforeNew):
                        if jobID<len(shared.jobs):
                            jobID += 1
                            jobName = shared.getJobName(jobID)
                            iActiveJobsOnThisStream += 1
                            shared.eventQueue.put( (shared.E_BOOT_READER, jobID, None, None ) )
                        else:
                            logging.logPrint('no more jobs, stopping launches', logLevel.DEBUG, p_jobID=jobID)
                            jobID += 1
                            bEndOfJobs = True
                    else:
                        if iActiveJobsOnThisStream == 0 and thisJob.bCloseStream:
                            if shared.dataQueue.qsize() == 0 and shared.eventQueue.qsize() == 0 and shared.stopWhenEmpty.value == False:
                                logging.logPrint('signaling the end of data for this stream.', p_jobID=eJobID)
                                shared.stopWhenEmpty.value = True

                if shared.Working.value:
                    if shared.SCREEN_STATS:
                        statsLine=f'\r{iTotalDataLinesRead:,} recs read ({(iTotalDataLinesRead/iTotalReadSecs):,.2f}/sec x {iReadingReaders}r,{iRunningQueries}q), {iTotalDataLinesWritten:,} recs written ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/sec x {iRunningWriters}), queue len: {iCurrentQueueSize:,}, max queue: {shared.maxQueueLenObserved:,}, timeout timer: {iIdleTimeout:,}, idle time: {shared.idleSecsObserved.value:,}, activeJobs: {iActiveJobsOnThisStream}        '
                        if shared.SCREEN_STATS_TO_STDOUT:
                            print(statsLine, file=sys.stdout, end='', flush = True)
                        else:
                            print(statsLine, file=sys.stderr, end='', flush = True)
                    logging.logPrint(f'reads:{iTotalDataLinesRead:,} ({(iTotalDataLinesRead/iTotalReadSecs):,.2f}/s x {iReadingReaders}r,{iRunningQueries}q); writes:{iTotalDataLinesWritten:,} ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/s x {iRunningWriters}); ql:{iCurrentQueueSize:,}, mq:{shared.maxQueueLenObserved:,}; i:{iIdleTimeout:,}, it:{shared.idleSecsObserved.value:,}, Working={shared.Working.value}, ActiveJobs={iActiveJobsOnThisStream}', logLevel.STATSONPROCNAME)

            logging.statsPrint('queueStats', jobID, shared.maxQueueLenObserved, shared.maxQueueLenObservedEvents, 0)
            print('\n\n', file=sys.stdout, flush = True)
            iEnd = timer()
            iTimeTaken = iEnd - iStart
            logging.logPrint(f'{iTotalDataLinesWritten:,} rows copied in {iTimeTaken:,.2f} seconds ({(iTotalDataLinesWritten/iTimeTaken):,.2f}/sec).')
            logging.statsPrint('writeDataEnd', jobID, iTotalDataLinesWritten, iTotalWrittenSecs, shared.dataQueue.qsize())
            logging.statsPrint('streamEnd', jobID, shared.idleSecsObserved.value, iTimeTaken, 0)
            shared.maxQueueLenObserved = 0
            shared.maxQueueLenObservedEvents = 0
            logging.logPrint(f'end of inner loop, with jobID=[{jobID}]', logLevel.DEBUG)
            jobID += 1

        logging.logPrint(f'end of outer loop, with jobID=[{jobID}]', logLevel.DEBUG)
        setproctitle(f'datacopy: jobManager thread, ended')
        with shared.Working.get_lock():
            shared.Working.value = False

    except Exception as e:
        logging.processError(p_e=e, p_message=f'({jobName}): unexpected exception', p_stack=traceback.format_exc(), p_stop=True, p_exitCode=5)
