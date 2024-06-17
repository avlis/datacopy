'''job sequencer control'''

#pylint: disable=invalid-name, broad-except, line-too-long

import sys
import os
import re
from timeit import default_timer as timer
import multiprocessing as mp

import queue

import modules.logging as logging
import modules.shared as shared
import modules.jobs as jobs
import modules.connections as connections
import modules.datahandlers as datahandlers

def copyData():
    ''' main job loop'''

    iWriters = 0
    iRunningReaders = 0
    iRunningQueries = 0

    tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval

    jobID = 0

    sWriteFileMode = 'w'
    sCSVHeader = ''

    bEndOfJobs = False

    iTotalDataLinesRead = 0
    iDataLinesRead = {}
    iTotalReadSecs = .001

    iRunningWriters = 0
    iTotalDataLinesWritten = 0
    iTotalWrittenSecs = .001

    iReadSecs = {}

    iIdleTimeout = 0

    logging.logPrint(f'copyData: entering jobs loop, max readers allowed: [{shared.parallelReaders}]')

    while jobID < len(shared.queries) and shared.Working.value and not shared.ErrorOccurred.value:
        logging.logPrint(f'entering jobID {jobID}', shared.L_DEBUG)

        iDataLinesRead[jobID] = 0
        iReadSecs[jobID] = .001

        writersNotStartedYet = True

        oMaxAlreadyInsertedData = None

        try:
            (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial, appendKeyColumn, getMaxQuery, getMaxDest) = jobs.prepQuery(jobID)
        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint(f"copyData::OuterPrepQuery({shared.queries['index'][jobID]}): ERROR: [{error}]")

        jobName = f"{shared.queries['index'][jobID]}-{source}-{dest}-{table}"
        logging.openLogFile(dest, table)
        logging.logPrint(jobName, shared.L_STREAM_START)
        logging.logPrint(f"datacopy version [{os.getenv('BASE_VERSION','<unkown>')}][{os.getenv('VERSION','<unkown>')}] starting")

        if not shared.testQueries:
            # cleaning up destination before inserts

            if connections.getConnectionParameter(dest, 'driver') == 'csv':
                if  mode.upper() in ('T','D'):
                    sWriteFileMode='w'
                    logging.logPrint(f'copyData({jobName}): creating new CSV file(s)')
                else:
                    sWriteFileMode='a'

                    logging.logPrint(f'copyData({jobName}): appending to existing CSV file(s)')
            else:
                siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')
                match mode.upper():
                    case 'T' | 'D':
                        cConn = connections.initConnections(dest, False, 1, '', table, 'w')[0]
                        cCleanData = cConn.cursor()
                        match mode.upper():
                            case 'T':
                                logging.logPrint(f'copyData({jobName}): cleaning up table (truncate) [{dest}].[{table}]')
                                cStart = timer()
                                try:
                                    logging.statsPrint('truncateStart', jobName, 0, 0, 0)
                                    cCleanData.execute(f'truncate table {siObjSep}{table}{siObjSep}')
                                    cConn.commit()
                                    logging.statsPrint('truncateEnd', jobName, 0, timer() - cStart, 0)
                                except Exception as error:
                                    logging.logPrint(f'copyData({jobName}): ERROR truncating table: [{error}]')
                                    logging.statsPrint('truncateError', jobName, 0, timer() - cStart, 0)
                                    shared.ErrorOccurred.value=True
                                    logging.closeLogFile(5)
                            case 'D':
                                logging.logPrint(f'copyData({jobName}): cleaning up table (delete) [{dest}].[{table}]')
                                cStart = timer()
                                deletedRows=-1
                                try:
                                    logging.statsPrint('deleteStart', jobName, 0, 0, 0)
                                    deletedRows=cCleanData.execute(f'delete from {siObjSep}{table}{siObjSep}')
                                    cConn.commit()
                                    logging.statsPrint('deleteEnd', jobName, deletedRows, timer() - cStart, 0)
                                except Exception as error:
                                    logging.logPrint(f'copyData({jobName}): ERROR deleting table: [{error}]')
                                    logging.statsPrint('deleteError', jobName, 0, timer() - cStart, 0)
                                    shared.ErrorOccurred.value=True
                                    logging.closeLogFile(5)
                        cCleanData.close()
                        cConn.close()
                    case 'A':
                        if getMaxDest == '':
                            getMaxDest = dest

                        cConn = connections.initConnections(getMaxDest, False, 1, '', table, 'w')[0]
                        cGetMaxID = cConn.cursor()

                        logging.logPrint(f'copyData({jobName}): figuring out max value for [{appendKeyColumn}] on [{getMaxDest}] with [{getMaxQuery}]')
                        cStart = timer()
                        try:
                            logging.statsPrint('getMaxAtDestinationStart', jobName, 0, 0, 0)
                            cGetMaxID.execute(getMaxQuery)
                            oMaxAlreadyInsertedData = cGetMaxID.fetchone()[0]

                            logging.statsPrint('getMaxAtDestinationEnd', jobName, oMaxAlreadyInsertedData, timer() - cStart, 0)
                            logging.logPrint(f'copyData({jobName}): max value  is [{oMaxAlreadyInsertedData}]')
                        except Exception as error:
                            logging.logPrint(f'copyData({jobName}): ERROR getting max value: [{error}]')
                            logging.statsPrint('getMaxAtDestinationError', jobName, 0, timer() - cStart, 0)
                            shared.ErrorOccurred.value=True
                            logging.closeLogFile(5)

                        cGetMaxID.close()
                        cConn.close()

        if 'insert_cols' in shared.queries:
            sOverrideCols = str(shared.queries['insert_cols'][jobID])
        else:
            sOverrideCols = ''

        if 'ignore_cols' in shared.queries:
            tIgnoreCols = (shared.queries['ignore_cols'][jobID]).split(',')
        else:
            tIgnoreCols = ()

        try:
            logging.logPrint(f'copyData({jobName}): entering insert loop...')
            shared.eventStream.put( (shared.E_BOOT_READER, jobID, jobName, 0, 0 ) )

            while ( shared.Working.value and not shared.ErrorOccurred.value and ( iRunningWriters > 0 or iRunningReaders > 0 or shared.eventStream.qsize() > 0 ) ):
                try:
                    eType, threadID, threadName, recs, secs = shared.eventStream.get(block = True, timeout = 1) # pylint: disable=unused-variable
                    #logging.logPrint('\nstreamevent: [{0},{1},{2},{3}]'.format(eType,threadID, recs, secs), shared.L_DEBUG)

                    iIdleTimeout = 0
                    match eType:
                        case shared.E_READ:
                            iDataLinesRead[threadID] += recs
                            iTotalDataLinesRead += recs
                            iReadSecs[threadID] += secs
                            iTotalReadSecs += secs

                        case shared.E_WRITE:
                            iTotalDataLinesWritten += recs
                            iTotalWrittenSecs += secs

                        case shared.E_BOOT_READER:
                            iDataLinesRead[threadID] = 0
                            iReadSecs[threadID] = .001

                            try:
                                (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWritersIgnored, bCloseStream, bCSVEncodeSpecial, appendKeyColumn, getMaxQuery, getMaxDest) = jobs.prepQuery(threadID) #pylint: disable=unused-variable
                            except Exception as error:
                                shared.ErrorOccurred.value = True
                                logging.logPrint(f"copyData::InnerPrepQuery({shared.queries['index'][threadID]}): ERROR: [{error}]")

                            jobName = f"{shared.queries['index'][threadID]}-{source}-{dest}-{table}"

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

                            shared.GetConn[threadID] = connections.initConnections(source, True, 1, preQuerySrc)[0]
                            shared.GetData[threadID] = connections.initCursor(shared.GetConn[threadID], threadID, fetchSize)
                            if source2 != '':
                                shared.GetConn2[threadID] = connections.initConnections(source2, True, 1)[0]
                                shared.GetData2[threadID] = connections.initCursor(shared.GetConn2[threadID], threadID, fetchSize)
                            logging.logPrint(f'copyData({jobName}): starting reading from [{source}] to [{dest}].[{table}], with query:\n***\n{query}\n***')
                            if query2 != '':
                                logging.logPrint(f'copyData({jobName}): and from [{source2}] with query:\n***\n{query2}\n***')
                                shared.readP[threadID]=mp.Process(target=datahandlers.readData2, args = (threadID, jobName, shared.GetConn[threadID], shared.GetConn2[threadID], shared.GetData[threadID], shared.GetData2[threadID], fetchSize, query, query2))
                            else:
                                shared.readP[threadID]=mp.Process(target=datahandlers.readData, args = (threadID, jobName, shared.GetConn[threadID], shared.GetData[threadID], fetchSize, query))
                            shared.readP[threadID].start()
                            iRunningReaders += 1

                        case shared.E_QUERY_START:
                            iRunningQueries += 1
                            logging.statsPrint('execQueryStart', threadName, 0, 0, iRunningQueries)

                        case shared.E_QUERY_END:
                            iRunningQueries -= 1
                            logging.statsPrint('execQueryEnd', threadName, 0, secs, iRunningQueries)

                        case shared.E_READ_START:
                            logging.logPrint(f'copyData({threadName}): received read start message', shared.L_DEBUG)
                            logging.statsPrint('readDataStart', threadName, 0, 0, iRunningReaders)

                            if writersNotStartedYet:
                                iRunningWriters = 0
                                iTotalDataLinesWritten = 0
                                iTotalWrittenSecs = .001                            # only start writers after a sucessful read
                                logging.logPrint(f'copyData({threadName}): writersNotStartedYet, processing cols to prepare insert statement: [{recs}]', shared.L_DEBUG)
                                sColNames = ''
                                sColsPlaceholders = ''

                                workingCols = None

                                match sOverrideCols:
                                    case '' | '@' | '@l' | '@u':
                                        workingCols = recs
                                    case '@d':
                                        # from destination:
                                        cConn = connections.initConnections(dest, False, 1, '', table, 'r')[0]
                                        tdCursor = cConn.cursor()
                                        siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')
                                        tdCursor.execute(f'SELECT * FROM {siObjSep}{query}{siObjSep}  WHERE 1=0')
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

                                logging.logPrint(sColNamesNoQuotes.split(','), shared.L_DUMPCOLS)
                                if connections.getConnectionParameter(dest, 'driver') == 'csv':
                                    logging.logPrint(f'copyData({threadName}): cols for CSV file(s): [{sColNamesNoQuotes}]')
                                    if  mode.upper() in ('T','D'):
                                        sCSVHeader = sColNamesNoQuotes
                                    else:
                                        sCSVHeader = ''

                                else:
                                    logging.logPrint(f'copyData({threadName}): insert query (cols {sIcolType}): [{iQuery}]')

                                if not shared.testQueries:
                                    logging.logPrint(f'copyData({threadName}): number of writers for this job: [{nbrParallelWriters}]')

                                    newWriteConns = connections.initConnections(dest, False, nbrParallelWriters, preQueryDst, table, sWriteFileMode)
                                    shared.stopWhenEmpty.value = False
                                    for x in range(nbrParallelWriters):
                                        shared.PutConn[iWriters] = newWriteConns[x]
                                        if isinstance(newWriteConns[x], tuple):
                                            shared.PutData[iWriters] = None
                                            shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeDataCSV, args = (threadID, threadName, iWriters, shared.PutConn[iWriters], sCSVHeader, bCSVEncodeSpecial) ))
                                            shared.writeP[iWriters].start()
                                        else:
                                            shared.PutData[iWriters] = shared.PutConn[iWriters].cursor()
                                            shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeData, args = (threadID, threadName, iWriters, shared.PutConn[iWriters], shared.PutData[iWriters], iQuery) ))
                                            shared.writeP[iWriters].start()
                                        iWriters += 1
                                        iRunningWriters += 1

                                    writersNotStartedYet = False
                                    logging.statsPrint('writeDataStart', threadName, 0, 0, iRunningWriters)

                        case shared.E_READ_END:
                            iRunningReaders -= 1
                            logging.statsPrint('readDataEnd', threadName, iDataLinesRead[threadID], iReadSecs[threadID], iRunningReaders)
                            try:
                                shared.readP[threadID].join(timeout=1)
                            except:
                                pass

                            if iRunningReaders == 0 and bCloseStream:
                                logging.logPrint(f'copyData({threadName}): signaling the end of data for this stream.')
                                shared.stopWhenEmpty.value = True

                        case shared.E_WRITE_START:
                            pass

                        case shared.E_WRITE_END:
                            iRunningWriters -= 1
                            try:
                                shared.writeP[threadID].join(timeout=1)
                            except:
                                pass

                        case shared.E_NOOP:
                            #no operation. just to force the common part of event processing.
                            pass

                        case _:
                            logging.logPrint(f'copyData({threadName}): unknown event in insert loop ({eType}), should not happen!')

                #nothing happended the last second, check idle timeout
                except queue.Empty:
                    iIdleTimeout += 1
                    shared.idleSecsObserved.value += 1

                    if shared.idleTimetoutSecs > 0 and iIdleTimeout > shared.idleTimetoutSecs:
                        logging.logPrint(f'copyData: ERROR: idle timeout secs [{shared.idleTimetoutSecs}] reached.')
                        logging.statsPrint('IdleTimeoutError', jobName, 0, shared.idleTimetoutSecs, 0)
                        shared.ErrorOccurred.value = True

                #common part of event processing:
                iCurrentQueueSize = shared.dataBuffer.qsize()

                if iCurrentQueueSize > shared.maxQueueLenObserved:
                    shared.maxQueueLenObserved = iCurrentQueueSize

                if iCurrentQueueSize == shared.queueSize:
                    shared.maxQueueLenObservedEvents +=1

                if tParallelReadersNextCheck < timer():
                    tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval
                    if  not bEndOfJobs and not bCloseStream and iRunningReaders < shared.parallelReaders and iCurrentQueueSize<shared.usedQueueBeforeNew:
                        if jobID<len(shared.queries)-1:
                            jobID += 1
                            jobName = f"{shared.queries['index'][jobID]}-{shared.queries['source'][jobID]}-{shared.queries['dest'][jobID]}-{shared.queries['table'][jobID]}"
                            shared.eventStream.put( (shared.E_BOOT_READER, jobID, jobName, 0, 0 ) )
                        else:
                            logging.logPrint('no more jobs, stopping launches', shared.L_DEBUG)
                            jobID += 1
                            bEndOfJobs = True

                if shared.screenStats:
                    print(f'\r{iTotalDataLinesRead:,} recs read ({(iTotalDataLinesRead/iTotalReadSecs):,.2f}/sec x {iRunningReaders}r,{iRunningQueries}q), {iTotalDataLinesWritten:,} recs written ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/sec x {iRunningWriters}), queue len: {iCurrentQueueSize:,}, max queue: {shared.maxQueueLenObserved}, timeout timer: {iIdleTimeout}, idle time: {shared.idleSecsObserved.value}        ', file=shared.screenStatsOutputFile, end='', flush = True)
                logging.logPrint(f'reads:{iTotalDataLinesRead:,} ({(iTotalDataLinesRead/iTotalReadSecs):,.2f}/s x {iRunningReaders}r,{iRunningQueries}q); writes:{iTotalDataLinesWritten:,} ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/s x {iRunningWriters}); ql:{iCurrentQueueSize:,}, mq:{shared.maxQueueLenObserved}; i:{iIdleTimeout}, it:{shared.idleSecsObserved.value}', shared.L_STATSONPROCNAME)
            if shared.ErrorOccurred.value:
                #clean up any remaining data
                shared.stopWhenEmpty.value = True
                while True:
                    try:
                        dummy = shared.dataBuffer.get(block = True, timeout = 1 )
                    except queue.Empty:
                        break


            print('\n', file=sys.stdout, flush = True)
            logging.logPrint(f'copyData({jobName}): {iTotalDataLinesWritten:,} rows copied in {iTotalWrittenSecs:,.2f} seconds ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/sec).')
            logging.statsPrint('writeDataEnd', jobName, iTotalDataLinesWritten, iTotalWrittenSecs, nbrParallelWriters)
            logging.statsPrint('queueStats', jobName, shared.maxQueueLenObserved, shared.maxQueueLenObservedEvents, fetchSize)
            shared.maxQueueLenObserved = 0
            shared.maxQueueLenObservedEvents = 0
            logging.logPrint(jobName, shared.L_STREAM_END)

        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint(f'copyData: ERROR at line ({sys.exc_info()[2].tb_lineno}): [{error}]')
            logging.statsPrint('genericError', jobName, 0, 0, 0)
        finally:
            #if a control-c occurred, also rename file
            if mode == mode.upper() or not shared.Working.value or (shared.stopJobsOnError and shared.ErrorOccurred.value):
                logging.closeLogFile()

        if shared.stopJobsOnError and shared.ErrorOccurred.value:
            break

        shared.ErrorOccurred.value = False

        jobID += 1
