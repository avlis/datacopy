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
        logging.logPrint(f"datacopy version [{os.getenv('VERSION','<unkown>')}] starting")

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
                if  mode.upper() in ('T','D'):
                    cConn = connections.initConnections(dest, False, 1, '', table, 'w')[0]
                    cCleanData = cConn.cursor()
                    if mode.upper() == 'T':
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
                    if mode.upper() == 'D':
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
                if  mode.upper() == 'A':

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

                        logging.statsPrint('getMaxAtDestinationEnd', jobName, 0, timer() - cStart, oMaxAlreadyInsertedData)
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

#shared.E_READ
                    if eType == shared.E_READ:
                        iDataLinesRead[threadID] += recs
                        iTotalDataLinesRead += recs
                        iReadSecs[threadID] += secs
                        iTotalReadSecs += secs
#shared.E_WRITE
                    elif eType == shared.E_WRITE:
                        iTotalDataLinesWritten += recs
                        iTotalWrittenSecs += secs

#shared.E_BOOT_READER
                    elif eType == shared.E_BOOT_READER:
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

#shared.E_QUERY_START
                    elif eType == shared.E_QUERY_START:
                        iRunningQueries += 1
                        logging.statsPrint('execQueryStart', threadName, 0, 0, iRunningQueries)

#shared.E_QUERY_END
                    elif eType == shared.E_QUERY_END:
                        iRunningQueries -= 1
                        logging.statsPrint('execQueryEnd', threadName, 0, secs, iRunningQueries)

#shared.E_READ_START
                    elif eType == shared.E_READ_START:

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

                            if sOverrideCols in ('', '@', '@l', '@u'):
                                workingCols = recs
                            elif sOverrideCols == '@d':
                                # from destination:
                                cConn = connections.initConnections(dest, False, 1, '', table, 'r')[0]
                                tdCursor = cConn.cursor()
                                siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')
                                tdCursor.execute(f'SELECT * FROM {siObjSep}{query}{siObjSep}  WHERE 1=0')
                                workingCols = tdCursor.description
                                cConn.rollback() #somehow, this select blocks truncates on postgres, if not rolled back?...
                                tdCursor.close()
                            else:
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
                            if sOverrideCols == '@d':
                                iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sColNames}) VALUES ({sColsPlaceholders})'
                                sIcolType = 'from destination'
                            elif sOverrideCols == '@l':
                                iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sColNames.lower()}) VALUES ({sColsPlaceholders})'
                                sIcolType = 'from source, lowercase'
                            elif sOverrideCols == '@u':
                                iQuery = f'INSERT INTO {siObjSep}{table}{siObjSep}({sColNames.upper()}) VALUES ({sColsPlaceholders})'
                                sIcolType = 'from source, upercase'
                            elif len(sOverrideCols)>0 and sOverrideCols[0] != '@':
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

                                for x in range(nbrParallelWriters):
                                    shared.PutConn[iWriters] = newWriteConns[x]
                                    if newWriteConns[x].__class__.__name__ == 'writer':
                                        shared.PutData[iWriters] = None
                                        shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeDataCSV, args = (threadID, threadName, iWriters, shared.PutConn[iWriters], sCSVHeader, bCSVEncodeSpecial) ))
                                        shared.writeP[iWriters].start()
                                    else:
                                        shared.PutData[iWriters] = shared.PutConn[iWriters].cursor()
                                        shared.writeP[iWriters] = (mp.Process(target=datahandlers.writeData, args = (threadID, threadName, iWriters, shared.PutConn[iWriters], shared.PutData[iWriters], iQuery) ))
                                        shared.writeP[iWriters].start()
                                    iWriters += 1
                                    iRunningWriters += 1
                                    logging.statsPrint('writeDataStart', threadName, 0, 0, iRunningWriters)
                                writersNotStartedYet = False

#shared.E_READ_END
                    elif eType == shared.E_READ_END:
                        iRunningReaders -= 1
                        logging.statsPrint('readDataEnd', threadName, iDataLinesRead[threadID], iReadSecs[threadID], iRunningReaders)
                        shared.readP[threadID].join(1)

                        if iRunningReaders == 0 and bCloseStream:
                            logging.logPrint(f'copyData({threadName}): signaling the end of data for this stream.')
                            for x in range(nbrParallelWriters):
                                shared.dataBuffer.put( (shared.seqnbr.value, shared.D_EOD, None), block = True )
                                #print('pushed shared.seqnbr {0} (end)'.format(shared.seqnbr.value), file=sys.stderr, flush = True)
                                shared.seqnbr.value += 1
#shared.E_WRITE_START
                    elif eType == shared.E_WRITE_START:
                        pass
#shared.E_WRITE_END
                    elif eType == shared.E_WRITE_END:
                        iRunningWriters -= 1

                except queue.Empty:
                    pass

#common part of event processing:
                if tParallelReadersNextCheck < timer():
                    tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval
                    if  not bEndOfJobs and not bCloseStream and iRunningReaders < shared.parallelReaders and shared.dataBuffer.qsize()<shared.usedQueueBeforeNew:
                        if jobID<len(shared.queries)-1:
                            jobID += 1
                            jobName = f"{shared.queries['index'][jobID]}-{shared.queries['source'][jobID]}-{shared.queries['dest'][jobID]}-{shared.queries['table'][jobID]}"
                            shared.eventStream.put( (shared.E_BOOT_READER, jobID, jobName, 0, 0 ) )
                        else:
                            logging.logPrint('no more jobs, stopping launches', shared.L_DEBUG)
                            jobID += 1
                            bEndOfJobs = True

                if shared.screenStats:
                    print(f'\r{iTotalDataLinesRead:,} records read ({(iTotalDataLinesRead/iTotalReadSecs):,.2f}/sec x {iRunningReaders}r,{iRunningQueries}q), {iTotalDataLinesWritten:,} records written ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/sec x {iRunningWriters}), data queue len: {shared.dataBuffer.qsize():,}       ', file=sys.stdout, end='', flush = True)

            if shared.ErrorOccurred.value:
                #clean up any remaining data
                while True:
                    try:
                        dummy=shared.dataBuffer.get(block = True, timeout = 1 )
                    except queue.Empty:
                        break
                for x in range(nbrParallelWriters):
                    shared.dataBuffer.put( (-3, shared.D_EOD, None) )

            print('\n', file=sys.stdout, flush = True)
            logging.logPrint(f'copyData({jobName}): {iTotalDataLinesWritten:,} rows copied in {iTotalWrittenSecs:,.2f} seconds ({(iTotalDataLinesWritten/iTotalWrittenSecs):,.2f}/sec).')
            logging.statsPrint('writeDataEnd', jobName, iTotalDataLinesWritten, iTotalWrittenSecs, nbrParallelWriters)
            logging.logPrint(jobName, shared.L_STREAM_END)

        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint(f'copyData: ERROR at line ({sys.exc_info()[2].tb_lineno}): [{error}]')
            logging.statsPrint('ERROR', jobName, 0, 0, 0)
        finally:
            #if a control-c occurred, also rename file
            if mode == mode.upper() or not shared.Working.value or (shared.stopJobsOnError and shared.ErrorOccurred.value):
                logging.closeLogFile()

        if shared.stopJobsOnError and shared.ErrorOccurred.value:
            break

        shared.ErrorOccurred.value = False

        jobID += 1

    #pylint: disable=consider-using-dict-items
    for i in shared.readP:
        shared.readP[i].terminate()
    for i in shared.writeP:
        shared.writeP[i].terminate()
