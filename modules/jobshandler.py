'''job sequencer control'''

#pylint: disable=invalid-name,broad-except

import sys
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

    iParallelReadersEventIntervalCount = shared.parallelReadersEventInterval

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

    logging.logPrint("copyData: entering jobs loop, max readers allowed: [{0}]".format(shared.parallelReaders))

    while jobID < len(shared.queries) and shared.Working.value and not shared.ErrorOccurred.value:
        logging.logPrint("entering jobID {0}".format(jobID), shared.L_DEBUG)

        iDataLinesRead[jobID] = 0
        iReadSecs[jobID] = .001

        writersNotStartedYet = True

        try:
            (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial) = jobs.prepQuery(jobID)
        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint("copyData::OuterPrepQuery({0}): ERROR: [{1}]".format(shared.queries["index"][jobID], error))

        jobName = '{0}-{1}-{2}-{3}'.format(shared.queries["index"][jobID], source, dest, table)
        logging.openLogFile(dest, table)
        logging.logPrint(jobName, shared.L_STREAM_START)

        if not shared.testQueries:
            # cleaning up destination before inserts

            if connections.getConnectionParameter(dest, 'driver') == 'csv':
                if  mode.upper() in ('T','D'):
                    sWriteFileMode='w'                 
                    logging.logPrint("copyData({0}): creating new CSV file(s)".format(jobName))
                else:
                    sWriteFileMode='a'
                    
                    logging.logPrint("copyData({0}): appending to existing CSV file(s)".format(jobName))
            else:
                if  mode.upper() in ('T','D'):
                    cConn = connections.initConnections(dest, False, 1, '', table, 'w')[0]
                    cCleanData = cConn.cursor()
                    if mode.upper() == 'T':
                        logging.logPrint("copyData({0}): cleaning up table (truncate) [{1}].[{2}]".format(jobName, dest,table))
                        cStart = timer()
                        try:
                            logging.statsPrint('truncateStart', jobName, 0, 0, 0)
                            cCleanData.execute("truncate table {0}".format(table))
                            cConn.commit()
                            logging.statsPrint('truncateEnd', jobName, 0, timer() - cStart, 0)
                        except Exception as error:
                            logging.logPrint("copyData({0}): ERROR truncating table: [{1}]".format(jobName, error))
                            logging.statsPrint('truncateError', jobName, 0, timer() - cStart, 0)
                            shared.ErrorOccurred.value=True
                            logging.closeLogFile(5)
                    if mode.upper() == 'D':
                        logging.logPrint("copyData({0}): cleaning up table (delete) [{1}].[{2}]".format(jobName, dest, table))
                        cStart = timer()
                        deletedRows=-1
                        try:
                            logging.statsPrint('deleteStart', jobName, 0, 0, 0)
                            deletedRows=cCleanData.execute("delete from {0}".format(table))
                            cConn.commit()
                            logging.statsPrint('deleteEnd', jobName, deletedRows, timer() - cStart, 0)
                        except Exception as error:
                            logging.logPrint("copyData({0}): ERROR deleting table: [{1}]".format(jobName, error))
                            logging.statsPrint('deleteError', jobName, 0, timer() - cStart, 0)
                            shared.ErrorOccurred.value=True
                            logging.closeLogFile(5)
                    cCleanData.close()
                    cConn.close()

        if "insert_cols" in shared.queries:
            sOverrideCols = str(shared.queries["insert_cols"][jobID])
        else:
            sOverrideCols = ''

        if "ignore_cols" in shared.queries:
            tIgnoreCols = (shared.queries["ignore_cols"][jobID]).split(',')
        else:
            tIgnoreCols = ()

        try:
            logging.logPrint("copyData({0}): entering insert loop...".format(jobName))
            shared.eventStream.put( (shared.E_BOOT_READER, jobID, jobName, 0, 0 ) )

            while ( shared.Working.value and not shared.ErrorOccurred.value and ( iRunningWriters > 0 or iRunningReaders > 0 or shared.eventStream.qsize() > 0 ) ):
                try:
                    eType, threadID, threadName, recs, secs = shared.eventStream.get(block = True, timeout = 1) # pylint: disable=unused-variable
                    #logging.logPrint("\nstreamevent: [{0},{1},{2},{3}]".format(eType,threadID, recs, secs), shared.L_DEBUG)

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
                            (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWritersIgnored, bCloseStream, bCSVEncodeSpecial) = jobs.prepQuery(threadID) #pylint: disable=unused-variable
                        except Exception as error:
                            shared.ErrorOccurred.value = True
                            logging.logPrint("copyData::InnerPrepQuery({0}): ERROR: [{1}]".format(shared.queries["index"][threadID], error))

                        jobName = '{0}-{1}-{2}-{3}'.format(shared.queries["index"][threadID], source, dest, table)
                        isSelect = re.search('(^|[ \t\n]+)SELECT[ \t\n]+', query.upper())
                        if not isSelect:
                            query="SELECT * FROM {0}".format(query)

                        shared.GetConn[threadID] = connections.initConnections(source, True, 1, preQuerySrc)[0]
                        shared.GetData[threadID] = connections.initCursor(shared.GetConn[threadID], threadID, fetchSize)
                        if source2 != '':
                            shared.GetConn2[threadID] = connections.initConnections(source2, True, 1)[0]
                            shared.GetData2[threadID] = connections.initCursor(shared.GetConn2[threadID], threadID, fetchSize)
                        logging.logPrint("copyData({0}): starting reading from [{1}] to [{2}].[{3}], with query:\n***\n{4}\n***".format(jobName, source, dest, table,query))
                        if query2 != '':
                            logging.logPrint("copyData({0}): and from [{1}] with query:\n***\n{2}\n***".format(jobName, source2, query2))
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

                        logging.logPrint("copyData({0}): received read start message".format(threadName), shared.L_DEBUG)
                        logging.statsPrint('readDataStart', threadName, 0, 0, iRunningReaders)

                        if writersNotStartedYet:
                            iRunningWriters = 0
                            iTotalDataLinesWritten = 0
                            iTotalWrittenSecs = .001                            # only start writers after a sucessful read
                            logging.logPrint("copyData({0}): writersNotStartedYet, processing cols to prepare insert statement: [{1}]".format(threadName, recs), shared.L_DEBUG)
                            sColNames = ''
                            sColsPlaceholders = ''

                            workingCols = None

                            if sOverrideCols in ('', '@', '@l', '@u'):
                                workingCols = recs
                            elif sOverrideCols == '@d':
                                # from destination:
                                cConn = connections.initConnections(dest, False, 1, '', table, 'r')[0]
                                tdCursor = cConn.cursor()
                                tdCursor.execute("SELECT * FROM {0} WHERE 1=0".format(table))
                                workingCols = tdCursor.description
                                cConn.rollback() #somehow, this select blocks truncates on postgres, if not rolled back?...
                                tdCursor.close()
                            else:
                                workingCols = []
                                for col in sOverrideCols.split(','):
                                    workingCols.append( (col,'dummy') )

                            sIP = connections.getConnectionParameter(dest, "insert_placeholder")
                            for col in workingCols:
                                if col[0] not in tIgnoreCols:
                                    sColNames = '{0}"{1}",'.format(sColNames, col[0])
                                    sColsPlaceholders = sColsPlaceholders + "{0},".format(sIP)
                            sColNames = sColNames[:-1]
                            sColsPlaceholders = sColsPlaceholders[:-1]

                            iQuery = ''

                            if sOverrideCols == '@d':
                                iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames,sColsPlaceholders)
                                sIcolType = "from destination"
                            elif sOverrideCols == '@l':
                                iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.lower(),sColsPlaceholders)
                                sIcolType = "from source, lowercase"
                            elif sOverrideCols == '@u':
                                iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.upper(),sColsPlaceholders)
                                sIcolType = "from source, upercase"
                            elif len(sOverrideCols)>0 and sOverrideCols[0] != '@':
                                iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sOverrideCols,sColsPlaceholders)
                                sIcolType = "overridden"
                            else:
                                iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames,sColsPlaceholders)
                                sIcolType = "from source"

                            sColNamesNoQuotes = sColNames.replace('"','')                      

                            logging.logPrint(sColNamesNoQuotes.split(','), shared.L_DUMPCOLS)
                            if connections.getConnectionParameter(dest, 'driver') == 'csv':
                                logging.logPrint("copyData({0}): cols for CSV file(s): [{1}]".format(threadName, sColNamesNoQuotes))
                                if  mode.upper() in ('T','D'):
                                    sCSVHeader = sColNamesNoQuotes
                                else:
                                    sCSVHeader = ''
                                
                            else:
                                logging.logPrint("copyData({0}): insert query (cols {1}): [{2}]".format(threadName, sIcolType, iQuery))

                            if not shared.testQueries:
                                logging.logPrint("copyData({0}): number of writers for this job: [{1}]".format(threadName, nbrParallelWriters))

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
                            logging.logPrint("copyData({0}): signaling the end of data for this stream.".format(threadName))
                            for x in range(nbrParallelWriters):
                                shared.dataBuffer.put( (shared.seqnbr.value, shared.D_EOD, None), block = True )
                                #print("pushed shared.seqnbr {0} (end)".format(shared.seqnbr.value), file=sys.stderr, flush = True)
                                shared.seqnbr.value += 1
#shared.E_WRITE_START
                    elif eType == shared.E_WRITE_START:
                        pass
#shared.E_WRITE_END
                    elif eType == shared.E_WRITE_END:
                        iRunningWriters -= 1
                        shared.writeP[threadID].join(1)

                except queue.Empty:
                    pass

#common part of event processing:
                if iParallelReadersEventIntervalCount == 0:
                    iParallelReadersEventIntervalCount = shared.parallelReadersEventInterval
                    if  not bEndOfJobs and not bCloseStream and iRunningReaders < shared.parallelReaders and shared.dataBuffer.qsize()<shared.usedQueueBeforeNew:
                        if jobID<len(shared.queries)-1:
                            jobID += 1
                            jobName = '{0}-{1}-{2}-{3}'.format(shared.queries["index"][jobID], shared.queries["source"][jobID], shared.queries["dest"][jobID], shared.queries["table"][jobID])
                            shared.eventStream.put( (shared.E_BOOT_READER, jobID, jobName, 0, 0 ) )
                        else:
                            logging.logPrint("no more jobs, stopping launches", shared.L_DEBUG)
                            jobID += 1
                            bEndOfJobs = True
                else:
                    iParallelReadersEventIntervalCount -= 1

                if shared.screenStats:
                    print("\r{0:,} records read ({1:.2f}/sec x {2}r,{3}q), {4:,} records written ({5:.2f}/sec x {6}), data queue len: {7}       ".format(iTotalDataLinesRead, (iTotalDataLinesRead/iTotalReadSecs), iRunningReaders, iRunningQueries, iTotalDataLinesWritten, (iTotalDataLinesWritten/iTotalWrittenSecs), iRunningWriters, shared.dataBuffer.qsize()), file=sys.stdout, end='', flush = True) #pylint: disable=line-too-long

            if shared.ErrorOccurred.value:
                #clean up any remaining data
                while True:
                    try:
                        dummy=shared.dataBuffer.get(block = True, timeout = 1 )
                    except queue.Empty:
                        break
                for x in range(nbrParallelWriters):
                    shared.dataBuffer.put( (-3, shared.D_EOD, None) )

            print("\n", file=sys.stdout, flush = True)
            logging.logPrint("copyData({0}): {1:,} rows copied in {2:.2f} seconds ({3:.2f}/sec).".format(jobName, iTotalDataLinesWritten, iTotalWrittenSecs, (iTotalDataLinesWritten/iTotalWrittenSecs)))
            logging.statsPrint('writeDataEnd', jobName, iTotalDataLinesWritten, iTotalWrittenSecs, nbrParallelWriters)
            logging.logPrint(jobName, shared.L_STREAM_END)

        except Exception as error:
            shared.ErrorOccurred.value = True
            logging.logPrint("copyData: ERROR at line ({1}): [{0}]".format(error, sys.exc_info()[2].tb_lineno))
            logging.statsPrint('ERROR', jobName, 0, 0, 0)
        finally:
            #if a control-c occurred, also rename file
            if mode == mode.upper() or not shared.Working.value or (shared.stopJobsOnError and shared.ErrorOccurred.value):
                logging.closeLogFile()

        if shared.stopJobsOnError and shared.ErrorOccurred.value:
            break

        shared.ErrorOccurred.value = False

        jobID += 1

    for i in shared.readP:
        shared.readP[i].terminate()
    for i in shared.writeP:
        shared.writeP[i].terminate()
