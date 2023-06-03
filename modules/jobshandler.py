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
                    sCSVHeader = sColNamesNoQuotes
                    logging.logPrint("copyData({0}): creating new CSV file(s)".format(jobName))
                else:
                    sWriteFileMode='a'
                    sCSVHeader = ''
                    logging.logPrint("copyData({0}): appending to existing CSV file(s)".format(jobName))
            else:
                if  mode.upper() in ('T','D'):
                    cConn = connections.initConnections(dest, False, 1, '', table, 'w')[0]
                    cCleanData = cConn.cursor()
                    if mode.upper() == 'T':
                        logging.logPrint("copyData({0}): cleaning up table (truncate) [{1}].[{2}]".format(jobName, dest,table))
                        cStart = timer()
                        cCleanData.execute("truncate table {0}".format(table))
                        cConn.commit()
                        logging.statsPrint('truncateTable', jobName, 0, timer() - cStart, 0)
                    if mode.upper() == 'D':
                        logging.logPrint("copyData({0}): cleaning up table (delete) [{1}].[{2}]".format(jobName, dest, table))
                        cStart = timer()
                        deletedRows=-1
                        deletedRows=cCleanData.execute("delete from {0}".format(table))
                        cConn.commit()
                        logging.statsPrint('deleteTable', jobName, deletedRows, timer() - cStart, 0)
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
            shared.GetConn[jobID] = connections.initConnections(source, True, 1, preQuerySrc)[0]
            if source2 != '':
                shared.GetConn2[jobID] = connections.initConnections(source2, True, 1)[0]

            isSelect = re.search('(^|[ \t\n]+)SELECT[ \t\n]+', query.upper())
            if not isSelect:
                query = "SELECT * FROM {0}".format(query)

            logging.logPrint("copyData({0}): running source query: [{1}]".format(jobName, query))

            tStart = timer()
            shared.GetData[jobID] = connections.initCursor(shared.GetConn[jobID], jobID, fetchSize)

            shared.GetData[jobID].execute(query)
            logging.logPrint("copyData({0}): source query took {1:.2f} seconds to reply.".format(jobName, (timer() - tStart)))
            logging.statsPrint('execQuery', jobName, 0, timer() - tStart, 0)

            if source2 != '':
                shared.GetData2[jobID] = connections.initCursor(shared.GetConn2[jobID], jobID, fetchSize)

            logging.logPrint("copyData({0}): starting reading from [{1}] to [{2}].[{3}], with query:\n***\n{4}\n***".format(jobName, source, dest, table, query))
            if query2 != '':
                logging.logPrint("copyData({0}): and from [{1}] with query:\n***\n{2}\n***".format(jobName, source2, query2))
                shared.readP[jobID]=mp.Process(target=datahandlers.readData2, args = (jobID, jobName, shared.GetConn[jobID], shared.GetConn2[jobID], shared.GetData[jobID], shared.GetData2[jobID], fetchSize, None, query2))
            else:
                shared.readP[jobID]=mp.Process(target=datahandlers.readData, args = (jobID, jobName, shared.GetConn[jobID], shared.GetData[jobID], fetchSize, None))
            shared.readP[jobID].start()
            iRunningReaders += 1

            iRunningWriters = 0

            logging.logPrint("copyData({0}): entering insert loop...".format(jobName))

            while ( shared.Working.value and not shared.ErrorOccurred.value and ( iRunningWriters > 0 or iRunningReaders > 0 or shared.eventStream.qsize() > 0 ) ):
                try:
                    eType, threadID, threadName, recs, secs = shared.eventStream.get(block=True,timeout = 1) # pylint: disable=unused-variable
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
#shared.E_READ_START
                    elif eType == shared.E_READ_START:

                        logging.logPrint("copyData({0}): received read start message".format(threadName), shared.L_DEBUG)
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
                                writersNotStartedYet = False

#shared.E_READ_END
                    elif eType == shared.E_READ_END:
                        logging.statsPrint('readData', threadName, iDataLinesRead[threadID], iReadSecs[threadID], iRunningReaders)
                        iRunningReaders -= 1

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

#common part of event processing:
                    if not bEndOfJobs and not bCloseStream and iRunningReaders < shared.parallelReaders and shared.dataBuffer.qsize()<shared.usedQueueBeforeNew:
                        if jobID<len(shared.queries)-1:
                            jobID += 1
                            jobName = '{0}-{1}-{2}-{3}'.format(shared.queries["index"][jobID], shared.queries["source"][jobID], shared.queries["dest"][jobID], shared.queries["table"][jobID])
                            iDataLinesRead[jobID] = 0
                            iReadSecs[jobID] = .001
                            try:
                                (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWritersIgnored, bCloseStream, bCSVEncodeSpecial) = jobs.prepQuery(jobID) #pylint: disable=unused-variable
                            except Exception as error:
                                shared.ErrorOccurred.value = True
                                logging.logPrint("copyData::InnerPrepQuery({0}): ERROR: [{1}]".format(shared.queries["index"][jobID], error))

                            jobName = '{0}-{1}-{2}-{3}'.format(shared.queries["index"][jobID], source, dest, table)
                            isSelect = re.search('(^|[ \t\n]+)SELECT[ \t\n]+', query.upper())
                            if not isSelect:
                                query="SELECT * FROM {0}".format(query)

                            shared.GetConn[jobID] = connections.initConnections(source, True, 1, preQuerySrc)[0]
                            shared.GetData[jobID] = connections.initCursor(shared.GetConn[jobID], jobID, fetchSize)
                            if source2 != '':
                                shared.GetConn2[jobID] = connections.initConnections(source2, True, 1)[0]
                                shared.GetData2[jobID] = connections.initCursor(shared.GetConn2[jobID], jobID, fetchSize)
                            logging.logPrint("copyData({0}): starting reading from [{1}] to [{2}].[{3}], with query:\n***\n{4}\n***".format(jobName, source, dest, table,query))
                            if query2 != '':
                                logging.logPrint("copyData({0}): and from [{1}] with query:\n***\n{2}\n***".format(jobName, source2, query2))
                                shared.readP[jobID]=mp.Process(target=datahandlers.readData2, args = (jobID, jobName, shared.GetConn[jobID], shared.GetConn2[jobID], shared.GetData[jobID], shared.GetData2[jobID], fetchSize, query, query2))
                            else:
                                shared.readP[jobID]=mp.Process(target=datahandlers.readData, args = (jobID, jobName, shared.GetConn[jobID], shared.GetData[jobID], fetchSize, query))
                            shared.readP[jobID].start()
                            iRunningReaders += 1
                        else:
                            logging.logPrint("no more jobs, stopping launches", shared.L_DEBUG)
                            jobID += 1
                            bEndOfJobs = True

                except queue.Empty:
                    pass

                if shared.screenStats:
                    print("\r{0:,} records read ({1:.2f}/sec x {2}), {3:,} records written ({4:.2f}/sec x {5}), data queue len: {6}       ".format(iTotalDataLinesRead, (iTotalDataLinesRead/iTotalReadSecs), iRunningReaders, iTotalDataLinesWritten, (iTotalDataLinesWritten/iTotalWrittenSecs), iRunningWriters, shared.dataBuffer.qsize()), file=sys.stdout, end='', flush = True) #pylint: disable=line-too-long

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
            logging.statsPrint('writeData', jobName, iTotalDataLinesWritten, iTotalWrittenSecs, nbrParallelWriters)
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
