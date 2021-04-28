class CopyJob():
    def __init__(self):
        None   

class CopyJobs:
    def __init__(self, p_connections,p_queries):
        global g_Working
        global g_fetchSize
        global g_defaultFetchSize
        global g_ErrorOccurred

        for i in range(0,len(p_queries)):
            qIndex=i+1
            g_ErrorOccurred = False
            source=p_queries["source"][i]
            if source=="" or source[0]=="#":
                continue
            dest=p_queries["dest"][i]
            mode=p_queries["mode"][i]
            query=p_queries["query"][i]
            table=p_queries["table"][i]
            if "fetch_size" in p_queries:
                qFetchSize=int(p_queries["fetch_size"][i])
                if qFetchSize == 0:
                    g_fetchSize=g_defaultFetchSize
                else:
                    g_fetchSize=qFetchSize

            sLogFile="{0}.{1}.running.log".format(dest,table)
            fLogFile=open(sLogFile,'a')

            logPrint("copyData({0}): starting copy from [{1}] to [{2}].[{3}], with query:[{4}]".format(qIndex, source, dest, table,query),fLogFile)

            try:
                bErrorOccurred=False
                iTotalDataLines = 0

                if mode.upper()=='T':
                    logPrint("copyData({0}): cleaning up table (truncate) [{1}].[{2}]".format(qIndex, dest,table) ,fLogFile)
                    cCleanData = p_connections[dest].cursor()
                    cCleanData.execute("truncate table {0}".format(table))
                    cCleanData.close()

                if mode.upper()=='D':
                    logPrint("copyData({0}): cleaning up table (delete) [{1}].[{2}]".format(qIndex, dest, table) ,fLogFile)
                    cCleanData = p_connections[dest].cursor()
                    cCleanData.execute("delete from {0}".format(table))
                    cCleanData.close()

                cGetData = p_connections[source].cursor()
                cPutData = p_connections[dest].cursor()

                logPrint("copyData({0}): running source query...".format(qIndex), fLogFile)

                tStart=timer()
                cGetData.execute(query)
                logPrint("copyData({0}): source query took {1:.2f} seconds to reply.".format(qIndex, (timer() - tStart)), fLogFile)

                iCols = 0
                sColsPlaceholders = ''
                sColNames=''
                for col in cGetData.description:

                    iCols = iCols+1
                    sColNames = sColNames + '"{0}",'.format(col[0])
                    sColsPlaceholders = sColsPlaceholders + "%s,"

                sColNames = sColNames[:-1]
                sColsPlaceholders = sColsPlaceholders[:-1]

                iQuery=''
                if "insert_cols" in p_queries:
                    sOverrideCols=str(p_queries["insert_cols"][i])
                    if sOverrideCols != '':
                        if sOverrideCols == '@':
                            iQuery = "INSERT INTO {0} VALUES ({1})".format(table,sColsPlaceholders)
                            sIcolType="from destination"
                        elif sOverrideCols == '@l':
                            iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.lower(),sColsPlaceholders)
                            sIcolType="from source, lowercase"
                        elif sOverrideCols == '@u':
                            iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames.upper(),sColsPlaceholders)
                            sIcolType="from source, upercase"
                        elif sOverrideCols != 'nan':
                            iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sOverrideCols,sColsPlaceholders)
                            sIcolType="overridden"
                if iQuery=='':
                    iQuery = "INSERT INTO {0}({1}) VALUES ({2})".format(table,sColNames,sColsPlaceholders)
                    sIcolType="from source"

                logPrint("copyData({0}): insert query (cols={1}): [{2}]".format(qIndex, sIcolType, iQuery) ,fLogFile)

                iTotalDataLinesRead = 0
                iTotalDataLinesWritten = 0

                bFetchRead = True
                bFinishedRead = False
                bFinishedWrite = False

                iTotalReadSecs=.001
                iTotalWrittenSecs=.001

                logPrint("copyData({0}): entering insert loop...".format(qIndex),fLogFile)

                while True:
                    try:
                        dummy=g_dataBuffer.get(block=False,timeout=1)
                    except queue.Empty:
                        break
                g_readT=threading.Thread(target=readData, args=(qIndex, p_connections[source], cGetData))
                g_readT.start()
                sleep(2)
                g_writeT=threading.Thread(target=writeData, args=(qIndex, p_connections[dest], cPutData, iQuery))
                g_writeT.start()
                print("", flush=True)
                while g_Working:
                    try:
                        if bFetchRead:
                            bFetchRead=False
                            recRead,readSecs = g_readRecords.get(block=False,timeout=1)
                            if not recRead:
                                bFinishedRead=True
                            else:
                                iTotalDataLinesRead = recRead
                                iTotalReadSecs += readSecs
                        else:
                            bFetchRead=True
                            recWritten,writeSecs = g_writtenRecords.get(block=False,timeout=1)
                            if not recWritten:
                                bFinishedWrite=True
                            else:
                                iTotalDataLinesWritten += recWritten
                                iTotalWrittenSecs += writeSecs
                    except queue.Empty:
                        continue

                    print("\r{0} records read ({1:.2f}/sec), {2} records written ({3:.2f}/sec)       ".format(iTotalDataLinesRead, (iTotalDataLinesRead/iTotalReadSecs), iTotalDataLinesWritten, (iTotalDataLinesWritten/iTotalWrittenSecs)),end='', flush=True)
                    if bFinishedRead and bFinishedWrite:
                        break

                cPutData.close()
                cGetData.close()

                print ("", flush=True)
                logPrint("copyData({0}): {1} rows copied in {2:.2f} seconds ({3:.2f}/sec).".format(qIndex, iTotalDataLinesWritten, (timer() - tStart), (iTotalDataLinesWritten/iTotalWrittenSecs)), fLogFile)
            except (Exception) as error:
                g_ErrorOccurred=True
                logPrint("copyData({0}): ERROR: [{1}]".format(qIndex, error), fLogFile)
            finally:
                fLogFile.close()
                if g_ErrorOccurred:
                    sLogFileFinalName = "{0}.{1}.ERROR.log".format(dest,table)
                else:
                    sLogFileFinalName = "{0}.{1}.ok.log".format(dest,table)
                if bErrorOccurred or mode==mode.upper():
                    os.rename(sLogFile, sLogFileFinalName)
            if not g_Working:
                break
