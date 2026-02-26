'''reading related event handlers for jobManager'''
import multiprocessing as mp
from timeit import default_timer as timer
import modules.shared as shared
import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.connections as connections
import modules.datahandlers.bigquery as datahandlers_bigquery
import modules.datahandlers.relational as datahandlers_relational
import modules.datahandlers.csv as datahandlers_csv

def handle_query_start(eJobID, recs, secs, context):
    pass # Placeholder if needed

def handle_query_end(eJobID, recs, secs, context):
    context.iRunningQueries -= 1
    logging.statsPrint('execQueryEnd', eJobID, context.iActiveJobsOnThisStream, secs, context.iRunningQueries)

def handle_keys_query_end(eJobID, recs, secs, context):
    context.iRunningQueries -= 1
    logging.statsPrint('execKeysQueryEnd', eJobID, 0, secs, context.iRunningQueries)

def handle_detail_query_end(eJobID, recs, secs, context):
    context.iRunningQueries -= 1
    context.iDetailsQueriesSecs[eJobID] += secs

def handle_read_start(eJobID, recs, secs, context):
    context.iReadingReaders += 1
    logging.statsPrint('readDataStart', p_jobID=eJobID, p_recs=secs, p_secs=0, p_threads=context.iRunningReaders)

    # writers setup
    if context.writersNotStartedYet and shared.Working.value:
        context.iRunningWriters = 0
        context.iTotalDataLinesWritten = 0
        context.fTotalWrittenSecs = .001
        logging.logPrint(f'writersNotStartedYet, processing cols to prepare insert statement: [{recs}]', logLevel.DEBUG, p_jobID=eJobID)
        sColNames = ''
        sColsPlaceholders = ''
        insertQuery = ''

        if not shared.GENERATE_CREATE_TABLES:
            workingCols = None

            match context.thisJob.overrideCols:
                case '' | '@' | '@l' | '@u':
                    workingCols = recs
                case '@d':
                    # from destination:
                    newConns = connections.initConnections(context.thisJob.dest, False, 1, context.thisJob.table, 'r')
                    if newConns is not None:
                        cConn = newConns[0]
                        siObjSep = connections.getConnectionParameter(context.thisJob.dest, 'insert_object_delimiter')
                        if context.thisJob.destDriver == 'bigquery':
                            workingCols = datahandlers_bigquery.calcColumnsFromDestination(eJobID, cConn, context.thisJob.table, siObjSep)
                        else:
                            workingCols = datahandlers_relational.calcColumnsFromDestination(eJobID, cConn, context.thisJob.table, siObjSep)
                        cConn.close()
                    else:
                        # Error occurred, but we are inside an event handler.
                        # We might need a way to signal the main loop to break.
                        context.bKeepGoing = False
                        return
                case _:
                    workingCols = []
                    for col in context.thisJob.overrideCols.split(','):
                        workingCols.append( (col,'dummy') )

            sIP = connections.getConnectionParameter(context.thisJob.dest, 'insert_placeholder')
            siObjSep = connections.getConnectionParameter(context.thisJob.dest, 'insert_object_delimiter')

            for col in workingCols:
                if col[0] not in context.thisJob.ignoreCols:
                    sColNames = f'{sColNames}{siObjSep}{col[0]}{siObjSep},'
                    sColsPlaceholders = f'{sColsPlaceholders}{sIP},'
            sColNames = sColNames[:-1]
            sColsPlaceholders = sColsPlaceholders[:-1]

            insertQuery = ''
            match context.thisJob.overrideCols:
                case '@d':
                    insertQuery = f'INSERT INTO {siObjSep}{context.thisJob.table}{siObjSep}({sColNames}) VALUES ({sColsPlaceholders})'
                    sIcolType = 'from destination'
                case '@l':
                    insertQuery = f'INSERT INTO {siObjSep}{context.thisJob.table}{siObjSep}({sColNames.lower()}) VALUES ({sColsPlaceholders})'
                    sIcolType = 'from source, lowercase'
                case '@u':
                    insertQuery = f'INSERT INTO {siObjSep}{context.thisJob.table}{siObjSep}({sColNames.upper()}) VALUES ({sColsPlaceholders})'
                    sIcolType = 'from source, upercase'
                case _:
                    if len(context.thisJob.overrideCols)>0 and context.thisJob.overrideCols[0] != '@':
                        insertQuery = f'INSERT INTO {siObjSep}{context.thisJob.table}{siObjSep}({context.thisJob.overrideCols}) VALUES ({sColsPlaceholders})'
                        sIcolType = 'overridden'
                    else:
                        insertQuery = f'INSERT INTO {siObjSep}{context.thisJob.table}{siObjSep}({sColNames}) VALUES ({sColsPlaceholders})'
                        sIcolType = 'from source'

            sColNamesNoQuotes = sColNames.replace(f'{siObjSep}','')

            logging.logPrint(sColNamesNoQuotes.split(','), logLevel.DUMP_COLS)
            if connections.getConnectionParameter(context.thisJob.dest, 'driver') == 'csv':
                logging.logPrint(f'cols for CSV file(s): [{sColNamesNoQuotes}]', p_jobID=eJobID)
                sCSVHeader = sColNamesNoQuotes if context.thisJob.mode.upper() in ('T','D') else ''
                context.sCSVHeader = sCSVHeader
            else:
                logging.logPrint(f'insert query (cols {sIcolType}): [{insertQuery}]', p_jobID=eJobID)
        else:
            # AI generation
            import modules.ai as ai
            create_statement = ai.generate_create_table(
                p_jobID=eJobID,
                p_description=recs,
                p_tablename=context.thisJob.table,
                p_source_db_name = connections.database_name_for_llm[ context.thisJob.sourceDriver ],
                p_target_db_name = connections.database_name_for_llm[ context.thisJob.destDriver ]
            )
            logging.logPrint(f'create statement generated by LLM:[\n{create_statement}\n]', p_jobID=eJobID)
            sColNamesNoQuotes = '' # Not needed in this branch

        if not shared.TEST_QUERIES:
            logging.logPrint(f'number of writers for this job: [{context.thisJob.nbrParallelWriters}]', p_jobID=eJobID)
            # Replaced with sWriteFileMode from context
            newWriteConns = connections.initConnections(context.thisJob.dest, False, context.thisJob.nbrParallelWriters, context.thisJob.table, context.sWriteFileMode)
            if newWriteConns is None:
                logging.processError(p_message='InitConnections returned None, giving up', p_stop=True)
            else:
                with shared.stopWhenEmpty.get_lock():
                    shared.stopWhenEmpty.value = False
                for x in range(context.thisJob.nbrParallelWriters):
                    shared.PutConn[context.iWriters] = newWriteConns[x]
                    if isinstance(newWriteConns[x], tuple):
                        shared.PutData[context.iWriters] = None
                        shared.writeP[context.iWriters] = (mp.Process(target=datahandlers_csv.writeDataCSV, args = (eJobID, context.iWriters, shared.PutConn[context.iWriters], context.sCSVHeader, context.thisJob.bCSVEncodeSpecial) ))
                        shared.writeP[context.iWriters].start()
                    elif connections.getConnectionParameter(context.thisJob.dest, 'driver') == 'bigquery':
                        shared.PutData[context.iWriters] = connections.initCursor(p_conn=shared.PutConn[context.iWriters], p_jobID=eJobID, p_source=context.thisJob.dest, p_fetchSize=context.thisJob.fetchSize)
                        if len(context.thisJob.preCmdDst) > 0:
                            try:
                                logging.logPrint(f'preparing BigQuery cursor #{context.iWriters} for inserts, executing preCmdDst=[{context.thisJob.preCmdDst}]', logLevel.DEBUG, p_jobID=eJobID)
                                shared.PutData[context.iWriters].execute(context.thisJob.preCmdDst)
                            except Exception as e:
                                logging.processError(p_e=e, p_message=f'preparing BigQuery cursor #{context.iWriters} for inserts, preCmdDst=[{context.thisJob.preCmdDst}]', p_jobID=eJobID,p_dontSendToStats=True)
                        
                        bq_conn = shared.PutConn[context.iWriters]
                        siObjSep = connections.getConnectionParameter(context.thisJob.dest, 'insert_object_delimiter')
                        table_id = f"{siObjSep}{bq_conn._bq_client.project}{siObjSep}.{siObjSep}{bq_conn._bq_dataset}{siObjSep}.{siObjSep}{bq_conn._bq_table}{siObjSep}"
                        targetCols = [col.strip() for col in sColNamesNoQuotes.split(',')]
                        shared.writeP[context.iWriters] = (mp.Process(target=datahandlers_bigquery.writeDataBigQuery, args = (eJobID, context.iWriters, shared.PutConn[context.iWriters], shared.PutData[context.iWriters], table_id, targetCols) ))
                        shared.writeP[context.iWriters].start()
                    else:
                        shared.PutData[context.iWriters] = shared.PutConn[context.iWriters].cursor()
                        if len(context.thisJob.preCmdDst) > 0:
                            try:
                                logging.logPrint(f'preparing cursor #{context.iWriters} for inserts, executing preCmdDst=[{context.thisJob.preCmdDst}]', logLevel.DEBUG, p_jobID=eJobID)
                                shared.PutData[context.iWriters].execute(context.thisJob.preCmdDst)
                            except Exception as e:
                                logging.processError(p_e=e, p_message=f'preparing cursor #{context.iWriters} for inserts, preCmdDst=[{context.thisJob.preCmdDst}]', p_jobID=eJobID,p_dontSendToStats=True)
                        shared.writeP[context.iWriters] = (mp.Process(target=datahandlers_relational.writeData, args = (eJobID, context.iWriters, shared.PutConn[context.iWriters], shared.PutData[context.iWriters], insertQuery) ))
                        shared.writeP[context.iWriters].start()
                    context.iWriters += 1
                    context.iRunningWriters += 1
                context.writersNotStartedYet = False
                logging.statsPrint('writeDataStart', eJobID, 0, 0, context.thisJob.nbrParallelWriters)

def handle_read(eJobID, recs, secs, context):
    context.iDataLinesRead[eJobID] += recs
    context.iTotalDataLinesRead += recs
    context.fReadSecs[eJobID] += secs
    context.fTotalReadSecs += secs

def handle_read_end(eJobID, recs, secs, context):
    context.iRunningReaders -= 1
    context.iReadingReaders -= 1
    if recs is None:
        context.iActiveJobsOnThisStream -= 1
        logging.statsPrint('readDataEnd', eJobID, context.iDataLinesRead[eJobID], context.fReadSecs[eJobID], context.iRunningReaders)
        try:
            shared.readP[eJobID].join(timeout=1)
        except:
            pass
    else:
        if context.iRunningReaders == 0:
            context.iActiveJobsOnThisStream -= 1
            logging.statsPrint('readDataEnd', eJobID, context.iDataLinesRead[eJobID], context.fReadSecs[eJobID], context.iRunningReaders)
        try:
            shared.readP[recs].join(timeout=1)
        except:
            pass

def handle_keys_read_start(eJobID, recs, secs, context):
    with shared.stopWhenKeysEmpty.get_lock():
        shared.stopWhenKeysEmpty.value = False
    logging.statsPrint('keysReadStart', p_jobID=eJobID, p_recs=recs, p_secs=0, p_threads=context.iRunningReaders)

def handle_keys_read_end(eJobID, recs, secs, context):
    with shared.stopWhenKeysEmpty.get_lock():
        shared.stopWhenKeysEmpty.value = True
    logging.statsPrint('keysReadEnd', eJobID, context.iDataLinesRead[eJobID], context.fReadSecs[eJobID], context.iRunningWriters)
