'''data handlers for BigQuery'''
from timeit import default_timer as timer
from multiprocessing import Queue
from queue import Empty as queueEmpty
from setproctitle import setproctitle

import modules.shared as shared
import modules.utils as utils
import modules.logging as logging
from modules.logging import logLevel as logLevel
from .common import playNice

def readDataBigQuery(p_jobID:int, p_connection, p_cursor, p_fetchSize:int, p_query:str, p_outQueue:Queue, p_finalDataReader:bool=True):
    '''gets data from BigQuery using optimized client methods'''

    playNice()

    utils.block_signals()

    jobName = shared.getJobName(p_jobID)
    processTitlePrefix:str =f'datacopy: readDataBigQuery{"" if p_finalDataReader else "[keys]"} '

    logging.logPrint(f'Started, fetchSize=[{p_fetchSize}], finalReader={p_finalDataReader}', logLevel.DEBUG, p_jobID=p_jobID)
    tStart = timer()
    
    errorOccurred = False

    try:
        shared.eventQueue.put( (
            shared.E_QUERY_START if p_finalDataReader else shared.E_KEYS_QUERY_START,
            p_jobID,  None, None)
        )

        # For BigQuery, we use the optimized client (p_cursor) to run the query
        # Note: BigQuery is job-oriented; the query must finish on the server
        # before any results can be iterated.
        # Use connectionTimeoutSecs for the initial API request
        query_job = p_cursor.query(p_query, timeout=shared.connectionTimeoutSecs)
        # Use idleTimeoutSecs for the wait time for the job to complete
        results = query_job.result(page_size=p_fetchSize, timeout=shared.idleTimeoutSecs)
        
        shared.eventQueue.put( (
            shared.E_QUERY_END if p_finalDataReader else shared.E_KEYS_QUERY_END,
            p_jobID,  None, (timer() - tStart))
        )

        # Get column descriptions (metadata)
        # BigQuery schema fields to DB-API-like description (7 items)
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        description = []
        for field in results.schema:
            description.append((
                field.name,
                field.field_type,
                None, # display_size
                None, # internal_size
                field.precision,
                field.scale,
                field.mode == 'NULLABLE'
            ))

        shared.eventQueue.put( (
            shared.E_READ_START if p_finalDataReader else shared.E_KEYS_READ_START,
            p_jobID, description, p_fetchSize )
        )

    except Exception as e:
        errorOccurred = True
        setproctitle(f'{processTitlePrefix}(error@query) [{jobName}]')
        logging.processError(p_e=e, p_message=f'executing BigQuery query', p_jobID=p_jobID, p_dontSendToStats=True)
        shared.eventQueue.put( (shared.E_QUERY_ERROR, p_jobID, None, (timer() - tStart)) )

    if not errorOccurred and not shared.TEST_QUERIES:
        setproctitle(f'{processTitlePrefix}(reading) [{jobName}]')
        
        use_storage_api = True
        try:
            # Try to use the Storage Read API (gRPC/Arrow)
            # This is significantly faster for large results.
            # to_arrow_iterable() will attempt to create a BQ Storage client automatically.
            arrow_batches = results.to_arrow_iterable()
            
            logging.logPrint('Using BigQuery Storage Read API (gRPC)', logLevel.DEBUG, p_jobID=p_jobID)
            
            for batch in arrow_batches:
                if not shared.Working.value:
                    break
                
                rStart = timer()
                # batch is a pyarrow.RecordBatch
                # Convert to list of tuples. to_pylist() is high-level, 
                # but we need a list of tuples to match the app's expectations.
                # Optimized way: use zip on the columns
                bData = list(zip(*(col.to_pylist() for col in batch.columns)))
                
                if bData:
                    shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData), (timer()-rStart)) )
                    p_outQueue.put( bData, block = True )
                    
        except Exception as e:
            # Fallback to standard REST API (results.pages)
            # This handles:
            # 1. Missing dependencies (pyarrow, google-cloud-bigquery-storage)
            # 2. Connectivity issues (Firewalls blocking gRPC/HTTP2)
            # 3. Logical/Driver issues
            use_storage_api = False
            logging.logPrint(f'BigQuery Storage API failed (falling back to REST): {str(e)}', logLevel.DEBUG, p_jobID=p_jobID)

        if not use_storage_api:
            # Fallback iteration using standard pages (REST API)
            for page in results.pages:
                if not shared.Working.value:
                    break
                
                rStart = timer()
                # Convert page of rows to list of tuples
                bData = [tuple(row.values()) for row in page]
                
                if bData:
                    shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData), (timer()-rStart)) )
                    p_outQueue.put( bData, block = True )

        logging.logPrint('exited read loop.', logLevel.DEBUG, p_jobID=p_jobID)

    setproctitle(f'{processTitlePrefix}(closing connection) [{jobName}]')
    try:
        p_connection.close()
    except Exception:
        pass

    shared.eventQueue.put( (
        shared.E_READ_END if p_finalDataReader else shared.E_KEYS_READ_END,
        p_jobID, None, None)
    )

    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID)

def readDataBigQuery2(p_jobID:int, p_threadID:int, p_connection2, p_cursor2, p_query2:str, p_fetchSize:int):
    '''gets data from BigQuery using optimized client methods, sublooping for keys'''

    playNice()

    utils.block_signals()

    from google.cloud import bigquery

    logging.logPrint(f'Started, with cursor2=[{id(p_cursor2)}], fetchSize=[{p_fetchSize}]', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)

    jobName = shared.getJobName(p_jobID)

    bColsNotSentYet = True
    errorOccurred = False

    setproctitle(f'datacopy: readDataBigQuery2 (reading) [{jobName}]#{p_threadID}')
    
    while shared.Working.value and not errorOccurred:
        try:
            bData = shared.dataKeysQueue.get(timeout=1)
        except queueEmpty:
            if shared.stopWhenKeysEmpty.value:
                break
            continue

        logging.logPrint(f'[{len(bData)}] rows received from readData Level 1', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
        for keys in bData:
            if shared.Working.value == False or errorOccurred:
                break

            logging.logPrint(f'executing query 2 with keys=[{keys}]', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
            qStart = timer()
            try:
                shared.eventQueue.put( (shared.E_DETAIL_QUERY_START, p_jobID, None, None) )
                
                # Positional parameters for BigQuery
                query_params = [bigquery.ScalarQueryParameter(None, None, k) for k in keys]
                job_config = bigquery.QueryJobConfig(query_parameters=query_params)
                
                # Use connectionTimeoutSecs for the API call; idleTimeoutSecs for the result wait
                query_job = p_cursor2.query(p_query2, job_config=job_config, timeout=shared.connectionTimeoutSecs)
                results = query_job.result(page_size=p_fetchSize, timeout=shared.idleTimeoutSecs)
                
                shared.eventQueue.put( (shared.E_DETAIL_QUERY_END, p_jobID, None, (timer() - qStart)) )
                
                # Check for Storage API optimization
                use_storage_api = True
                try:
                    arrow_batches = results.to_arrow_iterable()
                    # If this succeeds, iterate via Arrow
                    for batch in arrow_batches:
                        if not shared.Working.value:
                            break
                        
                        rStart = timer()
                        bData2 = list(zip(*(col.to_pylist() for col in batch.columns)))
                        
                        if bColsNotSentYet:
                            # description list (7 items)
                            description = []
                            for field in results.schema:
                                description.append((field.name, field.field_type, None, None, field.precision, field.scale, field.mode == 'NULLABLE'))
                            shared.eventQueue.put( (shared.E_READ_START, p_jobID, description, p_fetchSize ) )
                            if shared.TEST_QUERIES:
                                break
                            bColsNotSentYet = False

                        if bData2:
                            shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData2), (timer()-rStart)) )
                            shared.dataQueue.put( bData2, block = True )
                            
                except Exception as e:
                    # Fallback to REST API
                    use_storage_api = False
                    # Only log once or under DEBUG to avoid cluttering in a loop
                    if shared.TEST_QUERIES:
                        logging.logPrint(f'BigQuery Storage API fallback in readDataBigQuery2: {str(e)}', logLevel.DEBUG, p_jobID=p_jobID)

                if not use_storage_api:
                    # Iterate in pages
                    for page in results.pages:
                        if not shared.Working.value:
                            break
                        
                        rStart = timer()
                        # Convert page of rows to list of tuples
                        bData2 = [tuple(row.values()) for row in page]
                        
                        if bColsNotSentYet:
                            # description list (7 items)
                            description = []
                            for field in results.schema:
                                description.append((field.name, field.field_type, None, None, field.precision, field.scale, field.mode == 'NULLABLE'))
                            shared.eventQueue.put( (shared.E_READ_START, p_jobID, description, p_fetchSize ) )
                            if shared.TEST_QUERIES:
                                break
                            bColsNotSentYet = False

                        if bData2:
                            shared.eventQueue.put( (shared.E_READ, p_jobID, len(bData2), (timer()-rStart)) )
                            shared.dataQueue.put( bData2, block = True )

                if shared.TEST_QUERIES:
                    break

            except Exception as e:
                errorOccurred = True
                logging.processError(p_e=e, p_message=f'(execute2: keys=[{keys}], query2=[{p_query2}]', p_jobID=p_jobID, p_threadID=p_threadID)
                shared.eventQueue.put( (shared.E_QUERY_ERROR, p_jobID, None, (timer() - qStart)) )
                break

    setproctitle(f'datacopy: readDataBigQuery2 (closing connection) [{jobName}]#{p_threadID}')
    try:
        p_connection2.close()
    except Exception:
        pass

    shared.eventQueue.put( (shared.E_READ_END, p_jobID, p_threadID, None) )
    setproctitle(f'datacopy: readDataBigQuery2 (flushing) [{jobName}]#{p_threadID}')
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)

def writeDataBigQuery(p_jobID:int, p_threadID:int, p_connection, p_cursor, p_table_id:str, p_selected_fields:list=None):
    '''writes data to BigQuery using optimized insert_rows'''

    playNice()

    utils.block_signals()

    jobName = shared.getJobName(p_jobID)

    setproctitle(f'datacopy: writeDataBigQuery [{jobName}]')

    logging.logPrint('Started', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    shared.eventQueue.put( (shared.E_WRITE_START, p_jobID, None, None) )
    
    while shared.Working.value:
        try:
            bData = shared.dataQueue.get( block=True, timeout = 1 )
        except queueEmpty:
            if shared.stopWhenEmpty.value:
                logging.logPrint('end of data detected', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
                setproctitle(f'datacopy: writeDataBigQuery [{jobName}] stopping')
                break
            continue
        iStart = timer()
        try:
            # bData is a list of tuples
            # selected_fields allows us to specify which columns the data maps to
            # Use connectionTimeoutSecs for the API request timeout
            errors = p_cursor.insert_rows(p_table_id, bData, selected_fields=p_selected_fields, timeout=shared.connectionTimeoutSecs)
            if errors:
                raise Exception(f"BigQuery insert errors: {errors}")
        except Exception as e:
            setproctitle(f'datacopy: writeDataBigQuery [{jobName}], error occurred')
            logging.logPrint(bData, logLevel.DUMP_DATA)
            logging.processError(p_e=e, p_dontSendToStats=True, p_jobID=p_jobID, p_threadID=p_threadID)
            shared.eventQueue.put( (shared.E_WRITE_ERROR, p_jobID, p_threadID, None ) )
            break

        wr = len(bData)
        shared.eventQueue.put( (shared.E_WRITE, p_jobID, wr, (timer() - iStart)) )

    setproctitle(f'datacopy: writeDataBigQuery (closing connection) [{jobName}]')
    try:
        p_connection.close()
    except Exception:
        pass

    shared.eventQueue.put( (shared.E_WRITE_END, p_jobID, None, None) )
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID, p_threadID=p_threadID)
    setproctitle(f'datacopy: writeDataBigQuery [{jobName}], ended')

def calcColumnsFromDestination(p_jobID:int, p_connection, p_table:str, p_objSep:str):
    '''retrieves column metadata from BigQuery destination table'''
    # p_connection is the connection object, but for BigQuery we want the client
    # which is stored in p_connection._bq_client (see connections.py)
    client = p_connection._bq_client
    
    # Use fully qualified name with individual quoting for each part: `project`.`dataset`.`table`
    # This is more robust for IDs containing special characters like dashes or dots.
    full_table_id = f"{p_objSep}{client.project}{p_objSep}.{p_objSep}{p_connection._bq_dataset}{p_objSep}.{p_objSep}{p_table}{p_objSep}"
    
    fetchColsFromDestSql=f'SELECT * FROM {full_table_id}  WHERE 1=0'
    logging.logPrint(f'retrieving cols for @d, executing [{fetchColsFromDestSql}] on BigQuery', logLevel.DEBUG, p_jobID=p_jobID)
    
    # Use connectionTimeoutSecs for request; idleTimeoutSecs for result
    query_job = client.query(fetchColsFromDestSql, timeout=shared.connectionTimeoutSecs)
    results = query_job.result(timeout=shared.idleTimeoutSecs)
    
    description = []
    for field in results.schema:
        description.append((
            field.name,
            field.field_type,
            None, # display_size
            None, # internal_size
            field.precision,
            field.scale,
            field.mode == 'NULLABLE'
        ))
    
    # BigQuery connection doesn't have rollback
    return description

def cleanDestinationTable(p_jobID:int, p_connection, p_table:str, p_mode:str, p_objSep:str):
    '''cleans up BigQuery destination table (TRUNCATE or DELETE)'''
    client = p_connection._bq_client
    from timeit import default_timer as timer
    
    # Use fully qualified name with individual quoting for each part: `project`.`dataset`.`table`
    full_table_id = f"{p_objSep}{client.project}{p_objSep}.{p_objSep}{p_connection._bq_dataset}{p_objSep}.{p_objSep}{p_table}{p_objSep}"

    match p_mode.upper():
        case 'T':
            logging.logPrint(f'cleaning up table (truncate) [{full_table_id}] on BigQuery', p_jobID=p_jobID)
            cStart = timer()
            # TRUNCATE is more efficient in BigQuery (Metadata operation, no DML costs)
            cleanDestSQL=f'TRUNCATE TABLE {full_table_id}'
            try:
                logging.statsPrint('truncateStart', p_jobID, 0, 0, 0)
                client.query(cleanDestSQL, timeout=shared.connectionTimeoutSecs).result(timeout=shared.idleTimeoutSecs)
                logging.statsPrint('truncateEnd', p_jobID, 0, timer() - cStart, 0)
            except Exception as e:
                logging.statsPrint('truncateError', p_jobID, 0, timer() - cStart, 0)
                logging.processError(p_e=e, p_message=f'truncating BigQuery table [{full_table_id}] with sql=[{cleanDestSQL}]', p_jobID=p_jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                return False
        case 'D':
            logging.logPrint(f'cleaning up table (delete) [{full_table_id}] on BigQuery', p_jobID=p_jobID)
            cStart = timer()
            # DELETE is DML and might spend more quota/costs
            cleanDestSQL=f'DELETE FROM {full_table_id} WHERE TRUE'
            try:
                logging.statsPrint('deleteStart', p_jobID, 0, 0, 0)
                query_job = client.query(cleanDestSQL, timeout=shared.connectionTimeoutSecs)
                results = query_job.result(timeout=shared.idleTimeoutSecs)
                deletedRows = query_job.num_dml_affected_rows or -1
                logging.statsPrint('deleteEnd', p_jobID, deletedRows, timer() - cStart, 0)
            except Exception as e:
                logging.statsPrint('deleteError', p_jobID, 0, timer() - cStart, 0)
                logging.processError(p_e=e, p_message=f'deleting BigQuery table: [{full_table_id}] with sql=[{cleanDestSQL}]', p_jobID=p_jobID, p_dontSendToStats=True, p_stop=True, p_exitCode=5)
                return False
    
    return True
