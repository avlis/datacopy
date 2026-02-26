'''system related event handlers for jobManager'''
import re
import multiprocessing as mp
import modules.shared as shared
import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.connections as connections
import modules.datahandlers.common as datahandlers_common
import modules.datahandlers.relational as datahandlers_relational
import modules.datahandlers.bigquery as datahandlers_bigquery
import modules.datahandlers.csv as datahandlers_csv
import modules.jobs as jobs
import modules.utils as utils

def handle_boot(eJobID, recs, secs, context):
    if not shared.Working.value:
        return
    launchJob = jobs.Job(eJobID)
    if launchJob.mode.upper() == 'E':
        shared.eventQueue.put( (shared.E_BOOT_CMD, eJobID, None, None ) )
    else:
        shared.eventQueue.put( (shared.E_BOOT_READER, eJobID, None, None ) )

def handle_boot_cmd(eJobID, recs, secs, context):
    if not shared.Working.value:
        return

    context.tParallelReadersNextCheck = float('inf')
    context.iActiveJobsOnThisStream += 1

    context.iDataLinesRead[eJobID] = 0
    context.fReadSecs[eJobID] = .001

    thisJob = jobs.Job(eJobID)
    context.jobName = thisJob.jobName

    newConns = connections.initConnections(thisJob.source, p_readOnly=False, p_qtd=1)
    if newConns is not None:
        shared.GetConn[eJobID] = newConns[0]
        shared.GetData[eJobID] = connections.initCursor(p_conn=shared.GetConn[eJobID], p_jobID=eJobID, p_source=thisJob.dest, p_fetchSize=thisJob.fetchSize)

        logging.logPrint(f'executing statement on [{thisJob.source}] with query:\n***\n{thisJob.query}\n***', p_jobID=eJobID)
        r = mp.Process(target=datahandlers_common.executeStatement, args = (eJobID, shared.GetConn[eJobID], shared.GetData[eJobID], thisJob.source, thisJob.query))
        shared.readP[eJobID] = r
        r.start()
    else:
        context.bKeepGoing = False

def handle_boot_reader(eJobID, recs, secs, context):
    if not shared.Working.value:
        return

    context.iActiveJobsOnThisStream += 1
    context.iDataLinesRead[eJobID] = 0
    context.fReadSecs[eJobID] = .001

    thisJob = jobs.Job(eJobID)
    context.jobName = thisJob.jobName

    isSelect = re.search('(^|[ \t\n]+)SELECT[ \t\n]+', thisJob.query.upper())
    siObjSepSource = connections.getConnectionParameter(thisJob.source, 'insert_object_delimiter')

    if thisJob.mode.upper() == 'A' and context.oMaxAlreadyInsertedData:
        if utils.identify_type(context.oMaxAlreadyInsertedData) in ('integer', 'float'):
            sMaxAlreadyInsertedData = f'{context.oMaxAlreadyInsertedData}'
        else:
            sMaxAlreadyInsertedData = f"'{context.oMaxAlreadyInsertedData}'"

        if isSelect:
            thisJob.query = re.sub('#MAX_KEY_VALUE#', sMaxAlreadyInsertedData, thisJob.query)
        else:
            thisJob.query = f'SELECT * FROM {siObjSepSource}{thisJob.query}{siObjSepSource} WHERE {siObjSepSource}{thisJob.appendKeyColumn}{siObjSepSource} > {sMaxAlreadyInsertedData}'
    else:
        if not isSelect:
            if thisJob.sourceDriver != 'csv':
                thisJob.query = f'SELECT * FROM {siObjSepSource}{thisJob.query}{siObjSepSource}'

    # dual query case:
    if len(thisJob.key_source) > 0:
        logging.logPrint(f'reading keys from [{thisJob.key_source}] with query:\n***\n{thisJob.key_query}\n***', p_jobID=eJobID)
        logging.logPrint(f'and reading data from [{thisJob.source}] with query:\n***\n{thisJob.query}\n***', p_jobID=eJobID)

        r1JobID = eJobID * -1
        r1Query = thisJob.key_query
        r1SourceDriver = thisJob.key_sourceDriver
        r1FetchSize = thisJob.key_fetchSize
        
        if r1SourceDriver == 'csv':
            newConns = connections.initConnections(thisJob.key_source, True, 1, p_tableName=thisJob.key_query, p_mode='r')
        else:
            newConns = connections.initConnections(thisJob.key_source, True, 1)
        
        if newConns is not None:
            shared.GetConn[r1JobID] = newConns[0]
            if r1SourceDriver != 'csv':
                shared.GetData[r1JobID] = connections.initCursor(p_conn=shared.GetConn[r1JobID], p_jobID=eJobID, p_source=thisJob.key_source, p_fetchSize=thisJob.fetchSize)
        else:
            context.bKeepGoing = False
            return

        for i in range(1, shared.parallelReaders+1):
            thisThreadID = eJobID*1000+i
            newConns = connections.initConnections(thisJob.source, True, 1)
            if newConns is not None:
                shared.GetConn[thisThreadID] = newConns[0]
                shared.GetData[thisThreadID] = connections.initCursor(p_conn=shared.GetConn[thisThreadID], p_jobID=eJobID, p_source=thisJob.source, p_fetchSize=thisJob.fetchSize)

                if thisJob.sourceDriver == 'bigquery':
                    r2 = mp.Process(target=datahandlers_bigquery.readDataBigQuery2, args = (eJobID, thisThreadID, shared.GetConn[thisThreadID], shared.GetData[thisThreadID], thisJob.query, thisJob.fetchSize))
                else:
                    r2 = mp.Process(target=datahandlers_relational.readData2, args = (eJobID, thisThreadID, shared.GetConn[thisThreadID], shared.GetData[thisThreadID], thisJob.query, thisJob.fetchSize))
                shared.readP[thisThreadID] = r2
                r2.start()
                context.iRunningReaders += 1
            else:
                break

        outQueue = shared.dataKeysQueue
        r1FinalDataReader = False
        context.iDetailsQueriesSecs[eJobID] = 0.001
    else:
        r1JobID = eJobID
        r1Query = thisJob.query
        r1SourceDriver = thisJob.sourceDriver # Fixed from key_source
        r1FetchSize = thisJob.fetchSize
        newConns = connections.initConnections(p_name=thisJob.source, p_readOnly=True, p_qtd=1, p_tableName=thisJob.query, p_mode='r')
        if newConns is not None:
            shared.GetConn[r1JobID] = newConns[0]
        else:
            context.bKeepGoing = False
            return
        outQueue = shared.dataQueue
        r1FinalDataReader = True

        if r1SourceDriver != 'csv':
            shared.GetData[r1JobID] = connections.initCursor(p_conn=shared.GetConn[r1JobID], p_jobID=eJobID, p_source=thisJob.source, p_fetchSize=thisJob.fetchSize)
            logging.logPrint(f'reading data from [{thisJob.source}] with query:\n***\n{thisJob.query}\n***', p_jobID=eJobID)
        else:
            logging.logPrint(f'reading data from file [{shared.GetConn[r1JobID][0].name}]', p_jobID=eJobID)

    context.iDataLinesRead[r1JobID] = 0
    context.fReadSecs[r1JobID] = .001

    if r1SourceDriver == 'csv':
        shared.readP[r1JobID] = mp.Process(target=datahandlers_csv.readDataCSV, args = (r1JobID, shared.GetConn[r1JobID], r1FetchSize, outQueue, r1FinalDataReader))
    elif connections.getConnectionParameter(thisJob.source, 'driver') == 'bigquery':
        shared.readP[r1JobID] = mp.Process(target=datahandlers_bigquery.readDataBigQuery, args = (r1JobID, shared.GetConn[r1JobID], shared.GetData[r1JobID], r1FetchSize, r1Query, outQueue, r1FinalDataReader))
    else:
        shared.readP[r1JobID] = mp.Process(target=datahandlers_relational.readData, args = (r1JobID, shared.GetConn[r1JobID], shared.GetData[r1JobID], r1FetchSize, r1Query, outQueue, r1FinalDataReader))
    
    shared.readP[r1JobID].start()
    context.iRunningReaders += 1

    context.tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval

def handle_stop(eJobID, recs, secs, context):
    from setproctitle import setproctitle
    setproctitle(f'datacopy: jobManager thread, stop received')
    context.bStopRequested = True

def handle_noop(eJobID, recs, secs, context):
    pass
