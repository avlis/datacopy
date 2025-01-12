''' job configuration and validation'''

#pylint: disable=invalid-name, broad-except, line-too-long

import os
import re

import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 256)
pd.set_option('display.width', 1000)

from io import StringIO

import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.shared as shared
import modules.connections as connections

expected_query_columns = ('source','dest','mode','query','table')

def loadJobs(p_filename:str):
    '''loads job file into memory'''
    try:
        queriesRaw=pd.read_csv(p_filename, delimiter = '\t').fillna('')
        queriesRaw.index = queriesRaw.index+1
        shared.jobs = queriesRaw[ queriesRaw.source.str.contains('^[A-Z,a-z,0-9]') ].reset_index()
    except Exception as error:
        logging.processError(p_e=error, p_message=f'Loading [{p_filename}]', p_stop=True, p_exitCode=3)
        return

def preCheckSqlFile(p_filename:str, p_errorTitle:str):
    '''checks that a sql file exists, considering #include directives, and is not empty'''
    logging.logPrint(f'checking {p_errorTitle} file [{p_filename}]...', logLevel.DEBUG)
    if not os.path.isfile(p_filename):
        logging.processError(p_message=f'query file [{p_filename}] does not exist! giving up.', p_stop=True, p_exitCode=4)
        return
    else:
        buffer = readSqlFile(p_filename)
        if len(buffer) == 0:
            logging.processError(p_message=f'{p_errorTitle} file [{p_filename}] is empty! giving up.', p_stop=True, p_exitCode=4)
            return

def preCheck():
    '''checks that config files are consistent'''

    logging.logPrint('checking sources and destinations...')
    for ecol in expected_query_columns:
        if ecol not in shared.jobs:
            logging.processError(p_message=f'Missing column on queries file: [{ecol}]', p_stop=True, p_exitCode=4)
            return

    for i in range(0,len(shared.jobs)):
        source = shared.jobs['source'][i]
        if len(source) == 0 or source[0] == '#':
            continue
        dest = shared.jobs['dest'][i]
        mode = shared.jobs['mode'][i]
        query = shared.jobs['query'][i]
        if 'dest2' in shared.jobs:
            dest2 = shared.jobs['dest2'][i]
        else:
            dest2 = ''
        if 'append_source' in shared.jobs:
            apDest = shared.jobs['append_source'][i]
        else:
            apDest = ''
        if 'query2' in shared.jobs:
            query2 = shared.jobs['query2'][i]
        else:
            query2 = ''

        if source not in shared.connections:
            logging.processError(p_message=f'data source [{source}] not declared on connections.csv. giving up.', p_stop=True, p_exitCode=4)
            return

        if connections.getConnectionParameter(source, 'driver') == 'csv':
            logging.processError(p_message=f"ERROR: csv driver requested as source on [{source}], but it's only available as destination yet. giving up.", p_stop=True, p_exitCode=4)
            return

        if connections.getConnectionParameter(dest, 'driver') == 'csv':
            if 'insert_cols' in shared.jobs:
                if shared.jobs['insert_cols'][i] == '@d':
                    logging.processError(p_message=f'cannot infer (yet) insert cols from a csv destination (jobs line{i+1}). giving up.', p_stop=True, p_exitCode=4)
                    return

        if dest not in shared.connections:
            logging.processError(p_message=f'data destination [{dest}] not declared on connections.csv. giving up.', p_stop=True, p_exitCode=4)
            return

        if len(dest2) > 0:
            if dest2 not in shared.connections:
                logging.processError(p_message=f'data sub-destination [{dest2}] not declared on connections.csv. giving up.', p_stop=True, p_exitCode=4)
                return

        if len(apDest) > 0:
            if apDest not in shared.connections:
                logging.processError(p_message=f'append source [{apDest}] not declared on connections.csv. giving up.', p_stop=True, p_exitCode=4)
                return

        if len(query) > 0:
            if query[0] == '@':
                preCheckSqlFile(query[1:], 'query')
        else:
            logging.processError(p_message='query cannot be empty!! giving up.', p_stop=True, p_exitCode=4)
            return

        if len(query2) > 0:
            if query2[0] == '@':
                preCheckSqlFile(query2[1:], 'sub-query')

        if 'pre_query_src' in shared.jobs:
            preQuerySrc = shared.jobs['pre_query_src'][i]
            if len(preQuerySrc) > 0 and preQuerySrc[0] == '@':
                preCheckSqlFile(preQuerySrc[1:], 'pre-query on source')

        if 'pre_query_dst' in shared.jobs:
            preQueryDst = shared.jobs['pre_query_dst'][i]
            if len(preQueryDst) >0 and preQueryDst[0] == '@':
                preCheckSqlFile(preQueryDst[1:], 'pre-query on destination')

        if 'regexes' in shared.jobs:
            regex = shared.jobs['regexes'][i]
            if len(regex) > 0 and regex[0] == '@':
                if not os.path.isfile(regex[1:]):
                    logging.processError(p_message=f'regex file [{regex[1:]}] does not exist! giving up.', p_stop=True, p_exitCode=4)
                    return

        if mode.upper() == 'A':
            if 'append_column' in shared.jobs:
                apCol = shared.jobs['append_column'][i]
                if len(apCol) == 0:
                    logging.processError(p_message='append column is empty! giving up.', p_stop=True, p_exitCode=4)
                    return
            else:
                logging.processError(p_message='Mode A set, but append column not specified! giving up.', p_stop=True, p_exitCode=4)
                return


        if 'append_query' in shared.jobs:
            apQuery = shared.jobs['append_query'][i]
            if len(apQuery) > 0 and apQuery[0] == '@':
                if not os.path.isfile(apQuery[1:]):
                    logging.processError(p_message=f'append query file [{apQuery[1:]}] does not exist! giving up.', p_stop=True, p_exitCode=4)
                    return

    #prebuild the jobName used in logs and stats
    shared.jobs['jobName'] = shared.jobs.apply(lambda row: f"{row['index']}-{row['source']}-{row['dest']}-{row['table']}", axis=1)

    if shared.DEBUG:
        buffer = StringIO()
        print(shared.jobs, file=buffer, flush=True)
        logging.logPrint(f'final jobs data:\n{buffer.getvalue()}\n', logLevel.DEBUG)

def readSqlFile(p_filename:str, p_parentFilename:str = '') -> str:
    '''Reads a SQL file, handling #include directives.'''
    logging.logPrint(f'reading file [{p_filename}] (ref from [{p_parentFilename}])', logLevel.DEBUG)
    buffer = ''
    try:
        with open(p_filename, 'r', encoding = 'utf-8') as file:
            for line in file:
                if line.startswith('#include '):
                    # Extract the filename from the #include directive
                    include_filename = line[9:].strip()
                    # Recursively call readSqlFile to read the included file
                    buffer += readSqlFile(include_filename, p_filename)
                else:
                    # Append the current line to the buffer
                    buffer += line
        return buffer
    except Exception as error:
        if len(p_parentFilename) == 0:
            logging.processError(p_message=f'error trying to read sql file [{p_filename}]: [{error}]')
        else:
            logging.processError(p_message=f'error trying to read sql file [{p_filename}] mentioned in [{p_parentFilename}]: [{error}]', p_stop=True, p_exitCode=4)
        return


def calcJob(p_jobID:int) -> tuple:
    '''calculates all the job variables necessary for job execution'''

    def add_schema_prequery(p_conn_name:str, existing_prequery:str) -> str:
        buffer = existing_prequery
        try:
            schema = connections.getConnectionParameter(p_conn_name, 'schema')
            if schema is not None and len(schema) > 0:
                driver = connections.getConnectionParameter(p_conn_name, 'driver')
                driver_schema_cmd = connections.change_schema_cmd[driver]
                if len(driver_schema_cmd) > 0:
                    if len(existing_prequery) > 0:
                        buffer = driver_schema_cmd.format(schema)
                    else:
                        buffer = f'{driver_schema_cmd.format(schema)}; {existing_prequery}'
        except Exception as e:
            logging.processError(p_e=e, p_message=f'{p_conn_name}::{existing_prequery}', p_logLevel=logLevel.DEBUG, p_stop=True, p_exitCode=4)
            return

        return buffer


    bCloseStream = None

    source = shared.jobs['source'][p_jobID]
    if 'source2' in shared.jobs:
        source2 = shared.jobs['source2'][p_jobID]
    else:
        source2 = ''
    dest = shared.jobs['dest'][p_jobID]
    mode = shared.jobs['mode'][p_jobID]
    query = shared.jobs['query'][p_jobID]
    if 'query2' in shared.jobs:
        query2 = shared.jobs['query2'][p_jobID]
    else:
        query2 = ''
    preQuerySrc = ''
    preQueryDst = ''
    bCSVEncodeSpecial = False

    siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')

    if len(query) > 0 and query[0] == '@':
        query = readSqlFile(query[1:])
    if len(query2) > 0 and query2[0] == '@':
            query2 = readSqlFile(query2[1:])

    if 'pre_query_src' in shared.jobs:
        preQuerySrc = shared.jobs['pre_query_src'][p_jobID]
        if len(preQuerySrc) > 0 and preQuerySrc[0]  == '@':
            preQuerySrc = readSqlFile(preQuerySrc[1:])

    preQuerySrc = add_schema_prequery(source, preQuerySrc)

    if 'pre_query_dst' in shared.jobs:
        preQueryDst = shared.jobs['pre_query_dst'][p_jobID]
        if len(preQueryDst) > 0 and preQueryDst[0]  == '@':
            preQueryDst = readSqlFile(preQueryDst[1:])

    preQueryDst = add_schema_prequery(dest, preQueryDst)

    if 'regexes' in shared.jobs:
        regexes = shared.jobs['regexes'][p_jobID]
        if regexes[0] == '@':
            with open(regexes[1:], 'r', encoding = 'utf-8') as file:
                regexes = file.read().split('\n')
        else:
            if len(regexes)>0:
                regexes = [regexes.replace('/','\t')]
            else:
                regexes = None

        for regex in regexes:
            r = regex.split('\t')
            if len(r) >= 2:
                logging.logPrint(f'replacing [{r[0]}] with [{r[1]}] on queries', p_jobID=p_jobID)
                query = re.sub( r[0], r[1], query )
                query2 = re.sub( r[0], r[1], query2 )
                preQuerySrc = re.sub( r[0], r[1], preQuerySrc )
                preQueryDst = re.sub( r[0], r[1], preQueryDst )

    table = shared.jobs['table'][p_jobID]

    if 'fetch_size' in shared.jobs:
        qFetchSize = int(shared.jobs['fetch_size'][p_jobID])
        if qFetchSize == 0:
            fetchSize = shared.defaultFetchSize
        else:
            fetchSize = qFetchSize
    else:
        fetchSize = shared.defaultFetchSize

    appendKeyColumn = ''
    appendKeyQuery = ''
    getMaxQuery = ''
    getMaxDest = ''

    if mode.upper() == 'A':
        if 'append_column' in shared.jobs:
            appendKeyColumn = shared.jobs['append_column'][p_jobID]

        if 'append_query' in shared.jobs:
            appendKeyQuery = shared.jobs['append_query'][p_jobID]

        if len(appendKeyQuery) > 0:
            if appendKeyQuery[0] == '@':
                with open(appendKeyQuery[1:], 'r', encoding = 'utf-8') as file:
                    getMaxQuery = file.read()
            elif appendKeyQuery.upper()[:7] == 'SELECT ':
                getMaxQuery = appendKeyQuery
        else:
            getMaxQuery = f'SELECT MAX({appendKeyColumn}) FROM {siObjSep}{table}{siObjSep}'

        if 'append_source' in shared.jobs:
            getMaxDest = shared.jobs['append_source'][p_jobID]

    if 'parallel_writers' in shared.jobs and not shared.TEST_QUERIES:
        qParallelWriters = int(shared.jobs['parallel_writers'][p_jobID])
        if qParallelWriters == 0:
            nbrParallelWriters = 1
        else:
            nbrParallelWriters = qParallelWriters
    else:
        nbrParallelWriters = 1

    if 'csv_encode_special' in shared.jobs:
        bCSVEncodeSpecial = bool(shared.jobs['csv_encode_special'][p_jobID] == 'yes')

    if shared.REUSE_WRITERS:
        if len(shared.jobs) > 1 and p_jobID < len(shared.jobs)-1 and shared.jobs['dest'][p_jobID+1] == dest and shared.jobs['table'][p_jobID+1] == table:
            bCloseStream = False
        else:
            bCloseStream = True
    else:
        bCloseStream = True

    logging.logPrint(f'source=[{source}], source2=[{source2}], dest=[{dest}], preQuerySrc=[{preQuerySrc}] preQueryDst=[{preQueryDst}] table=[{table}] closeStream=[{bCloseStream}], CSVEncodeSpecial=[{bCSVEncodeSpecial}], appendKeyColumn=[{appendKeyColumn}], getMaxQuery=[{getMaxQuery}]', logLevel.DEBUG, p_jobID=p_jobID)
    return (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial, appendKeyColumn, getMaxQuery, getMaxDest)
