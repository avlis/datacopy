''' job configuration and validation'''

#pylint: disable=invalid-name, broad-except, line-too-long

import os
import re

import pandas as pd

import modules.logging as logging
import modules.shared as shared
import modules.connections as connections

expected_query_columns = ('source','dest','mode','query','table')

def loadJobs(p_filename:str):
    '''loads job file into memory'''
    try:
        queriesRaw=pd.read_csv(p_filename, delimiter = '\t').fillna('')
        queriesRaw.index = queriesRaw.index+1
        shared.queries = queriesRaw[ queriesRaw.source.str.contains('^[A-Z,a-z,0-9]') ].reset_index()
    except Exception as error:
        logging.logPrint(f'error Loading [{p_filename}]: [{error}]')
        logging.closeLogFile(3)

def preCheck():
    '''checks that config files are consistent'''

    logging.logPrint('checking sources and destinations...')
    for ecol in expected_query_columns:
        if ecol not in shared.queries:
            logging.logPrint(f'Missing column on queries file: [{ecol}]')
            logging.closeLogFile(4)

    for i in range(0,len(shared.queries)):
        source = shared.queries['source'][i]
        if source == '' or source[0] == '#':
            continue
        dest = shared.queries['dest'][i]
        mode = shared.queries['mode'][i]
        query = shared.queries['query'][i]
        if 'dest2' in shared.queries:
            dest2 = shared.queries['dest2'][i]
        else:
            dest2 = ''
        if 'append_source' in shared.queries:
            apDest = shared.queries['append_source'][i]
        else:
            apDest = ''
        if 'query2' in shared.queries:
            query2 = shared.queries['query2'][i]
        else:
            query2 = ''

        if source not in shared.connections:
            logging.logPrint(f'ERROR: data source [{source}] not declared on connections.csv. giving up.')
            logging.closeLogFile(4)

        if connections.getConnectionParameter(source, 'driver') == 'csv':
            logging.logPrint(f"ERROR: csv driver requested as source on [{source}], but it's only available as destination yet. giving up.")
            logging.closeLogFile(4)

        if connections.getConnectionParameter(dest, 'driver') == 'csv':
            if 'insert_cols' in shared.queries:
                if shared.queries['insert_cols'][i] == '@d':
                    logging.logPrint(f'ERROR: cannot infer (yet) insert cols from a csv destination (jobs line{i+1}). giving up.')
                    logging.closeLogFile(4)

        if dest not in shared.connections:
            logging.logPrint(f'ERROR: data destination [{dest}] not declared on connections.csv. giving up.')
            logging.closeLogFile(4)

        if dest2 != '':
            if dest2 not in shared.connections:
                logging.logPrint(f'ERROR: data sub-destination [{dest2}] not declared on connections.csv. giving up.')
                logging.closeLogFile(4)

        if apDest != '':
            if apDest not in shared.connections:
                logging.logPrint(f'ERROR: append source [{apDest}] not declared on connections.csv. giving up.')
                logging.closeLogFile(4)

        if query[0] == '@':
            if not os.path.isfile(query[1:]):
                logging.logPrint(f'ERROR: query file [{query[1:]}] does not exist! giving up.')
                logging.closeLogFile(4)

        if query2 != '':
            if query2[0] == '@':
                if not os.path.isfile(query[1:]):
                    logging.logPrint(f'ERROR: sub-query file [{query2[1:]}] does not exist! giving up.')
                    logging.closeLogFile(4)

        if 'pre_query_src' in shared.queries:
            preQuerySrc = shared.queries['pre_query_src'][i]
            if preQuerySrc[0] == '@':
                if not os.path.isfile(preQuerySrc[1:]):
                    logging.logPrint(f'ERROR: pre query file [{preQuerySrc[1:]}] does not exist! giving up.')
                    logging.closeLogFile(4)

        if 'pre_query_dst' in shared.queries:
            preQueryDst = shared.queries['pre_query_dst'][i]
            if preQueryDst[0] == '@':
                if not os.path.isfile(preQueryDst[1:]):
                    logging.logPrint(f'ERROR: pre query file [{preQueryDst[1:]}] does not exist! giving up.')
                    logging.closeLogFile(4)

        if 'regexes' in shared.queries:
            regex = shared.queries['regexes'][i]
            if regex[0] == '@':
                if not os.path.isfile(regex[1:]):
                    logging.logPrint('ERROR: regex file [{regex[1:]}] does not exist! giving up.')
                    logging.closeLogFile(4)

        if mode.upper() == 'A':
            if 'append_column' in shared.queries:
                apCol = shared.queries['append_column'][i]
                if apCol == '':
                    logging.logPrint('ERROR: append column is empty! giving up.')
                    logging.closeLogFile(4)
            else:
                logging.logPrint('ERROR: Mode A set, but append column not specified! giving up.')
                logging.closeLogFile(4)


        if 'append_query' in shared.queries:
            apQuery = shared.queries['append_query'][i]
            if apQuery[0] == '@':
                if not os.path.isfile(apQuery[1:]):
                    logging.logPrint(f'ERROR: append query file [{apQuery[1:]}] does not exist! giving up.')
                    logging.closeLogFile(4)

def prepQuery(p_index):
    '''prepares the job step'''

    bCloseStream = None

    qIndex = shared.queries['index'][p_index]
    source = shared.queries['source'][p_index]
    if 'source2' in shared.queries:
        source2 = shared.queries['source2'][p_index]
    else:
        source2 = ''
    dest = shared.queries['dest'][p_index]
    mode = shared.queries['mode'][p_index]
    query = shared.queries['query'][p_index]
    if 'query2' in shared.queries:
        query2 = shared.queries['query2'][p_index]
    else:
        query2 = ''
    preQuerySrc = ''
    preQueryDst = ''
    bCSVEncodeSpecial = False

    siObjSep = connections.getConnectionParameter(dest, 'insert_object_delimiter')

    if query[0] == '@':
        with open(query[1:], 'r', encoding = 'utf-8') as file:
            query = file.read()
    if query2 != '':
        if query2[0] == '@':
            with open(query2[1:], 'r', encoding = 'utf-8') as file:
                query2 = file.read()

    if 'pre_query_src' in shared.queries:
        preQuerySrc = shared.queries['pre_query_src'][p_index]
        if preQuerySrc[0]  == '@':
            with open(preQuerySrc[1:], 'r', encoding = 'utf-8') as file:
                preQuerySrc = file.read()

    if 'pre_query_dst' in shared.queries:
        preQueryDst = shared.queries['pre_query_dst'][p_index]
        if preQueryDst[0]  == '@':
            with open(preQueryDst[1:], 'r', encoding = 'utf-8') as file:
                preQueryDst = file.read()

    if 'regexes' in shared.queries:
        regexes = shared.queries['regexes'][p_index]
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
                logging.logPrint(f'prepQuery({qIndex}): replacing [{r[0]}] with [{r[1]}] on queries')
                query = re.sub( r[0], r[1], query )
                query2 = re.sub( r[0], r[1], query2 )
                preQuerySrc = re.sub( r[0], r[1], preQuerySrc )
                preQueryDst = re.sub( r[0], r[1], preQueryDst )

    table = shared.queries['table'][p_index]

    if 'fetch_size' in shared.queries:
        qFetchSize = int(shared.queries['fetch_size'][p_index])
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
        if 'append_column' in shared.queries:
            appendKeyColumn = shared.queries['append_column'][p_index]

        if 'append_query' in shared.queries:
            appendKeyQuery = shared.queries['append_query'][p_index]

        if appendKeyQuery != '':
            if appendKeyQuery[0] == '@':
                with open(appendKeyQuery[1:], 'r', encoding = 'utf-8') as file:
                    getMaxQuery = file.read()
            elif appendKeyQuery.upper()[:7] == 'SELECT ':
                getMaxQuery = appendKeyQuery
        else:
            getMaxQuery = f'SELECT MAX({appendKeyColumn}) FROM {siObjSep}{table}{siObjSep}'

        if 'append_source' in shared.queries:
            getMaxDest = shared.queries['append_source'][p_index]

    if 'parallel_writers' in shared.queries and not shared.testQueries:
        qParallelWriters = int(shared.queries['parallel_writers'][p_index])
        if qParallelWriters == 0:
            nbrParallelWriters = 1
        else:
            nbrParallelWriters = qParallelWriters
    else:
        nbrParallelWriters = 1

    if 'csv_encode_special' in shared.queries:
        bCSVEncodeSpecial = bool(shared.queries['csv_encode_special'][p_index] == 'yes')

    if shared.ReuseWriters:
        if len(shared.queries) > 1 and p_index < len(shared.queries)-1 and shared.queries['dest'][p_index+1] == dest and shared.queries['table'][p_index+1] == table:
            bCloseStream = False
        else:
            bCloseStream = True
    else:
        bCloseStream = True

    logging.logPrint(f'prepQuery({qIndex}): source=[{source}], source2=[{source2}], dest=[{dest}], table=[{table}] closeStream=[{bCloseStream}], CSVEncodeSpecial=[{bCSVEncodeSpecial}], appendKeyColumn=[{appendKeyColumn}], getMaxQuery=[{getMaxQuery}]', shared.L_DEBUG)
    return (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial, appendKeyColumn, getMaxQuery, getMaxDest)
