''' job configuration and validation'''

#pylint: disable=invalid-name,broad-except

import os
import re

import pandas as pd

import modules.logging as logging
import modules.shared as shared
import modules.connections as connections

expected_query_columns = ("source","dest","mode","query","table")

def loadJobs(p_filename:str):
    '''loads job file into memory'''
    try:
        queriesRaw=pd.read_csv(p_filename, delimiter = '\t').fillna('')
        queriesRaw.index = queriesRaw.index+1
        shared.queries = queriesRaw[ queriesRaw.source.str.contains("^[A-Z,a-z,0-9]") ].reset_index()
    except Exception as error:
        logging.logPrint("error Loading [{0}]: [{1}]".format(p_filename, error))
        logging.closeLogFile(3)

def preCheck():
    '''checks that config files are consistent'''

    logging.logPrint("checking sources and destinations...")
    for ecol in expected_query_columns:
        if ecol not in shared.queries:
            logging.logPrint("Missing column on queries file: [{0}]".format(ecol))
            logging.closeLogFile(4)

    for i in range(0,len(shared.queries)):
        source = shared.queries["source"][i]
        if source == "" or source[0] == "#":
            continue
        dest = shared.queries["dest"][i]
        query = shared.queries["query"][i]
        if "dest2" in shared.queries:
            dest2 = shared.queries["dest2"][i]
        else:
            dest2 = ''
        if "query2" in shared.queries:
            query2 = shared.queries["query2"][i]
        else:
            query2 = ''

        if source not in shared.connections:
            logging.logPrint("ERROR: data source [{0}] not declared on connections.csv. giving up.".format(source))
            logging.closeLogFile(4)

        if connections.getConnectionParameter(source, "driver") == "csv":
            logging.logPrint("ERROR: csv driver requested as source on [{0}], but it's only available as destination yet. giving up.".format(source))
            logging.closeLogFile(4)

        if connections.getConnectionParameter(dest, "driver") == "csv":
            if "insert_cols" in shared.queries:
                if shared.queries["insert_cols"][i] == "@":
                    logging.logPrint("ERROR: cant infer (yet) insert cols from a csv destination (jobs line{0}). giving up.".format(i+1))
                    logging.closeLogFile(4)

        if dest not in shared.connections:
            logging.logPrint("ERROR: data destination [{0}] not declared on connections.csv. giving up.".format(dest))
            logging.closeLogFile(4)

        if dest2 != '':
            if dest2 not in shared.connections:
                logging.logPrint("ERROR: data sub-destination [{0}] not declared on connections.csv. giving up.".format(dest2))
                logging.closeLogFile(4)

        if query[0] == '@':
            if not os.path.isfile(query[1:]):
                logging.logPrint("ERROR: query file [{0}] does not exist! giving up.".format(query[1:]))
                logging.closeLogFile(4)

        if query2 != '':
            if query2[0] == '@':
                if not os.path.isfile(query[1:]):
                    logging.logPrint("ERROR: sub-query file [{0}] does not exist! giving up.".format(query2[1:]))
                    logging.closeLogFile(4)

        if "pre_query_src" in shared.queries:
            preQuerySrc = shared.queries["pre_query_src"][i]
            if preQuerySrc[0] == '@':
                if not os.path.isfile(preQuerySrc[1:]):
                    logging.logPrint("ERROR: pre query file [{0}] does not exist! giving up.".format(preQuerySrc[1:]))
                    logging.closeLogFile(4)

        if "pre_query_dst" in shared.queries:
            preQueryDst = shared.queries["pre_query_dst"][i]
            if preQueryDst[0] == '@':
                if not os.path.isfile(preQueryDst[1:]):
                    logging.logPrint("ERROR: pre query file [{0}] does not exist! giving up.".format(preQueryDst[1:]))
                    logging.closeLogFile(4)

        if "regexes" in shared.queries:
            regex = shared.queries["regexes"][i]
            if regex[0] == '@':
                if not os.path.isfile(regex[1:]):
                    logging.logPrint("ERROR: regex file [{0}] does not exist! giving up.".format(regex[1:]))
                    logging.closeLogFile(4)

def prepQuery(p_index):
    '''prepares the job step'''

    bCloseStream = None

    qIndex = shared.queries["index"][p_index]
    source = shared.queries["source"][p_index]
    if "source2" in shared.queries:
        source2 = shared.queries["source2"][p_index]
    else:
        source2 = ''
    dest = shared.queries["dest"][p_index]
    mode = shared.queries["mode"][p_index]
    query = shared.queries["query"][p_index]
    if "query2" in shared.queries:
        query2 = shared.queries["query2"][p_index]
    else:
        query2 = ''
    preQuerySrc = ''
    preQueryDst = ''
    bCSVEncodeSpecial = False

    if query[0] == '@':
        with open(query[1:], 'r') as file:
            query = file.read()
    if query2 != '':
        if query2[0] == '@':
            with open(query2[1:], 'r') as file:
                query2 = file.read()

    if "pre_query_src" in shared.queries:
        preQuerySrc = shared.queries["pre_query_src"][p_index]
        if preQuerySrc[0]  == '@':
            with open(preQuerySrc[1:], 'r') as file:
                preQuerySrc = file.read()

    if "pre_query_dst" in shared.queries:
        preQueryDst = shared.queries["pre_query_dst"][p_index]
        if preQueryDst[0]  == '@':
            with open(preQueryDst[1:], 'r') as file:
                preQueryDst = file.read()

    if "regexes" in shared.queries:
        regexes = shared.queries["regexes"][p_index]
        if regexes[0] == '@':
            with open(regexes[1:], 'r') as file:
                regexes = file.read().split('\n')
        else:
            if len(regexes)>0:
                regexes = [regexes.replace('/','\t')]
            else:
                regexes = None

        for regex in regexes:
            r = regex.split('\t')
            #result = re.sub(r"(\d.*?)\s(\d.*?)", r"\g<1> \g<2>", string1)
            if len(r) >= 2:
                logging.logPrint(f"prepQuery({0}): replacing [{1}] with [{2}] on queries".format(qIndex, r[0], r[1]))
                query = re.sub( r[0], r[1], query )
                query2 = re.sub( r[0], r[1], query2 )
                preQuerySrc = re.sub( r[0], r[1], preQuerySrc )
                preQueryDst = re.sub( r[0], r[1], preQueryDst )

    table = shared.queries["table"][p_index]

    if "fetch_size" in shared.queries:
        qFetchSize = int(shared.queries["fetch_size"][p_index])
        if qFetchSize == 0:
            fetchSize = shared.defaultFetchSize
        else:
            fetchSize = qFetchSize
    else:
        fetchSize = shared.defaultFetchSize

    if "parallel_writers" in shared.queries and not shared.testQueries:
        qParallelWriters = int(shared.queries["parallel_writers"][p_index])
        if qParallelWriters == 0:
            nbrParallelWriters = 1
        else:
            nbrParallelWriters = qParallelWriters
    else:
        nbrParallelWriters = 1

    if "csv_encode_special" in shared.queries:
        bCSVEncodeSpecial = bool(shared.queries["csv_encode_special"][p_index] == 'yes')

    if shared.ReuseWriters:
        if len(shared.queries) > 1 and p_index < len(shared.queries)-1 and shared.queries["dest"][p_index+1] == dest and shared.queries["table"][p_index+1] == table:
            bCloseStream = False
        else:
            bCloseStream = True
    else:
        bCloseStream = True

    logging.logPrint("prepQuery({0}): source=[{1}], source2=[{2}], dest=[{3}], table=[{4}] closeStream=[{5}], CSVEncodeSpecial=[{6}]".format(qIndex, source, source2, dest, table, bCloseStream, bCSVEncodeSpecial), shared.L_DEBUG)
    return (source, source2, dest, mode, preQuerySrc, preQueryDst, query, query2, table, fetchSize, nbrParallelWriters, bCloseStream, bCSVEncodeSpecial)
