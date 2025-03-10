''' job configuration and validation'''

#pylint: disable=invalid-name, broad-except, line-too-long

import os
import re

import json

from typing import Any

import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.shared as shared
import modules.utils as utils
import modules.connections as connections

expected_query_columns = ('source','dest','mode','query','table')

class Job:
    '''job variables organizer class to ease management'''

    def __init__(self, p_jobID:int):

        thisJobData = shared.jobs[p_jobID]

        ### "PUBLIC PROPERTIES"
        self.jobID = p_jobID
        if 'jobName' in thisJobData:
            self.jobName = thisJobData['jobName']
        else:
            self.jobName = f'{p_jobID}-{thisJobData["source"]}-{thisJobData["dest"]}-{thisJobData["table"]}'

        self.source = thisJobData['source']
        if 'source2' in thisJobData:
            self.source2 = thisJobData['source2']
        else:
            self.source2 = ''
        self.dest = thisJobData['dest']
        self.mode = thisJobData['mode']
        self.query = thisJobData['query']
        if 'query2' in thisJobData:
            self.query2 = thisJobData['query2']
        else:
            self.query2 = ''
        self.preQuerySrc = ''
        self.preQueryDst = ''

        siObjSep = connections.getConnectionParameter(self.dest, 'insert_object_delimiter')

        if len(self.query) > 0 and self.query[0] == '@':
            self.query = readSqlFile(self.query[1:])
        if len(self.query2) > 0 and self.query2[0] == '@':
                self.query2 = readSqlFile(self.query2[1:])

        if 'pre_query_src' in thisJobData:
            self.preQuerySrc = thisJobData['pre_query_src']
            if len(self.preQuerySrc) > 0 and self.preQuerySrc[0]  == '@':
                self.preQuerySrc = readSqlFile(self.preQuerySrc[1:])

        self.preQuerySrc = self.__add_schema_prequery(self.source, self.preQuerySrc)

        if 'pre_query_dst' in thisJobData:
            self.preQueryDst = thisJobData['pre_query_dst']
            if len(self.preQueryDst) > 0 and self.preQueryDst[0]  == '@':
                self.preQueryDst = readSqlFile(self.preQueryDst[1:])

        self.preQueryDst = self.__add_schema_prequery(self.dest, self.preQueryDst)

        if 'regexes' in thisJobData:
            regexes = thisJobData['regexes']
            if regexes[0] == '@':
                with open(regexes[1:], 'r', encoding = 'utf-8') as file:
                    regexes = file.read().split('\n')
            else:
                if len(regexes)>0:
                    regexes = [regexes.replace('/','\t')]
                else:
                    regexes = None

            self.regexes:dict[str, str] = {}
            if regexes is not None:
                for regex in regexes:
                    r = regex.split('\t')
                    if len(r) >= 2:
                        self.regexes[r[0]] = r[1]
                        logging.logPrint(f'replacing [{r[0]}] with [{r[1]}] on queries', p_jobID=p_jobID)
                        self.query = re.sub( r[0], r[1], self.query )
                        self.query2 = re.sub( r[0], r[1], self.query2 )
                        self.preQuerySrc = re.sub( r[0], r[1], self.preQuerySrc )
                        self.preQueryDst = re.sub( r[0], r[1], self.preQueryDst )

        self.table = thisJobData['table']

        if 'fetch_size' in thisJobData:
            qFetchSize = int(thisJobData['fetch_size'])
            if qFetchSize == 0:
                self.fetchSize = shared.defaultFetchSize
            else:
                self.fetchSize = qFetchSize
        else:
            self.fetchSize = shared.defaultFetchSize

        self.appendKeyColumn = ''
        self.appendKeyQuery = ''
        self.getMaxQuery = ''
        self.getMaxDest = ''

        if self.mode.upper() == 'A':
            if 'append_column' in thisJobData:
                self.appendKeyColumn = thisJobData['append_column']

            if 'append_query' in thisJobData:
                self.appendKeyQuery = thisJobData['append_query']

            if len(self.appendKeyQuery) > 0:
                if self.appendKeyQuery[0] == '@':
                    with open(self.appendKeyQuery[1:], 'r', encoding = 'utf-8') as file:
                        self.getMaxQuery = file.read()
                elif self.appendKeyQuery.upper()[:7] == 'SELECT ':
                    self.getMaxQuery = self.appendKeyQuery
            else:
                self.getMaxQuery = f'SELECT MAX({self.appendKeyColumn}) FROM {siObjSep}{self.table}{siObjSep}'

            if 'append_source' in thisJobData:
                self.getMaxDest = thisJobData['append_source']

        if 'insert_cols' in thisJobData:
            self.overrideCols:str = str(thisJobData['insert_cols'])
        else:
            self.overrideCols:str = ''

        if 'ignore_cols' in thisJobData:
            self.ignoreCols:tuple = tuple((thisJobData['ignore_cols']).split(','))
        else:
            self.ignoreCols:tuple = ()

        if 'parallel_writers' in thisJobData and not shared.TEST_QUERIES:
            qParallelWriters = int(thisJobData['parallel_writers'])
            if qParallelWriters == 0:
                self.nbrParallelWriters:int = 1
            else:
                self.nbrParallelWriters:int = qParallelWriters
        else:
            self.nbrParallelWriters:int = 1

        if 'csv_encode_special' in thisJobData:
            self.bCSVEncodeSpecial = bool(thisJobData['csv_encode_special'] == 'yes')
        else:
            self.bCSVEncodeSpecial = False

        if shared.REUSE_WRITERS:
            if len(shared.jobs) > 1 and p_jobID < len(shared.jobs) and shared.jobs[p_jobID+1]['dest'] == self.dest and shared.jobs[p_jobID+1]['table'] == self.table:
                self.bCloseStream = False
            else:
                self.bCloseStream = True
        else:
            self.bCloseStream = True

        #logging.logPrint(f'source=[{source}], source2=[{source2}], dest=[{dest}], preQuerySrc=[{preQuerySrc}] preQueryDst=[{preQueryDst}] table=[{table}] closeStream=[{bCloseStream}], CSVEncodeSpecial=[{bCSVEncodeSpecial}], appendKeyColumn=[{appendKeyColumn}], getMaxQuery=[{getMaxQuery}]', logLevel.DEBUG, p_jobID=p_jobID)
        logging.logPrint(f'created:\n{json.dumps(self.to_dict(), indent=2)}\n', logLevel.DEBUG, p_jobID=p_jobID)

    def to_dict(self) -> dict[str, Any]:
        buffer:dict[str, Any] = {}
        for p in dir(self):
            value = getattr(self, p)
            if p[0:1] != '_' and not callable(value):
                buffer[p] = value
        return buffer

    def __iter__(self):
        buffer:list[str]=[]
        for p in dir(self):
            value = getattr(self, p)
            if p[0:1] != '_' and not callable(value):
                buffer.append(p)
        yield buffer

    def __getitem__(self, key:str):
        if hasattr(self, key) and not callable(getattr(self, key)):
            return getattr(self, key)
        else:
            raise KeyError(f'[{key}] not found in [{self.__class__.__name__}].')

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    def __repr__(self):
        return f'Job({self.__str__()})'

    @staticmethod
    def __add_schema_prequery(p_conn_name:str, existing_prequery:str) -> str:
        buffer = existing_prequery
        try:
            schema = connections.getConnectionParameter(p_conn_name, 'schema')
            if schema is not None and len(schema) > 0:
                driver = connections.getConnectionParameter(p_conn_name, 'driver')
                if driver is not None:
                    driver_schema_cmd = connections.change_schema_cmd[driver]
                else:
                    driver_schema_cmd = ''
                if len(driver_schema_cmd) > 0:
                    if len(existing_prequery) > 0:
                        buffer = driver_schema_cmd.format(schema)
                    else:
                        buffer = f'{driver_schema_cmd.format(schema)}; {existing_prequery}'
        except Exception as e:
            logging.processError(p_e=e, p_message=f'{p_conn_name}::{existing_prequery}', p_stop=True, p_exitCode=4)
            return ''

        return buffer


def loadJobs(p_filename:str):
    '''loads job file into memory'''
    try:
        j = utils.read_csv_config(p_filename, emptyValuesDefault='', sequencialLineNumbers=True)
    except Exception as error:
        logging.processError(p_e=error, p_message=f'Loading [{p_filename}]', p_stop=True, p_exitCode=3)
        return
    shared.jobs = j
    logging.logPrint(f'raw loaded jobs file:\n{json.dumps(j, indent=2)}\n', logLevel.DEBUG)

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

    for key in shared.jobs.keys():
        aJob:dict[str, Any] = shared.jobs[key]
        for ecol in expected_query_columns:
            if ecol not in aJob:
                logging.processError(p_message=f'[{key}]: Missing column on queries file: [{ecol}]', p_stop=True, p_exitCode=4)
                return
        source = aJob['source']
        dest = aJob['dest']
        mode = aJob['mode']
        query = aJob['query']
        if 'dest2' in aJob:
            dest2 = aJob['dest2']
        else:
            dest2 = ''
        if 'append_source' in aJob:
            apDest = aJob['append_source']
        else:
            apDest = ''
        if 'query2' in aJob:
            query2 = aJob['query2']
        else:
            query2 = ''

        if source not in shared.connections:
            logging.processError(p_message=f'data source [{source}] not declared on connections.csv. giving up.', p_stop=True, p_exitCode=4)
            return

        if connections.getConnectionParameter(source, 'driver') == 'csv':
            logging.processError(p_message=f"ERROR: csv driver requested as source on [{source}], but it's only available as destination yet. giving up.", p_stop=True, p_exitCode=4)
            return

        if connections.getConnectionParameter(dest, 'driver') == 'csv':
            if 'insert_cols' in aJob:
                if aJob['insert_cols'] == '@d':
                    logging.processError(p_message=f'cannot infer (yet) insert cols from a csv destination (jobs line{key+1}). giving up.', p_stop=True, p_exitCode=4)
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

        if 'pre_query_src' in aJob:
            preQuerySrc = aJob['pre_query_src']
            if len(preQuerySrc) > 0 and preQuerySrc[0] == '@':
                preCheckSqlFile(preQuerySrc[1:], 'pre-query on source')

        if 'pre_query_dst' in aJob:
            preQueryDst = aJob['pre_query_dst']
            if len(preQueryDst) >0 and preQueryDst[0] == '@':
                preCheckSqlFile(preQueryDst[1:], 'pre-query on destination')

        if 'regexes' in aJob:
            regex = aJob['regexes']
            if len(regex) > 0 and regex[0] == '@':
                if not os.path.isfile(regex[1:]):
                    logging.processError(p_message=f'regex file [{regex[1:]}] does not exist! giving up.', p_stop=True, p_exitCode=4)
                    return

        if mode.upper() == 'A':
            if 'append_column' in aJob:
                apCol = aJob['append_column']
                if len(apCol) == 0:
                    logging.processError(p_message='append column is empty! giving up.', p_stop=True, p_exitCode=4)
                    return
            else:
                logging.processError(p_message='Mode A set, but append column not specified! giving up.', p_stop=True, p_exitCode=4)
                return


        if 'append_query' in aJob:
            apQuery = aJob['append_query']
            if len(apQuery) > 0 and apQuery[0] == '@':
                if not os.path.isfile(apQuery[1:]):
                    logging.processError(p_message=f'append query file [{apQuery[1:]}] does not exist! giving up.', p_stop=True, p_exitCode=4)
                    return

        #prebuild the jobName used in logs and stats
        aJob['jobName'] =  f'{key}-{source}-{dest}-{aJob["table"]}'

    #buffer = StringIO() ; print(shared.jobs, file=buffer, flush=True); logging.logPrint(f'final jobs data:\n{buffer.getvalue()}\n', logLevel.DEBUG)
    logging.logPrint(f'final jobs data:\n{json.dumps(shared.jobs, indent=2)}\n', logLevel.DEBUG)

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
        return ''
