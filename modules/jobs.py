''' job configuration and validation'''

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

        self.source:str = thisJobData['source']
        self.sourceDriver:str = str(connections.getConnectionParameter(self.source, 'driver'))

        if 'key_source' in thisJobData:
            self.key_source = thisJobData['key_source']
            self.key_sourceDriver:str = str(connections.getConnectionParameter(self.key_source, 'driver'))
        else:
            self.key_source:str = ''
            self.key_sourceDriver:str = ''

        self.dest = thisJobData['dest']
        self.destDriver:str = str(connections.getConnectionParameter(self.dest, 'driver'))

        self.mode = thisJobData['mode']
        self.query = thisJobData['query']
        if 'key_query' in thisJobData:
            self.key_query = thisJobData['key_query']
        else:
            self.key_query = ''
        self.preCmdSrc = ''
        self.preCmdDst = ''

        if 'jobName' in thisJobData:
            self.jobName = thisJobData['jobName']
        else:
            if self.mode.upper() == 'E':
                if len(self.query) > 0 and self.query[0] == '@':
                        statementName:str = self.query[1:]
                else:
                    statementName:str = 'ExecuteStatement'

                self.jobName = f'{p_jobID}-{thisJobData["source"]}-{statementName}'
            else:
                self.jobName = f'{p_jobID}-{thisJobData["source"]}-{thisJobData["dest"]}-{thisJobData["table"]}'

        siObjSep = connections.getConnectionParameter(self.dest, 'insert_object_delimiter')

        if len(self.query) > 0 and self.query[0] == '@':
            self.query = _readSqlFile(self.query[1:])
        if len(self.key_query) > 0 and self.key_query[0] == '@':
                self.key_query = _readSqlFile(self.key_query[1:])

        if 'src_pre_cmd' in thisJobData:
            self.preCmdSrc = thisJobData['src_pre_cmd']
            if len(self.preCmdSrc) > 0 and self.preCmdSrc[0]  == '@':
                self.preCmdSrc = _readSqlFile(self.preCmdSrc[1:])

        if 'dst_pre_cmd' in thisJobData:
            self.preCmdDst = thisJobData['dst_pre_cmd']
            if len(self.preCmdDst) > 0 and self.preCmdDst[0]  == '@':
                self.preCmdDst = _readSqlFile(self.preCmdDst[1:])

        if 'regexes' in thisJobData:
            regexes = thisJobData['regexes']
            if len(regexes)>0:
                if regexes[0] == '@':
                    with open(regexes[1:], 'r', encoding = 'utf-8') as file:
                        regexes = file.read().split('\n')
                else:
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
                        self.key_query = re.sub( r[0], r[1], self.key_query )
                        self.preCmdSrc = re.sub( r[0], r[1], self.preCmdSrc )
                        self.preCmdDst = re.sub( r[0], r[1], self.preCmdDst )

        self.table = thisJobData['table']

        if 'fetch_size' in thisJobData:
            qFetchSize = int(thisJobData['fetch_size'])
            if qFetchSize == 0:
                self.fetchSize = shared.defaultFetchSize
            else:
                self.fetchSize = qFetchSize
        else:
            self.fetchSize = shared.defaultFetchSize

        if 'key_fetch_size' in thisJobData:
            qkFetchSize = int(thisJobData['key_fetch_size'])
            if qkFetchSize == 0:
                self.key_fetchSize:int = shared.defaultFetchSize
            else:
                self.key_fetchSize:int = qkFetchSize
        else:
            self.key_fetchSize = shared.defaultFetchSize

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

        #logging.logPrint(f'source=[{source}], key_source=[{key_source}], dest=[{dest}], preCmdSrc=[{preCmdSrc}] preCmdDst=[{preCmdDst}] table=[{table}] closeStream=[{bCloseStream}], CSVEncodeSpecial=[{bCSVEncodeSpecial}], appendKeyColumn=[{appendKeyColumn}], getMaxQuery=[{getMaxQuery}]', logLevel.DEBUG, p_jobID=p_jobID)
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


def load(p_filename:str) -> dict[int, dict[str, Any]]:
    '''loads job file into memory'''

    j:dict[int, dict[str, Any]] = {}
    try:
        j:dict[int, dict[str, Any]] = utils.read_csv_config(p_filename, emptyValuesDefault='', sequencialLineNumbers=True)
    except Exception as error:
        logging.processError(p_e=error, p_message=f'Loading [{p_filename}]', p_stop=True, p_exitCode=3)
        return {}

    logging.logPrint(f'raw loaded jobs file:\n{json.dumps(j, indent=2)}\n', logLevel.DEBUG)
    return j

def preCheck(raw_jobs:dict[int, dict[str, Any]]) -> dict[int, dict[str, Any]]:
    '''checks that config files are consistent'''

    logging.logPrint('checking sources and destinations...')

    allJobs:dict[int, dict[str, Any]] = {}

    for key in raw_jobs.keys():
        aJob=raw_jobs[key]
        for ecol in expected_query_columns:
            if ecol not in aJob:
                logging.processError(p_message=f'[{key}]: Missing column on queries file: [{ecol}]', p_stop=True, p_exitCode=4)
                return {}
        source = aJob['source']
        sourceDriver = connections.getConnectionParameter(source, 'driver')
        dest = aJob['dest']
        destDriver = connections.getConnectionParameter(dest, 'driver')
        mode = aJob['mode']
        query = aJob['query']

        if 'key_source' in aJob:
            key_source = aJob['key_source']
        else:
            key_source = ''

        if 'append_source' in aJob:
            apDest = aJob['append_source']
        else:
            apDest = ''

        if 'key_query' in aJob:
            key_query = aJob['key_query']
        else:
            key_query = ''

        if source not in shared.connections:
            logging.processError(p_message=f'data source [{source}] not declared on connections. giving up.', p_stop=True, p_exitCode=4)
            return {}

        if key_source != '' and key_source not in shared.connections:
            logging.processError(p_message=f'keys source [{key_source}] not declared on connections. giving up.', p_stop=True, p_exitCode=4)
            return {}

        if sourceDriver == 'csv':
            if shared.GENERATE_CREATE_TABLES:
                logging.processError(p_message=f'cannot generate create table statements from CSV sources (jobs line {key+1}), giving up.', p_stop=True, p_exitCode=4)
                return {}
            if 'insert_cols' in aJob:
                if aJob['insert_cols'] in ('' , '@' , '@l' , '@u'):
                    logging.processError(p_message=f'cannot infer (yet) insert cols from a csv source (jobs line {key+1}). giving up.', p_stop=True, p_exitCode=4)
                    return {}

        if dest not in shared.connections and mode.upper() != 'E':
            logging.processError(p_message=f'data destination [{dest}] not declared on connections. giving up.', p_stop=True, p_exitCode=4)
            return {}

        if destDriver == 'csv':
            if shared.GENERATE_CREATE_TABLES:
                logging.processError(p_message=f'cannot generate create table statements for CSV destination (jobs line {key+1}), giving up.', p_stop=True, p_exitCode=4)
                return {}
            if 'insert_cols' in aJob:
                if aJob['insert_cols'] == '@d':
                    logging.processError(p_message=f'cannot infer (yet) insert cols from a csv destination (jobs line {key+1}). giving up.', p_stop=True, p_exitCode=4)
                    return {}

        if key_source != '' and sourceDriver == 'csv':
            logging.processError(p_message='Query and SubQuery mode does not support main data from csv (yet), only for keys. giving up.')
            return {}

        if len(apDest) > 0:
            if apDest not in shared.connections:
                logging.processError(p_message=f'append source [{apDest}] not declared on connections. giving up.', p_stop=True, p_exitCode=4)
                return {}

        if len(query) > 0:
            if query[0] == '@':
                if not _preCheckSqlFile(query[1:], 'query'): return {}
        else:
            logging.processError(p_message='query cannot be empty!! giving up.', p_stop=True, p_exitCode=4)
            return {}

        if len(key_query) > 0:
            if key_query[0] == '@':
                if not _preCheckSqlFile(key_query[1:], 'key_query'): return {}

            if len(key_source) > 0:
                if key_source not in shared.connections:
                    logging.processError(p_message=f'key_query specified, but key_source [{key_source}] not declared on connections. giving up.', p_stop=True, p_exitCode=4)
                    return {}
            else:
                logging.processError(p_message=f'key_query specified, but no key_source specified on connections. giving up.', p_stop=True, p_exitCode=4)
                return {}

        if 'src_pre_cmd' in aJob:
            preCmdSrc = aJob['src_pre_cmd']
            if len(preCmdSrc) > 0 and preCmdSrc[0] == '@':
                if not _preCheckSqlFile(preCmdSrc[1:], 'pre-cmd on source'): return {}

        if 'key_pre_cmd' in aJob:
            preCmdSrc2 = aJob['key_pre_cmd']
            if len(preCmdSrc2) > 0 and preCmdSrc2[0] == '@':
                if not _preCheckSqlFile(preCmdSrc2[1:], 'key_pre_cmd on key_source'): return {}

            if len(key_source) > 0:
                if key_source not in shared.connections:
                    logging.processError(p_message=f'key_pre_cmd specified, but key_source [{key_source}] not declared on connections. giving up.', p_stop=True, p_exitCode=4)
                    return {}
            else:
                logging.processError(p_message=f'key_pre_cmd specified, but no key_source specified on connections. giving up.', p_stop=True, p_exitCode=4)
                return {}

        if 'dst_pre_cmd' in aJob:
            preCmdDst = aJob['dst_pre_cmd']
            if len(preCmdDst) >0 and preCmdDst[0] == '@':
                if not _preCheckSqlFile(preCmdDst[1:], 'pre-cmd on destination'): return {}

        if 'regexes' in aJob:
            regex = aJob['regexes']
            if len(regex) > 0 and regex[0] == '@':
                if not os.path.isfile(regex[1:]):
                    logging.processError(p_message=f'regex file [{regex[1:]}] does not exist! giving up.', p_stop=True, p_exitCode=4)
                    return {}

        if mode.upper() == 'A':
            if 'append_column' in aJob:
                apCol = aJob['append_column']
                if len(apCol) == 0:
                    logging.processError(p_message='append column is empty! giving up.', p_stop=True, p_exitCode=4)
                    return {}
            else:
                logging.processError(p_message='Mode A set, but append column not specified! giving up.', p_stop=True, p_exitCode=4)
                return {}

        if 'append_query' in aJob:
            apQuery = aJob['append_query']
            if len(apQuery) > 0 and apQuery[0] == '@':
                if not os.path.isfile(apQuery[1:]):
                    logging.processError(p_message=f'append query file [{apQuery[1:]}] does not exist! giving up.', p_stop=True, p_exitCode=4)
                    return {}

        #prebuild the jobName used in logs and stats

        if mode.upper() == 'E':
            if len(query) > 0 and query[0] == '@':
                    statementName:str = query[1:]
            else:
                statementName:str = 'ExecuteStatement'

            aJob['jobName'] = f'{key}-{source}-{statementName}'
        else:
            aJob['jobName'] = f'{key}-{source}-{dest}-{aJob["table"]}'


        allJobs[key]=aJob
    #buffer = StringIO() ; print(shared.jobs, file=buffer, flush=True); logging.logPrint(f'final jobs data:\n{buffer.getvalue()}\n', logLevel.DEBUG)
    logging.logPrint(f'final jobs data:\n{json.dumps(allJobs, indent=2)}\n', logLevel.DEBUG)

    return allJobs

### PRIVATE STUFF

def _readSqlFile(p_filename:str, p_parentFilename:str = '') -> str:
    '''Reads a SQL file, handling #include directives.'''
    logging.logPrint(f'reading file [{p_filename}] (ref from [{p_parentFilename}])', logLevel.DEBUG)
    buffer = ''
    try:
        with open(p_filename, 'r', encoding = 'utf-8') as file:
            for line in file:
                if line.startswith('#include '):
                    # Extract the filename from the #include directive
                    include_filename = line[9:].strip()
                    # Recursively call _readSqlFile to read the included file
                    buffer += _readSqlFile(include_filename, p_filename)
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

def _preCheckSqlFile(p_filename:str, p_errorTitle:str) -> bool:
    '''checks that a sql file exists, considering #include directives, and is not empty'''
    logging.logPrint(f'checking {p_errorTitle} file [{p_filename}]...', logLevel.DEBUG)
    if not os.path.isfile(p_filename):
        logging.processError(p_message=f'query file [{p_filename}] does not exist! giving up.', p_stop=True, p_exitCode=4)
        return False
    else:
        buffer = _readSqlFile(p_filename)
        if len(buffer) == 0:
            logging.processError(p_message=f'{p_errorTitle} file [{p_filename}] is empty! giving up.', p_stop=True, p_exitCode=4)
            return False
    return True
