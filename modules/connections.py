'''connections handling stuff'''

import os
import csv

import json

from typing import Any, Optional

import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.shared as shared
import modules.utils as utils

expected_conns_columns_db = ('name','driver','server','database','user','password')
expected_conns_columns_csv = ('name','driver','paths','delimiter','quoting')

check_bd_version_cmd:dict[str, str] = {
    'psycopg2':     'SELECT version()',
    'mysql':        'SELECT version()',
    'mariadb':      'SELECT version()',
    'csv':          '',
    'oracledb':    'SELECT * FROM V$VERSION',
    'pyodbc':       'SELECT @@version',
    'databricks':   'SELECT current_version()',
    '':             ''
}

#to use with .format()
change_schema_cmd:dict[str, str] = {
    'psycopg2':     'SET search_path TO {0}',
    'mysql':        'USE {0}',
    'mariadb':      'USE {0}',
    'csv':          '',
    'oracledb':    'ALTER SESSION SET CURRENT_SCHEMA = {0}}',
    'pyodbc':       'USE {0}',
    'databricks':   '',
    '':             ''
}

#to use with .format()
change_timeout_cmd:dict[str, str] = {
    'psycopg2':     "SET statement_timeout = '{0}s'",
    'mysql':        'SET SESSION wait_timeout = {0}',
    'mariadb':      'SET SESSION wait_timeout = {0}',
    'csv':          '',
    'oracledb':    '',
    'pyodbc':       '',
    'databricks':   '',
    '':             ''
}

insert_objects_delimiter:dict[str, str] = {
    'psycopg2':     '"',
    'mysql':        '`',
    'mariadb':      '`',
    'csv':          '',
    'oracledb':    '"',
    'pyodbc':       '"',
    'databricks':    '`',
    '':             ''
}

database_name_for_llm:dict[str, str] = {
    'psycopg2':     'postgres',
    'mysql':        'mysql',
    'mariadb':      'mariadb',
    'csv':          '',
    'oracledb':    'oracle',
    'pyodbc':       'microsoft sql server',
    'databricks':    'databricks',
    '':             ''
}

csv_quoting_decoder:dict[str, int] = {
    'ALL':          csv.QUOTE_ALL,
    'MINIMAL':      csv.QUOTE_MINIMAL,
    'NONE':         csv.QUOTE_NONE,
    'NONNUMERIC':   csv.QUOTE_NONNUMERIC
}

def oracledb_OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    import oracledb
    # Map Oracle LOB types to Python native types for eager loading
    if defaultType == oracledb.DB_TYPE_CLOB:
        # Pylance likes 'str' or oracledb.STRING
        return cursor.var(str, arraysize=cursor.arraysize)
    if defaultType == oracledb.DB_TYPE_BLOB:
        # Pylance likes 'bytes' or oracledb.BINARY
        return cursor.var(bytes, arraysize=cursor.arraysize)

def load(p_filename:str) -> dict[int, dict[str, Any]]:
    '''
    load connections file into memory
    '''

    c:dict[int, dict[str, Any]] = {}
    try:
        c=utils.read_csv_config(p_filename, emptyValuesDefault='', sequencialLineNumbers=True)
    except Exception as e:
        logging.processError(p_e=e, p_message=f'Loading [{p_filename}]', p_stop=True, p_exitCode=1)
        return {}

    logging.logPrint(f'raw loaded connections file:\n{json.dumps(c, indent=2)}\n', logLevel.DEBUG)

    return c

def preCheck(raw_connections:dict[int, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    '''
    pre-check connections file
    '''

    conns:dict[str, Any] = {}

    for key in raw_connections.keys():
        aConn:dict[str, Any] = raw_connections[key]
        cName = aConn['name']

        if cName in conns:
            logging.processError(p_message=f'[{cName}]: Duplicate connection name in line [{key}]', p_stop=True, p_exitCode=1)
            return {}

        if aConn['driver'] == 'csv':
            for ecol in expected_conns_columns_csv:
                if ecol not in aConn:
                    logging.processError(p_message=f'[{cName}]: Missing value on connections file: [{ecol}]', p_stop=True, p_exitCode=1)
                    return {}
            nc = {
                'driver':       aConn['driver'],
                'paths':        aConn['paths'],
                'delimiter':    aConn['delimiter'],
                'quoting':      aConn['quoting'],
                }
        else:
            for ecol in expected_conns_columns_db:
                if ecol not in aConn:
                    logging.processError(p_message=f'[{cName}]: Missing value on connections file: [{ecol}]', p_stop=True, p_exitCode=1)
                    return {}
            if aConn['driver'] == 'cx_Oracle':
                logging.logPrint('Warning: cx_Oracle driver was replaced by oracledb. Please update your connections file.')
                aConn['driver'] = 'oracledb'
            if 'trustservercertificate' in aConn:
                sTSC = aConn['trustservercertificate']
            else:
                sTSC = 'no'
            sIP=''
            if 'override_insert_placeholder' in aConn:
                sIP = aConn['override_insert_placeholder']
            else:
                sIP = '%s'

            if os.getenv('ADD_NAMES_DELIMITERS','no') == 'yes':
                sOD = insert_objects_delimiter[aConn['driver']]
            else:
                if 'insert_objects_delimiter' in aConn:
                    sOD = utils.delimiter_decoder(aConn['insert_objects_delimiter'])
                else:
                    sOD = ''

            if 'schema' in aConn:
                sSchema = aConn['schema']
            else:
                sSchema = ''

            nc = {
                'driver':                   aConn['driver'],
                'server':                   aConn['server'],
                'database':                 aConn['database'],
                'user':                     aConn['user'],
                'password':                 aConn['password'],
                'trustservercertificate':   sTSC,
                'insert_placeholder':       sIP,
                'insert_object_delimiter':  sOD,
                'schema':                   sSchema
            }

        conns[cName] = nc

    logging.logPrint(f'final connections data:\n{json.dumps(conns, indent=2)}\n', logLevel.DEBUG)
    return conns

def initConnections(p_name:str, p_readOnly:bool, p_qtd:int, p_tableName = '', p_mode = 'w', p_test_mode:bool=False) -> Optional[dict[int, Any]]:
    ''' creates connection objects to sources or destinations
        returns an array of connections, if connecting to databases, or an array of tupples of (file, stream), if driver == csv
    '''

    logging.logPrint(f'called, name=[{p_name}], qtd=[{p_qtd}], readOnly={p_readOnly}, tableName=[{p_tableName}], mode=[{p_mode}], p_test_mode={p_test_mode}', logLevel.DEBUG)
    nc:dict[int, Any] = {}
    c = shared.connections[p_name]

    logging.logPrint(f'({p_name}): trying to connect...', logLevel.DEBUG, reportFrom=True)

    match c['driver']:
        case 'pyodbc':
            try:
                import pyodbc
                for x in range(p_qtd):
                    # parameters in string because if added as independent parameters, it segfaults
                    # used to be:
                    #nc[x]=pyodbc.connect(driver='{ODBC Driver 18 for SQL Server}', server=c['server'], database=c['database'], user=c['user'], password=c['password'], encoding = 'UTF-8', nencoding = 'UTF-8', readOnly = p_readOnly, trustservercertificate = c['trustservercertificate'] )
                    nc[x]=pyodbc.connect(f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={c['server']};DATABASE={{{c['database']}}};UID={{{c['user']}}};PWD={{{c['password']}}};ENCODING=UTF-8;TRUSTSERVERCERTIFICATE={c['trustservercertificate']};APP={shared.applicationName}")
                    try:
                        nc[x].timeout = shared.idleTimeoutSecs
                    except Exception as e:
                        logging.logPrint(f'({p_name}): exception [{e}] happened while trying to set timeout to [{shared.idleTimeoutSecs}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production mode we comment the previous line
            except (Exception, pyodbc.DatabaseError) as e: # type:ignore
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return None

        case 'oracledb':
            try:
                import oracledb
                for x in range(p_qtd):
                    try:
                        nc[x]=oracledb.connect(
                            user=c['user'],
                            password=c['password'],
                            dsn=f"{c['server']}/{c['database']}"
                        )
                    except oracledb.Error as e:
                        # Check if error is specifically "Unsupported version for Thin mode" (DPY-3010)
                        if "DPY-3010" in str(e):
                            logging.logPrint("Oracle driver: Falling back to Thick Mode for legacy database...")
                            try:
                                # Initialize once per application lifecycle
                                oracledb.init_oracle_client(lib_dir="/opt/instantclient")
                            except oracledb.ProgrammingError:
                                pass # Already initialized

                            # Retry connection in Thick mode
                            nc[x]=oracledb.connect(
                                user=c['user'],
                                password=c['password'],
                                dsn=f"{c['server']}/{c['database']}"
                            )
                        else:
                            raise e
                    nc[x].outputtypehandler = oracledb_OutputTypeHandler

                    try:
                        nc[x].client_identifier=shared.applicationName
                    except Exception as e:
                        logging.logPrint(f'({p_name}): could not set client_identifier on connection: [{e}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production mode we comment the previous line
                    try:
                        nc[x].call_timeout = (shared.idleTimeoutSecs * 1000) # in milisecs
                    except Exception as e:
                        logging.logPrint(f'({p_name}): exception [{e}] happened while trying to set call_timeout to [{shared.idleTimeoutSecs}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production mode we comment the previous line

            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return None

        case 'psycopg2':
            try:
                from psycopg2 import pool as pgpools
                tpool = pgpools.ThreadedConnectionPool(
                    1,
                    p_qtd,
                    host=c['server'],
                    database=c['database'],
                    user=c['user'],
                    password = c['password'],
                    application_name=shared.applicationName,
                    connect_timeout = shared.connectionTimeoutSecs
                )
                for x in range(p_qtd):
                    nc[x] = tpool.getconn()
                    nc[x].readonly = p_readOnly
            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return None

        case 'mysql':
            try:
                import mysql.connector
                for x in range(p_qtd):
                    nc[x]=mysql.connector.connect(
                        host=c['server'],
                        database=c['database'],
                        user=c['user'],
                        password = c['password'],
                        connect_timeout = shared.connectionTimeoutSecs

                    )
                    try:
                        nc[x]._client_name = shared.applicationName
                    except Exception as e:
                        logging.logPrint(f'({p_name}): could not set client_name on connection: [{e}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production mode we comment the previous line
            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return None

        case 'mariadb':
            try:
                import mariadb
                for x in range(p_qtd):
                    nc[x]=mariadb.connect(
                        host=c['server'],
                        database=c['database'],
                        user=c['user'],
                        password = c['password'],
                        connect_timeout = shared.connectionTimeoutSecs
                    )
                    try:
                        nc[x]._client_name = shared.applicationName
                    except Exception as e:
                        logging.logPrint(f'({p_name}): could not set client_name on connection: [{e}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production mode we comment the previous line
            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return None

        case 'databricks':
            #https://docs.databricks.com/en/dev-tools/python-sql-connector.html#auth-m2m
            try:

                import databricks.sql as dbricksSql

                server_name=c['server'].split('/')[0]
                client_id=c['user']
                client_secret=c['password']

                logging.logPrint(f'({p_name}): databricks auth: trying to get OAuth SP with client_id=[{client_id}], client_secret [{client_secret}], server_name=[{server_name}]', logLevel.DEBUG, reportFrom=True)
                def dbricks_connection_provider():
                    from databricks.sdk.core import Config as dbricksConfig
                    from databricks.sdk.core import oauth_service_principal as dbricksOauthSP
                    try:
                        dbricksOauthConfig = dbricksConfig(
                            host          = f'https://{server_name}',
                            client_id     = client_id,
                            client_secret = client_secret
                        )

                        return dbricksOauthSP(dbricksOauthConfig)
                    except Exception as e:
                        logging.processError(p_e=e, p_message=f'({p_name}): databricks auth, dbricks_connection_provider')

                try:
                    token = dbricks_connection_provider().oauth_token()
                    #access_token = token.access_token

                except Exception as e:
                    logging.processError(p_e=e, p_message=f'({p_name}): databricks auth, trying to retrieve Token', p_stop=True, p_exitCode=2)
                    return None

                logging.logPrint(f'({p_name}): databricks auth: got Token: [{token}]', logLevel.DEBUG, reportFrom=True)

                for x in range(p_qtd):
                    logging.logPrint(f'({p_name}): databricks[{x}]: establishing connection...', logLevel.DEBUG, reportFrom=True)
                    nc[x]=dbricksSql.connect(
                        server_hostname=c['server'],
                        http_path=c['database'],
                        credentials_provider = dbricks_connection_provider,
                        #access_token = access_token,
                        client_name=shared.applicationName,
                        connection_timeout = shared.connectionTimeoutSecs,
                        query_timeout=shared.idleTimeoutSecs,
                        http_timeout=shared.idleTimeoutSecs
                    )
                    logging.logPrint(f'({p_name}): databricks[{x}]: connected.', logLevel.DEBUG, reportFrom=True)
            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return None

        case 'csv':
            try:
                _delim = utils.delimiter_decoder(c['delimiter'])
                logging.logPrint(f'({p_name}): csv delimiter set to [{_delim}]', logLevel.DEBUG, reportFrom=True)
            except Exception:
                if len(c['delimiter']) == 1:
                    _delim = c['delimiter']
                else:
                    _delim = ','
            try:
                _quote=csv_quoting_decoder[c['quoting']]
            except Exception:
                _quote=csv.QUOTE_MINIMAL

            csv.register_dialect(p_name, delimiter = _delim, quoting = _quote)
            logging.logPrint(f'({p_name}): registering csv dialect with delim=[{_delim}], quoting=[{_quote}]', logLevel.DEBUG, reportFrom=True)

            if p_readOnly:
                # connections for readers
                if 'paths' in c:
                    _paths=c['paths'].split('|')
                else:
                    _paths = ('.')
                sFileName = os.path.join(_paths[0], p_tableName)
                logging.logPrint(f'({p_name}): opening for reading, file=[{sFileName}], mode=[{p_mode}]', logLevel.DEBUG, reportFrom=True)
                fileHandle = open(sFileName, p_mode, encoding = 'utf-8')
                newStream = csv.reader(fileHandle, dialect = p_name)
                nc[0] = (fileHandle, newStream)
            else:
                # connections for writers
                if 'paths' in c:
                    _paths=c['paths'].split('|')
                else:
                    _paths = ('.')

                for _path in _paths:
                    if not os.path.isdir(_path):
                        try:
                            os.makedirs(name=_path)
                        except Exception as e:
                            logging.processError(p_message=f'({p_name}): directory does not exist, and exception happened when trying to create it [{_path}]', p_stop=True, p_exitCode=2)
                            return None

                sFileName = ''
                logging.logPrint(f'({p_name}): dumping CSV files to {_paths}', logLevel.DEBUG, reportFrom=True)
                try:
                    if p_qtd > 1:
                        ipath=0
                        for x in range(p_qtd):
                            sFileName = os.path.join(_paths[ipath], f'{p_tableName}_{x+1}.csv')
                            if ipath<len(_paths)-1:
                                ipath += 1
                            else:
                                ipath = 0
                            logging.logPrint(f'({p_name}): opening file=[{sFileName}], mode=[{p_mode}]', logLevel.DEBUG, reportFrom=True)
                            newFile = open(sFileName, p_mode, encoding = 'utf-8')
                            newStream = csv.writer(newFile, dialect = p_name)
                            nc[x] = (newFile, newStream)
                    else:
                        sFileName = os.path.join(_paths[0], f'{p_tableName}.csv')
                        logging.logPrint(f'({p_name}): opening file=[{sFileName}], mode=[{p_mode}]', logLevel.DEBUG, reportFrom=True)
                        newFile = open(sFileName, p_mode, encoding = 'utf-8')
                        newStream = csv.writer(newFile, dialect = p_name)
                        nc[0] = (newFile, newStream)
                except Exception as error:
                    logging.logPrint(f'({p_name}): CSV error [{error}] opening file [{sFileName}]')

    # all connections: change schemas if applicable, and set timeouts.
    for x in range(p_qtd):
        if 'schema' in c:
            s:str = c['schema']
            if len(s) > 0:
                sql = change_schema_cmd[c['driver']].format(s)
                if len(sql) > 0:
                    try:
                        logging.logPrint(f'({p_name}[{x}]): setting schema with [{sql}]', logLevel.INFO, reportFrom=True)
                        nc[x].cursor().execute(sql)
                    except Exception as e:
                        logging.processError(p_e=e,p_message=f'({p_name}[{x}]): happened while trying to set schema', p_stop=True)

        if shared.idleTimeoutSecs > 0:
            sql:str = change_timeout_cmd[c['driver']].format(shared.idleTimeoutSecs)
            if len(sql) > 0:
                try:
                    logging.logPrint(f'({p_name}[{x}]): setting timeout with [{sql}]', logLevel.INFO, reportFrom=True)
                    nc[x].cursor().execute(sql)
                except Exception as e:
                    logging.processError(p_e=e, p_message=f'({p_name}[{x}]): happened while trying to set timeout', p_stop=True)

    try:
        if p_test_mode:
            connTestLogLevel = logLevel.INFO
            reportFrom = False
        else:
            connTestLogLevel = logLevel.DEBUG
            reportFrom = True

        sGetVersion = check_bd_version_cmd[c['driver']]
        if len(sGetVersion) > 0:
            cur = nc[0].cursor() # type: ignore
            logging.logPrint(f'({p_name}): testing connection, getting version with [{sGetVersion}]...', logLevel.DEBUG, reportFrom=True)
            cur.execute(sGetVersion)
            db_version = cur.fetchone()
            logging.logPrint(f'({p_name}): ok, connected to DB version: {db_version}', connTestLogLevel, reportFrom=reportFrom)
            if not p_test_mode:
                logging.logPrint(f'({p_name}): connected')
            cur.close()
    except Exception as e:
        logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
        return None

    return nc

def getConnectionParameter(p_name:str, p_otion:str):
    '''gets connection option'''

    if p_name in shared.connections:
        c = shared.connections[p_name]
        if p_otion in c:
            return c[p_otion]
        else:
            return None
    else:
        return None

def initCursor(p_conn, p_jobID:int, p_source:str, p_fetchSize:int):
    '''prepares the object that will send commands to databases'''
    # postgres: try not to fetch all rows to memory, using server side cursors
    # mysql, mariaDB: use unbuffered cursors

    try:
        logging.logPrint(f'({p_source}): trying to get server side cursor...', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
        newCursor = p_conn.cursor(name = f'jobid-{p_jobID}')
        logging.logPrint(f'({p_source}): got server side cursor!', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
    except:
        logging.logPrint(f'({p_source}): server side cursor did not work, trying to get an unbuffered cursor', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
        try:
            newCursor = p_conn.cursor(buffered=False)
            logging.logPrint(f'({p_source}): got unbuffered cursor!', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
        except:
            logging.logPrint(f'({p_source}): unbuffered cursor did not work, getting a normal cursor', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
            newCursor = p_conn.cursor()
            logging.logPrint(f'({p_source}): got a normal cursor.', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
    try:
        #current only works on postgres...
        newCursor.itersize = p_fetchSize
        logging.logPrint(f'({p_source}): set cursor itersize to [{p_fetchSize}]', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
    except Exception as error:
        logging.logPrint(f'({p_source}): could not set cursor itersize: [{error}]', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
        pass #do not remove as on production mode we comment the previous line

    return newCursor

def testConnections():
    '''test all connections'''
    logging.logPrint('testing connections:')
    for key in shared.connections.keys():
        try:
            testConn=initConnections(p_name=key, p_readOnly=True, p_qtd=1, p_test_mode=True)[0] # type: ignore
        except:
            pass
