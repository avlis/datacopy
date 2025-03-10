'''connections handling stuff'''

#pylint: disable=invalid-name, broad-except, import-outside-toplevel, c-extension-no-member, line-too-long

import os
import csv

import json

from typing import Any

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
    'cx_Oracle':    'SELECT * FROM V$VERSION',
    'pyodbc':       'SELECT @@version',
    'databricks':   'SELECT current_version()',
    '':             ''
}

#to use with .format()
change_schema_cmd:dict[str, str] = {
    'psycopg2':     'SET search_path TO {0};',
    'mysql':        'USE {0}',
    'mariadb':      'USE {0}',
    'csv':          '',
    'cx_Oracle':    'ALTER SESSION SET CURRENT_SCHEMA = {0}}',
    'pyodbc':       'USE {0}',
    'databricks':   '',
    '':             ''
}

insert_objects_delimiter = {
    'psycopg2':     '"',
    'mysql':        '`',
    'mariadb':      '`',
    'csv':          '',
    'cx_Oracle':    '"',
    'pyodbc':       '"',
    'databricks':    '`',
    '':             ''
}

csv_quoting_decoder = {
    'ALL':          csv.QUOTE_ALL,
    'MINIMAL':      csv.QUOTE_MINIMAL,
    'NONE':         csv.QUOTE_NONE,
    'NONNUMERIC':   csv.QUOTE_NONNUMERIC
}

def cx_Oracle_OutputTypeHandler(cursor, name, defaultType, size, precision, scale): # pylint: disable=unused-argument
    '''oracle custom stuff'''
    import cx_Oracle
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize = cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize = cursor.arraysize)

def loadConnections(p_filename:str):
    '''
    load connections file into memory
    '''

    conns:dict[str, Any] = {}
    try:
        c=utils.read_csv_config(p_filename, emptyValuesDefault='', sequencialLineNumbers=True)
    except Exception as e:
        logging.processError(p_e=e, p_message=f'Loading [{p_filename}]', p_stop=True, p_exitCode=1)
        return

    logging.logPrint(f'raw loaded connections file:\n{json.dumps(c, indent=2)}\n', logLevel.DEBUG)

    for key in c.keys():
        aConn:dict[str, Any] = c[key]
        cName = aConn['name']

        if cName in conns:
            logging.processError(p_message=f'[{cName}]: Duplicate connection name in line [{key}]', p_stop=True, p_exitCode=1)
            return

        if aConn['driver'] == 'csv':
            for ecol in expected_conns_columns_csv:
                if ecol not in aConn:
                    logging.processError(p_message=f'[{cName}]: Missing value on connections file: [{ecol}]', p_stop=True, p_exitCode=1)
                    return
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
                    return
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

    shared.connections = conns
    logging.logPrint(f'final connections data:\n{json.dumps(conns, indent=2)}\n', logLevel.DEBUG)

def initConnections(p_name:str, p_readOnly:bool, p_qtd:int, p_tableName = '', p_mode = 'w') -> dict[int, Any]:
    ''' creates connection objects to sources or destinations
        returns an array of connections, if connecting to databases, or an array of tupples of (file, stream), if driver == csv
    '''

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
                        pass #do not remove as on production we delete the previous line
            except (Exception, pyodbc.DatabaseError) as e: # type:ignore
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return {}

        case 'cx_Oracle':
            try:
                import cx_Oracle
                for x in range(p_qtd):
                    nc[x]=cx_Oracle.connect(
                        c['user'],
                        c['password'],
                        f"{c['server']}/{c['database']}",
                        encoding = 'UTF-8',
                        nencoding = 'UTF-8'
                    )
                    nc[x].outputtypehandler = cx_Oracle_OutputTypeHandler
                    try:
                        nc[x].client_identifier=shared.applicationName
                    except Exception as e:
                        logging.logPrint(f'({p_name}): could not set client_identifier on connection: [{e}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production we delete the previous line
                    try:
                        nc[x].call_timeout = (shared.idleTimeoutSecs * 1000) # in milisecs
                    except Exception as e:
                        logging.logPrint(f'({p_name}): exception [{e}] happened while trying to set call_timeout to [{shared.idleTimeoutSecs}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production we delete the previous line

            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return {}

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
                return {}

        case 'mysql':
            try:
                import mysql.connector
                for x in range(p_qtd):
                    nc[x]=mysql.connector.connect(
                        host=c['server'],
                        database=c['database'],
                        user=c['user'],
                        password = c['password'],
                        connection_timeout = shared.connectionTimeoutSecs

                    )
                    try:
                        nc[x]._client_name = shared.applicationName
                    except Exception as e:
                        logging.logPrint(f'({p_name}): could not set client_name on connection: [{e}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production we delete the previous line
            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return {}

        case 'mariadb':
            try:
                import mariadb
                for x in range(p_qtd):
                    nc[x]=mariadb.connect(
                        host=c['server'],
                        database=c['database'],
                        user=c['user'],
                        password = c['password'],
                        connection_timeout = shared.connectionTimeoutSecs
                    )
                    try:
                        nc[x]._client_name = shared.applicationName
                    except Exception as e:
                        logging.logPrint(f'({p_name}): could not set client_name on connection: [{e}]', logLevel.DEBUG, reportFrom=True)
                        pass #do not remove as on production we delete the previous line
            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return {}

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
                    return {}

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
                        query_timeout=shared.idleTimeoutSecs
                    )
                    logging.logPrint(f'({p_name}): databricks[{x}]: connected.', logLevel.DEBUG, reportFrom=True)
            except Exception as e:
                logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
                return {}

        case 'csv':
            if 'paths' in c:
                _paths=c['paths'].split('|')
            else:
                _paths = ('.') #pylint:disable=superfluous-parens

            for _path in _paths:
                if not os.path.isdir(_path):
                    try:
                        os.makedirs(name=_path)
                    except Exception as e:
                        logging.processError(p_message=f'({p_name}): directory does not exist, and exception happened when trying to create it [{_path}]', p_stop=True, p_exitCode=2)
                        return {}

            sFileName = ''
            logging.logPrint(f'({p_name}): dumping CSV files to {_paths}', logLevel.DEBUG, reportFrom=True)
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

    try:
        sGetVersion = check_bd_version_cmd[c['driver']]
        if len(sGetVersion) > 0:
            cur = nc[0].cursor() # type: ignore
            logging.logPrint(f'({p_name}): testing connection, getting version with [{sGetVersion}]...', logLevel.DEBUG, reportFrom=True)
            cur.execute(sGetVersion)
            db_version = cur.fetchone()
            logging.logPrint(f'({p_name}): ok, connected to DB version: {db_version}', logLevel.DEBUG, reportFrom=True)
            logging.logPrint(f'({p_name}): connected')
            cur.close()
    except Exception as e:
        logging.processError(p_e=e, p_message=p_name, p_stop=True, p_exitCode=2)
        return {}

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
        #only works on postgres...
        newCursor.itersize = p_fetchSize
        logging.logPrint(f'({p_source}): set cursor itersize to [{p_fetchSize}]', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
    except Exception as error:
        logging.logPrint(f'({p_source}): could not set cursor itersize: [{error}]', logLevel.DEBUG, p_jobID=p_jobID, reportFrom=True)
        pass #do not remove as on production we delete the previous line

    return newCursor
