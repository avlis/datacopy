'''connections handling stuff'''

#pylint: disable=invalid-name, broad-except, import-outside-toplevel, c-extension-no-member, line-too-long

import os
import csv
import pandas as pd

import modules.logging as logging
import modules.shared as shared



expected_conns_columns_db = ('name','driver','server','database','user','password')
expected_conns_columns_csv = ('name','driver','paths','delimiter','quoting')

check_bd_version_cmd = {
    'pyodbc':       'SELECT @@version',
    'cx_Oracle':    'SELECT * FROM V$VERSION',
    'psycopg2':     'SELECT version()',
    'mysql':        'SELECT version()',
    'mariadb':      'SELECT version()',
    'csv':          '',
    '':             ''
}

insert_objects_delimiter = {
    'pyodbc':       '"',
    'cx_Oracle':    '"',
    'psycopg2':     '"',
    'mysql':        '`',
    'mariadb':      '`',
    'csv':          '',
    '':             ''
}

csv_delimiter_decoder = {
    'tab':      '\t',
    'coma':     ',',
    'pipe':     '|',
    'colon':    ':',
    'hash':     '#'
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

    conns = {}
    try:
        c=pd.read_csv(p_filename, delimiter = '\t').fillna('')
    except Exception as error:
        logging.logPrint(f'error Loading [{p_filename}]: [{error}]')
        logging.closeLogFile(1)

    for i in range(len(c)):
        cName = c['name'][i]
        if cName=='' or cName[0] == '#':
            continue
        if c['driver'][i] == 'csv':
            for ecol in expected_conns_columns_csv:
                if ecol not in c:
                    logging.logPrint(f'loadConnections[{cName}]: Missing column on connections file: [{ecol}]')
                    logging.closeLogFile(1)
            nc = {
                'driver':       c['driver'][i],
                'paths':        c['paths'][i],
                'delimiter':    c['delimiter'][i],
                'quoting':      c['quoting'][i],
                }
        else:
            for ecol in expected_conns_columns_db:
                if ecol not in c:
                    logging.logPrint(f'loadConnections[{cName}]: Missing column on connections file: [{ecol}]')
                    logging.closeLogFile(1)
            if 'trustservercertificate' in c:
                sTSC = c['trustservercertificate'][i]
            else:
                sTSC = 'no'
            sIP=''
            if 'override_insert_placeholder' in c:
                sIP = c['override_insert_placeholder'][i]
            else:
                sIP = '%s'

            if os.getenv('ADD_NAMES_DELIMITERS','no') == 'yes':
                object_delimiters = insert_objects_delimiter[c['driver'][i]]
            else:
                object_delimiters = ''

            nc = {
                'driver':                   c['driver'][i],
                'server':                   c['server'][i],
                'database':                 c['database'][i],
                'user':                     c['user'][i],
                'password':                 c['password'][i],
                'trustservercertificate':   sTSC,
                'insert_placeholder':       sIP,
                'insert_object_delimiter':  object_delimiters
            }

        conns[cName] = nc

    shared.connections = conns

def initConnections(p_name:str, p_readOnly:bool, p_qtd:int, p_preQuery:str = '', p_tableName = '', p_mode = 'w'):
    '''creates connection objects to sources or destinations'''

    nc = {}

    if p_name in shared.connections:
        c = shared.connections[p_name]

    logging.logPrint(f'initConnections[{p_name}]: trying to connect...', shared.L_DEBUG)
    if c['driver'] == 'pyodbc':
        try:
            import pyodbc
            for x in range(p_qtd):
                # parameters in string because if added as independent parameters, it segfaults
                # used to be:
                #nc[x]=pyodbc.connect(driver='{ODBC Driver 18 for SQL Server}', server=c['server'], database=c['database'], user=c['user'], password=c['password'], encoding = 'UTF-8', nencoding = 'UTF-8', readOnly = p_readOnly, trustservercertificate = c['trustservercertificate'] )
                nc[x]=pyodbc.connect(f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={c['server']};DATABASE={{{c['database']}}};UID={{{c['user']}}};PWD={{{c['password']}}};ENCODING=UTF-8;TRUSTSERVERCERTIFICATE={c['trustservercertificate']}")
        except (Exception, pyodbc.DatabaseError) as error:
            logging.logPrint(f'initConnections({p_name}): DB error [{error}]')
            shared.ErrorOccurred.value=True
            logging.closeLogFile(2)

    if c['driver'] == 'cx_Oracle':
        try:
            import cx_Oracle
            for x in range(p_qtd):
                nc[x]=cx_Oracle.connect(c['user'], c['password'], f"{c['server']}/{c['database']}", encoding = 'UTF-8', nencoding = 'UTF-8' )
                nc[x].outputtypehandler = cx_Oracle_OutputTypeHandler
        except Exception as error:
            logging.logPrint(f'initConnections({p_name}): DB error [{error}]')
            shared.ErrorOccurred.value=True
            logging.closeLogFile(2)

    if c['driver'] == 'psycopg2':
        try:
            from psycopg2 import pool as pgpools
            tpool = pgpools.ThreadedConnectionPool(1, p_qtd, host=c['server'], database=c['database'], user=c['user'], password = c['password'])
            for x in range(p_qtd):
                nc[x] = tpool.getconn()
                nc[x].readonly = p_readOnly
        except Exception as error:
            logging.logPrint(f'initConnections({p_name}): DB error [{error}]')
            shared.ErrorOccurred.value=True
            logging.closeLogFile(2)

    if c['driver'] == 'mysql':
        try:
            import mysql.connector
            for x in range(p_qtd):
                nc[x]=mysql.connector.connect(host=c['server'], database=c['database'], user=c['user'], password = c['password'])
        except Exception as error:
            logging.logPrint(f'initConnections({p_name}): DB error [{error}]')
            shared.ErrorOccurred.value=True
            logging.closeLogFile(2)

    if c['driver'] == 'mariadb':
        try:
            import mariadb
            for x in range(p_qtd):
                nc[x]=mariadb.connect(host=c['server'], database=c['database'], user=c['user'], password = c['password'])
        except Exception as error:
            logging.logPrint(f'initConnections({p_name}): DB error [{error}]')
            shared.ErrorOccurred.value=True
            logging.closeLogFile(2)

    if c['driver'] == 'csv':
        if 'paths' in c:
            _paths=c['paths'].split('|')
        else:
            _paths = ('.') #pylint:disable=superfluous-parens

        for _path in _paths:
            if not os.path.isdir(_path):
                logging.logPrint(f'initConnections({p_name}): directory does not exist [{_path}]')
                shared.ErrorOccurred.value=True
                logging.closeLogFile(2)

        sFileName = ''
        logging.logPrint(f'initConnections({p_name}): dumping CSV files to {_paths}', shared.L_DEBUG)
        try:
            _delim=csv_delimiter_decoder[c['delimiter']]
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
        logging.logPrint(f'initConnections({p_name}): registering csv dialect with delim=[{_delim}], quoting=[{_quote}]', shared.L_DEBUG)
        try:
            if p_qtd > 1:
                ipath=0
                for x in range(p_qtd):
                    sFileName = os.path.join(_paths[ipath], f'{p_tableName}_{x+1}.csv')
                    if ipath<len(_paths)-1:
                        ipath += 1
                    else:
                        ipath = 0
                    logging.logPrint(f'initConnections({p_name}): opening file=[{sFileName}], mode=[{p_mode}]', shared.L_DEBUG)
                    nc[x] = csv.writer(open(sFileName, p_mode, encoding = 'utf-8'), dialect = p_name)
            else:
                sFileName = os.path.join(_paths[0], f'{p_tableName}.csv')
                logging.logPrint(f'initConnections({p_name}): opening file=[{sFileName}], mode=[{p_mode}]', shared.L_DEBUG)
                nc[0] = csv.writer(open(sFileName, p_mode, encoding = 'utf-8'), dialect = p_name)
        except Exception as error:
            logging.logPrint(f'initConnections({p_name}): CSV error [{error}] opening file [{sFileName}]')

    try:
        sGetVersion = check_bd_version_cmd[c['driver']]
        if sGetVersion != '':
            cur = nc[0].cursor()
            logging.logPrint(f'initConnections({p_name}): Testing connection, getting version with [{sGetVersion}]...', shared.L_DEBUG)
            cur.execute(sGetVersion)
            db_version = cur.fetchone()
            logging.logPrint(f'initConnections({p_name}): ok, connected to DB version: {db_version}', shared.L_DEBUG)
            logging.logPrint(f'initConnections({p_name}): connected')
            cur.close()
    except Exception as error:
        logging.logPrint(f'initConnections({p_name}): error [{error}]')
        shared.ErrorOccurred.value=True
        logging.closeLogFile(2)

    #pylint: disable=consider-using-dict-items
    if p_preQuery != '':
        for i in nc:
            pc = nc[i].cursor()
            try:
                logging.logPrint(f'initConnections({p_name}): executing pre_query [{p_preQuery}]', shared.L_DEBUG)
                pc.execute(p_preQuery)
            except Exception as error:
                logging.logPrint(f'initConnections({p_name}): error executing pre_query [{p_preQuery}] [{error}]')
                shared.ErrorOccurred.value=True
                logging.closeLogFile(2)
            pc.close()

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

def initCursor(p_conn, p_jobID:int, p_fetchSize:int):
    '''prepares the object that will send commands to databases'''
    # postgres: try not to fetch all rows to memory, using server side cursors

    try:
        logging.logPrint('trying to get server side cursor...', shared.L_DEBUG)
        newCursor = p_conn.cursor(name = f'jobid-{p_jobID}')
    except Exception as error:
        logging.logPrint(f'server side cursor did not work, getting a normal cursor: [{error}]', shared.L_DEBUG)
        newCursor = p_conn.cursor()
    try:
        #only works on postgres...
        newCursor.itersize = p_fetchSize
    except Exception as error:
        logging.logPrint(f'could not set itersize: [{error}]', shared.L_DEBUG)

    return newCursor
