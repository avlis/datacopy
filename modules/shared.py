'''global shared objecs and constants'''

#pylint: disable=invalid-name, line-too-long

import os
import sys
import psutil

import signal
import inspect

import multiprocessing as mp
from datetime import datetime

from time import time

from typing import Callable
import pandas as pd


#### Event fast "enum" ############################################################################

E_NOOP = 0
E_QUERY = 1
E_READ = 2
E_WRITE = 4

E_BOOT = 8
E_START = 16
E_END = 32
E_ERROR = 64
E_STOP = 128

E_QUERY_START = E_QUERY + E_START
E_QUERY_ERROR = E_QUERY + E_ERROR
E_QUERY_END = E_QUERY + E_END

E_BOOT_READER = E_BOOT + E_READ
E_READ_START = E_READ + E_START
E_READ_ERROR = E_READ + E_ERROR
E_READ_END = E_READ + E_END

E_WRITE_START = E_WRITE + E_START
E_WRITE_ERROR = E_WRITE + E_ERROR
E_WRITE_END = E_WRITE + E_END

#reverse naming helper dict:
eventsDecoder:dict = {}
__local_vars=list((inspect.currentframe().f_locals.items()))
for name, value in __local_vars:
    if name.startswith("E_"):
        eventsDecoder[value] = name


#### timestamp utilities ##########################################################################

def timestamp_compact() -> str:
    return datetime.now().strftime('%Y%m%d%H%M%S.%f')

def timestamp_readable() -> str:
    return str(datetime.now())

def timestamp_unix() -> float:
    return time()


#### shared Constants #############################################################################

applicationName = 'datacopy'

queueSize:int = int(os.getenv('QUEUE_SIZE','256'))
usedQueueBeforeNew:int = int(queueSize/int(os.getenv('QUEUE_FB4NEWR','3')))

REUSE_WRITERS:bool = bool(os.getenv('REUSE_WRITERS','yes') == 'yes')

TEST_QUERIES:bool = bool(os.getenv('TEST_QUERIES','no') == 'yes')

SCREEN_STATS:bool = bool(os.getenv('SCREEN_STATS','yes') == 'yes')

SCREEN_STATS_TO_STDOUT:bool = bool(os.getenv('SCREEN_STATS_OUTPUT','stderr') == 'stdout')

DEBUG_TO_STDERR:bool = bool(os.getenv('DEBUG_TO_STDERR','no') == 'yes')
DEBUG_TO_LOG:bool = bool(os.getenv('DEBUG_TO_LOG','no') == 'yes')

if DEBUG_TO_LOG or DEBUG_TO_STDERR:
    DEBUG=True
else:
    DEBUG:bool = bool(os.getenv('DEBUG','no') == 'yes')

DUMP_ON_ERROR:bool = bool(os.getenv('DUMP_ON_ERROR','no') == 'yes')
dumpFileSeparator:str = os.getenv('DUMPFILE_SEP','|')

executionID:str = os.getenv('EXECUTION_ID',datetime.now().strftime('%Y%m%d%H%M%S.%f'))

logTimestampFormat = os.getenv('LOG_TIMESTAMP_FORMAT','date')
logTimestampFunction:Callable[[], str | float] = None #type: ignore

match logTimestampFormat:
    case 'linux':
        logTimestampFunction=timestamp_unix
    case 'compact':
        logTimestampFunction=timestamp_compact
    case _:
        logTimestampFunction=timestamp_readable

statsTimestampFormat = os.getenv('STATS_TIMESTAMP_FORMAT','date')
statsTimestampFunction:Callable[[], str | float] = None #type: ignore

match statsTimestampFormat:
    case 'linux':
        statsTimestampFunction=timestamp_unix
    case 'compact':
        statsTimestampFunction=timestamp_compact
    case _:
        statsTimestampFunction=timestamp_readable

memoryTimestampFormat = os.getenv('MEMORY_STATS_TIMESTAMP_FORMAT','date')
memoryTimestampFunction:Callable[[], str | float] = None # type: ignore

match memoryTimestampFormat:
    case 'linux':
        memoryTimestampFunction=timestamp_unix
    case 'compact':
        memoryTimestampFunction=timestamp_compact
    case _:
        memoryTimestampFunction=timestamp_readable


STATS_IN_JSON:bool = bool(os.getenv('STATS_IN_JSON','no') == 'yes')
statsFormat:str
if STATS_IN_JSON:
    if isinstance(statsTimestampFunction(),str):
        statsFormat:str = '{{"dc.ts":"{0}","dc.execID":"{1}","dc.event":"{2}","dc.jobID":"{3}","dc.recs":{4},"dc.secs":{5:.2f},"dc.threads":{6}}},'
    else:
        statsFormat:str = '{{"dc.ts":{0},"dc.execID":"{1}","dc.event":"{2}","dc.jobID":"{3}","dc.recs":{4},"dc.secs":{5:.2f},"dc.threads":{6}}},'
else:
    statsFormat:str = '{0}\t{1}\t{2}\t{3}\t{4}\t{5:.2f}\t{6}'

parallelReaders:int = int(os.getenv('PARALLEL_READERS','1'))
parallelReadersLaunchInterval:float = float(os.getenv('PARALLEL_READERS_LAUNCH_INTERVAL','0.1'))

idleTimeoutSecs:int = int(os.getenv('IDLE_TIMEOUT_SECS','0'))
connectionTimeoutSecs:int = int(os.getenv('CONNECTION_TIMEOUT_SECS','22'))


COLLECT_MEMORY_STATS:bool = bool(os.getenv('COLLECT_MEMORY_STATS','no') == 'yes')
collectMemoryStatsIntervalSecs:float = float(os.getenv('COLLECT_MEMORY_STATS_INTERVAL_SECS','1'))

MEMORY_STATS_IN_JSON:bool = bool(os.getenv('MEMORY_STATS_IN_JSON','no') == 'yes')
if MEMORY_STATS_IN_JSON:
    if isinstance(memoryTimestampFunction(),str):
        memoryStatsFormat:str = '{{"dc.ts":"{0}","dc.execID":"{1}","dc.proc.memory":{2:.2f},"dc.proc.pid":{3},"dc.proc.status":"{4}","dc.proc.name":"{5}"}},'
    else:
        memoryStatsFormat:str = '{{"dc.ts":{0},"dc.execID":"{1}","dc.proc.memory":{2:.2f},"dc.proc.pid":{3},"dc.proc.status":"{4}","dc.proc.name":"{5}"}},'
else:
    memoryStatsFormat:str = '{0}\t{1}\t{2:.2f}\t{3}\t{4}\t{5}'

collectMemoryMainProcessID = psutil.Process(os.getpid())

defaultFetchSize:int = 1024


#### Shared Variables, but changed in single thread contexts ######################################

connections:pd.DataFrame = None # type: ignore
jobs:pd.DataFrame = None #type: ignore

PutConn = {}
PutData = {}
GetConn = {}
GetConn2 = {}
GetData = {}
GetData2 = {}

readP = {}
writeP = {}

logName:str = ''

if len(sys.argv) < 4:
    logName = os.getenv('LOG_NAME',timestamp_compact())
else:
    logName = sys.argv[3]

maxQueueLenObserved:int = 0
maxQueueLenObservedEvents:int = 0


#### OBJECTS shared / edited in multithreads  #####################################################

dataQueue:mp.Queue = mp.Queue(queueSize)
''' message format: just a bData object returned by cursor.fetchmany()'''

eventQueue:mp.Queue = mp.Queue()
'''message format: tuple(Type:int, jobID:int, recs:int, secs:float)'''

logQueue:mp.Queue = mp.Queue()
'''message format: tuple(logLevel:Enum, message, jobID:int, jobName:str, where:str)'''

Working = mp.Value('b', True)
runningReaders = mp.Value('i', 0)
runningWriters = mp.Value('i', 0)
ErrorOccurred =  mp.Value('b',False)
stopWhenEmpty = mp.Value('b', False)
logIsAlreadyClosed = mp.Value('b', False)

idleSecsObserved = mp.Value('i', 0)

exitCode = mp.Value('i', 0)

#### Other Utilities ##############################################################################

def encodeSpecialChars(p_in):
    '''convert special chars to escaped representation'''
    buff=[]
    for line in p_in:
        newLine=[]
        for col in line:
            if isinstance(col, str):
                newData=[]
                for b in col:
                    i=ord(b)
                    if i<32:
                        newData.append(r'\{hex(i)}')
                    else:
                        newData.append(b)
                newLine.append(''.join(newData))
            else:
                newLine.append(col)
        buff.append(tuple(newLine))
    return tuple(buff)

def identify_type(obj):
    '''Identifies the type of an object as integer, float, date, or string.

    Args:
        obj: The object to identify the type of.

    Returns:
        A string indicating the type of the object.
    '''
    t = type(obj)
    if t == int:
        return 'integer'
    elif t == float:
        return 'float'
    elif str(t).startswith("<class 'datetime."):  # Replace with your date library if needed
        return 'date'
    elif t == str:
        return 'string'
    else:
        return 'unknown'

def delimiter_decoder(delimiter_name:str) -> str:
    delim_decoder = {
    'tab': '\t',
    'space': ' ',
    'comma': ',',
    'colon': ':',
    'semicolon': ';',
    'newline': '\n',
    'pipe': '|',
    'slash': '/',
    'backslash': '\\',
    'dollar': '$',
    'at': '@',
    'percent': '%',
    'hash': '#',
    'ampersand': '&',
    'equals': '=',
    'plus': '+',
    'minus': '-',
    'asterisk': '*',
    'lparen': '(',
    'rparen': ')',
    'lbracket': '[',
    'rbracket': ']',
    'lbrace': '{',
    'rbrace': '}',
    'lt': '<',
    'gt': '>',
    'tilde': '~',
    'exclamation': '!',
    'question': '?',
    'dot': '.',
    'underscore': '_',
    'hiphen': '-',
    'backtick': '`',
    'doublequote': '"',
    'singlequote': "'",
    }

    try:
        return delim_decoder[delimiter_name]
    except:
        return delimiter_name

def block_signals():
    '''we want to make sure these threads exit gracefully, only when shared.Working.value is False'''
    signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGINT])
    signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGTERM])

    #the previous lines don't seem to fix everything, also trying to ignore the signals
    signal.SIG_IGN
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
