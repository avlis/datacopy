'''global shared objecs and constants'''

import os
import psutil

import inspect

import multiprocessing as mp
from multiprocessing.sharedctypes import Synchronized

from datetime import datetime
import zoneinfo

from typing import Callable, Any


#### Event fast "enum" ############################################################################
# (not real Enum because it is a lot slower)

E_NOOP = 0
E_QUERY = 1
E_READ = 2
E_WRITE = 4

E_BOOT = 8
E_START = 16
E_END = 32
E_ERROR = 64
E_STOP = 128

E_KEYS = 265
E_DETAIL = 512

E_QUERY_START = E_QUERY + E_START
E_QUERY_ERROR = E_QUERY + E_ERROR
E_QUERY_END = E_QUERY + E_END

E_KEYS_QUERY_START = E_QUERY + E_KEYS + E_START
E_KEYS_QUERY_ERROR = E_QUERY + E_KEYS + E_ERROR
E_KEYS_QUERY_END = E_QUERY + E_KEYS + E_END

E_DETAIL_QUERY_START = E_QUERY + E_DETAIL + E_START
E_DETAIL_QUERY_ERROR = E_QUERY + E_DETAIL + E_ERROR
E_DETAIL_QUERY_END = E_QUERY + E_DETAIL + E_END

E_BOOT_READER = E_BOOT + E_READ
E_READ_START = E_READ + E_START
E_READ_ERROR = E_READ + E_ERROR
E_READ_END = E_READ + E_END
E_KEYS_READ_START = E_KEYS + E_START
E_KEYS_READ_END = E_KEYS + E_READ_END

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


timezone:str = os.getenv('TZ', "UTC")
timezoneinfo = zoneinfo.ZoneInfo(timezone)

def timestamp_short() -> str:
    return datetime.now(timezoneinfo).strftime('%H%M%S.%f')

def timestamp_compact() -> str:
    return datetime.now(timezoneinfo).strftime('%Y%m%d%H%M%S.%f')

def timestamp_local() -> str:
    return str(datetime.now())

def timestamp_unix() -> float:
    return datetime.now(timezoneinfo).timestamp()

def timestamp_iso8601() -> str:
    return datetime.now(timezoneinfo).isoformat()

timestamp_functions_map:dict[str, Callable] = {
    'short':    timestamp_short,
    'compact':  timestamp_compact,
    'readable': timestamp_local,
    'unix':     timestamp_unix,
    'iso8601':  timestamp_iso8601
}
# Job Name Calculator, used in multiple places

def getJobName(p_jobID:int) -> str:
    if p_jobID < 0:
        jobID = p_jobID * -1
    else:
        jobID = p_jobID
    try:
        return jobs[jobID]['jobName']
    except:
        return 'global'

#### shared Constants #############################################################################

applicationName = 'datacopy'

queueSize:int = int(os.getenv('QUEUE_SIZE','256'))
usedQueueBeforeNew:int = int(queueSize/int(os.getenv('QUEUE_FB4NEWR','3')))

REUSE_WRITERS:bool = bool(os.getenv('REUSE_WRITERS','yes') == 'yes')

TEST_QUERIES:bool = bool(os.getenv('TEST_QUERIES','no') == 'yes')

SCREEN_STATS:bool = bool(os.getenv('SCREEN_STATS','yes') == 'yes')

SCREEN_STATS_TO_STDOUT:bool = bool(os.getenv('SCREEN_STATS_OUTPUT','stderr') == 'stdout')

DEBUG_MODULES:list[str] = os.getenv('DEBUG_MODULES','').split(',')
if DEBUG_MODULES[0] == '':
    DEBUG_MODULES = []
DEBUG_READWRITES:bool = bool(os.getenv('DEBUG_READWRITES','no') == 'yes')
DEBUG_TO_STDERR:bool = bool(os.getenv('DEBUG_TO_STDERR','no') == 'yes')
DEBUG_TO_LOG:bool = bool(os.getenv('DEBUG_TO_LOG','no') == 'yes')

if DEBUG_TO_LOG or DEBUG_TO_STDERR:
    DEBUG=True
else:
    DEBUG:bool = bool(os.getenv('DEBUG','no') == 'yes')

DUMP_ON_ERROR:bool = bool(os.getenv('DUMP_ON_ERROR','no') == 'yes')
dumpFileSeparator:str = os.getenv('DUMPFILE_SEP','|')

executionID:str = os.getenv('EXECUTION_ID',datetime.now().strftime('%Y%m%d%H%M%S.%f'))

logTimestampFormat:str = os.getenv('LOG_TIMESTAMP_FORMAT','')
logTimestampFunction:Callable[[], str | float] = timestamp_local

if logTimestampFormat in timestamp_functions_map:
    logTimestampFunction = timestamp_functions_map[logTimestampFormat]

statsTimestampFormat:str = os.getenv('STATS_TIMESTAMP_FORMAT','')
statsTimestampFunction:Callable[[], str | float] = timestamp_iso8601

if  statsTimestampFormat in timestamp_functions_map:
    statsTimestampFunction=timestamp_functions_map[statsTimestampFormat]

memoryTimestampFormat:str = os.getenv('MEMORY_TIMESTAMP_FORMAT','')
memoryTimestampFunction:Callable[[], str | float] = timestamp_short

if  memoryTimestampFormat in timestamp_functions_map:
    memoryTimestampFunction=timestamp_functions_map[memoryTimestampFormat]

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
parallelReadersLaunchInterval:float = float(os.getenv('PARALLEL_READERS_LAUNCH_INTERVAL','0.2'))

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

collectMemoryMainProcessID:psutil.Process = psutil.Process(os.getpid())

defaultFetchSize:int = 1024


#### Shared Variables, but changed in single thread contexts ######################################

connections:dict[str, dict[str, Any]] = {}
jobs:dict[int, dict[str, Any]] = {}

PutConn:dict[int, Any] = {}
PutData:dict[int, Any] = {}
GetConn:dict[int, Any] = {}
GetConn2:dict[int, Any] = {}
GetData:dict[int, Any] = {}
GetData2:dict[int, Any] = {}

readP:dict[int, Any] = {}
writeP:dict[int, Any] = {}

logName:str = ''

maxQueueLenObserved:int = 0
maxQueueLenObservedEvents:int = 0


#### OBJECTS shared / edited in multithreads  #####################################################

dataQueue:mp.Queue = mp.Queue(queueSize)
''' message format: just a bData object returned by cursor.fetchmany()'''

dataKeysQueue:mp.Queue = mp.Queue(queueSize)
''' message format: just a bData object returned by cursor.fetchmany()'''

eventQueue:mp.Queue = mp.Queue()
'''message format: tuple(Type:int, jobID:int, recs:int, secs:float)'''

logQueue:mp.Queue = mp.Queue()
'''message format: tuple(logLevel:Enum, message, jobID:int, jobName:str, where:str)'''

Working:Synchronized = mp.Value('b', True)

ErrorOccurred:Synchronized =  mp.Value('b',False)
stopWhenEmpty:Synchronized = mp.Value('b', False)
stopWhenKeysEmpty:Synchronized = mp.Value('b', False)
logIsAlreadyClosed:Synchronized = mp.Value('b', False)

idleSecsObserved:Synchronized = mp.Value('i', 0)

exitCode:Synchronized = mp.Value('i', 0)
