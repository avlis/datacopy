'''global shared objecs and constants'''

#pylint: disable=invalid-name

import os
import multiprocessing as mp
from datetime import datetime

# Constants

E_READ = 1
E_WRITE = 2
E_READ_START = 4+1
E_WRITE_START = 4+2
E_READ_END = 8+1
E_WRITE_END = 8+2

L_INFO = 1
L_DEBUG = 2
L_STATS = 4
L_DUMPCOLS = 8 + 1
L_DUMPDATA = 8 + 2
L_OPEN = 16
L_STREAM_START = 16 + 4
L_CLOSE = 32
L_STREAM_END = 32 + 4
L_END = 255

D_COD = 'C'
D_EOD = '\x04'

connections = {}
queries = {}

defaultFetchSize:int = 1000

logFileName:str = ''
readP = {}
writeP = {}

queueSize = int(os.getenv('QUEUE_SIZE','256'))
usedQueueBeforeNew = int(queueSize/int(os.getenv('QUEUE_FB4NEWR','3')))

ReuseWriters:bool = bool(os.getenv('REUSE_WRITERS','no') == 'yes')

testQueries:bool = bool(os.getenv('TEST_QUERIES','no') == 'yes')

screenStats:bool = bool(os.getenv('SCREEN_STATS','yes') == 'yes')

stopJobsOnError:bool = bool(os.getenv('STOP_JOBS_ON_ERROR','yes') == 'yes')

DEBUG:bool = bool(os.getenv('DEBUG','no') == 'yes')

DUMP_ON_ERROR:str = bool(os.getenv('DUMP_ON_ERROR','no') == 'yes')
DUMPFILE_SEP:str = os.getenv('DUMPFILE_SEP','|')

executionID:str = os.getenv('EXECUTION_ID',datetime.now().strftime('%Y%m%d%H%M%S.%f'))

if os.getenv('STATS_IN_JSON','no') == 'yes':
    statsFormat = '{{"dc.ts":"{0}","dc.execID":"{1}","dc.event":"{2}","dc.jobID":"{3}","dc.recs":{4},"dc.secs":{5:.2f},"dc.threads":{6}}}'
else:
    statsFormat = "{0}\t{1}\t{2}\t{3}\t{4}\t{5:.2f}\t{6}"

parallelReaders = int(os.getenv('PARALLEL_READERS','1'))



#### SHARED OBJECTS

dataBuffer = mp.Manager().Queue(queueSize)
eventStream = mp.Manager().Queue()
logStream = mp.Manager().Queue()

seqnbr = mp.Value('i', 0)
Working = mp.Value('b', True)
ErrorOccurred =  mp.Value('b',False)


PutConn = {}
PutData = {}
GetConn = {}
GetConn2 = {}
GetData = {}
GetData2 = {}


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
                        newData.append(r'\{0}'.format(hex(i)))
                    else:
                        newData.append(b)
                newLine.append(''.join(newData))
            else:
                newLine.append(col)
        buff.append(tuple(newLine))
    return tuple(buff)
