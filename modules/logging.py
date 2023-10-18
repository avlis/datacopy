'''log and stats stuff'''

#pylint: disable=invalid-name,broad-except
import os
import sys
from datetime import datetime
from time import sleep
from timeit import default_timer as timer
import multiprocessing as mp
import signal
import queue
import csv
import setproctitle

import modules.shared as shared


def logPrint(p_Message:str, p_logLevel:int=1):
    ''' sends message to the queue that manages logging'''

    if p_logLevel in (shared.L_INFO, shared.L_DEBUG):
        sMsg = "{0}: {1}".format(str(datetime.now()), p_Message)
    else:
        sMsg=p_Message
    shared.logStream.put((p_logLevel, sMsg))

def statsPrint(p_type:str, p_jobName:str, p_recs:int, p_secs:float, p_threads:int):
    '''sends stat messages to the log queue'''

    sMsg = shared.statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), shared.executionID, p_type, p_jobName, p_recs, p_secs, p_threads )
    shared.logStream.put((shared.L_STATS, sMsg))


def openLogFile(p_dest:str, p_table:str):
    '''setups the log file'''

    sLogFilePrefix = ''
    if shared.logFileName == '':
        sLogFilePrefix = "{0}.{1}".format(p_dest, p_table)
    else:
        sLogFilePrefix = "{0}".format(shared.logFileName)

    shared.logStream.put( (shared.L_OPEN, sLogFilePrefix) )

def closeLogFile(p_exitCode = None):
    '''makes sure the log file is properly handled.'''

    #give some time to other threads to say whatever they need to say to logs...
    if p_exitCode is None:
        sleep(3)

    shared.logStream.put( (shared.L_CLOSE, '') )
    shared.logStream.put( (shared.L_END, mp.current_process().name) )
    loopTimeout = 3
    while shared.logStream.qsize() > 0 and loopTimeout >0:
        sleep(1)
        loopTimeout -= 1

    if p_exitCode is not None:
        sys.exit(p_exitCode)

def writeLogFile():
    '''processes messages on the log queue and sends them to file, stdout, stderr acordingly'''

    # WARNING: Log writing failures does not stop processing!

    #ignore control-c on this thread
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    setproctitle.setproctitle(f'datacopy: log writer')


    logFile = None
    statsFile = None
    dumpColNames = None
    dumpFile = None
    sLogFilePrefix = ''

    bKeepGoing=True
    while bKeepGoing:
        try:
            (logLevel, sMsg) = shared.logStream.get( block=True, timeout = 1 )
        except queue.Empty:
            continue
        except Exception:
            continue

        #print("logwriter: received message [{0}][{1}]".format(logLevel, sMsg), file=sys.stderr, flush=True)
        if logLevel == shared.L_INFO:
            print(sMsg, file=sys.stdout, flush=True)
            if logFile:
                try:
                    print(sMsg, file=logFile, flush=True)
                except Exception:
                    pass
            continue

        if logLevel == shared.L_DEBUG and shared.DEBUG:
            print(sMsg, file=sys.stderr, flush=True)
            continue

        if logLevel == shared.L_STATS:
            if statsFile:
                try:
                    print(sMsg, file=statsFile, flush=True)
                except Exception:
                    pass
            continue

        if logLevel == shared.L_DUMPCOLS:
            dumpColNames = sMsg
            continue

        if logLevel == shared.L_DUMPDATA:
            if shared.DEBUG or shared.DUMP_ON_ERROR:
                try:
                    dumpFile = open( "{0}.DUMP".format(sLogFilePrefix), 'w')
                    dumper=csv.writer(dumpFile, delimiter = shared.DUMPFILE_SEP, quoting = csv.QUOTE_MINIMAL)
                    dumper.writerow(dumpColNames)
                    dumper.writerows(shared.encodeSpecialChars(sMsg))
                    dumpFile.close()
                except Exception:
                    pass
            continue

        if logLevel == shared.L_OPEN:
            try:
                print("writeLogFile: opening [{0}]".format(sMsg), file=sys.stderr, flush=True)
                sLogFilePrefix = sMsg
                logFile = open( "{0}.running.log".format(sMsg), 'a')
            except Exception as error:
                print('could not open log file [{0}]: [{1}]'.format(sMsg, error), file=sys.stderr, flush=True)
            try:
                statsFile = open( "{0}.stats".format(sMsg), 'a')
            except Exception as error:
                print('could not open stats file [{0}]: [{1}]'.format(sMsg, error), file=sys.stderr, flush=True)
            continue

        if logLevel == shared.L_STREAM_START:
            rStart = timer()
            if statsFile:
                try:
                    print(shared.statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), shared.executionID, 'streamStart', sMsg, 0, 0, shared.parallelReaders ), file=statsFile, flush=True)
                except Exception:
                    pass
            continue

        if logLevel == shared.L_STREAM_END:
            if statsFile:
                try:
                    print(shared.statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), shared.executionID, 'streamEnd', sMsg, 0, timer()-rStart, 0 ), file=statsFile, flush=True)
                except Exception:
                    pass
            continue

        if logLevel == shared.L_CLOSE:
            if logFile:
                logFile.close()
                logFile=None
                if shared.ErrorOccurred.value:
                    sLogFileFinalName = "{0}.ERROR.log".format(sLogFilePrefix)
                else:
                    sLogFileFinalName = "{0}.ok.log".format(sLogFilePrefix)
                try:
                    os.rename("{0}.running.log".format(sLogFilePrefix), sLogFileFinalName)
                except Exception:
                    pass
            continue

        if logLevel == shared.L_END:
            bKeepGoing = False

    print('writeLogFile exiting...', file=sys.stderr, flush=True)
