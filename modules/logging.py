'''log and stats stuff'''

#pylint: disable=invalid-name, broad-except, bare-except, line-too-long
import os
import sys
from datetime import datetime
from time import sleep
from timeit import default_timer as timer
import multiprocessing as mp
import signal
import queue
import csv
import inspect

import setproctitle

import modules.shared as shared

logThread = None

def logPrint(p_Message:str, p_logLevel:int=shared.L_INFO):
    ''' sends message to the queue that manages logging'''
    #if shared.DEBUG:
    #    print(f'logPrint: adding message to the log queue [{p_logLevel}][{p_Message}]', file=sys.stderr, flush=True)
    match p_logLevel:
        case shared.L_DEBUG:
            if not shared.DEBUG:
                return
            modName = '<unknown>'
            funcName = '<unknown>'
            nl = ''
            if p_Message.startswith('\n'):
                nl = '\n'
                p_Message = p_Message[1:]
            try:
                funcName = inspect.stack()[1].function
                modName = inspect.getmodule(inspect.stack()[1][0]).__name__ # type: ignore
            except:
                pass
            shared.logStream.put((p_logLevel, f'{nl}{modName}.{funcName}: {p_Message}'), block=True, timeout=shared.idleTimetoutSecs)
        case shared.L_STATSONPROCNAME:
            shared.logStream.put((p_logLevel, p_Message), block=True, timeout=shared.idleTimetoutSecs)
        case _:
            shared.logStream.put((p_logLevel, p_Message))


def statsPrint(p_type:str, p_jobName:str, p_recs:int, p_secs:float, p_threads:int):
    '''sends stat messages to the log queue'''

    sMsg = shared.statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), shared.executionID, p_type, p_jobName, p_recs, p_secs, p_threads )
    shared.logStream.put((shared.L_STATS, sMsg))


def openLogFile(p_dest:str, p_table:str):
    '''setups the log file'''

    sLogFilePrefix = ''
    if shared.logFileName == '':
        sLogFilePrefix = f'{p_dest}.{p_table}'
    else:
        sLogFilePrefix = f'{shared.logFileName}'

    shared.logStream.put( (shared.L_OPEN, sLogFilePrefix) )


def closeLogFile(p_exitCode = None):
    '''makes sure the log file is properly handled.'''

    #give some time to other threads to say whatever they need to say to logs...
    if p_exitCode is None:
        sleep(3)

    loopTimeout = 10
    while shared.logStream.qsize() > 0 and loopTimeout > 0:
        if shared.DEBUG:
            print(f'closeLogFile: waiting for empty log queue, stage 1 [{shared.logStream.qsize()}]', file=sys.stderr, flush=True)
        sleep(1)
        loopTimeout -= 1

    shared.logStream.put( (shared.L_CLOSE, '') )
    loopTimeout = 3
    while shared.logStream.qsize() > 0 and loopTimeout > 0:
        if shared.DEBUG:
            print(f'closeLogFile: waiting for empty log queue, stage 2 [{shared.logStream.qsize()}]', file=sys.stderr, flush=True)
        sleep(1)
        loopTimeout -= 1

    if p_exitCode is not None:
        if shared.DEBUG:
            print('closeLogFile: sending the log thread a stop command.', file=sys.stderr, flush=True)
        shared.logStream.put( (shared.L_END, mp.current_process().name) )
        sleep(1)

    loopTimeout = 3
    while shared.logStream.qsize() > 0 and loopTimeout > 0:
        if shared.DEBUG:
            print(f'closeLogFile: waiting for empty log queue, stage 3 [{shared.logStream.qsize()}]', file=sys.stderr, flush=True)
        sleep(1)
        loopTimeout -= 1

    if shared.logStream.qsize() > 0 and shared.DEBUG:
        print('closeLogFile: something is wrong, quitting with log messages still in the queue!', file=sys.stderr, flush=True)

    if p_exitCode is not None:
        if shared.DEBUG:
            print('closeLogFile: making sure log thread is terminated...', file=sys.stderr, flush=True)

        try:
            logThread.join(timeout=1)
            logThread.terminate()
            logThread.join(timeout=1)

        except Exception as error:
            print(f'closeLogFile: error terminating log thread ({sys.exc_info()[2].tb_lineno}): [{error}]', file=sys.stderr, flush=True)

        if shared.DEBUG:
            print('closeLogFile: log thread terminated and joined.', file=sys.stderr, flush=True)

        if shared.DEBUG:
            print(f'closeLogFile: exiting with exitcode [{p_exitCode}]', file=sys.stderr, flush=True)
        sys.exit(p_exitCode)


def writeLogFile():
    '''processes messages on the log queue and sends them to file, stdout, stderr acordingly'''

    # WARNING: Log writing failures does not stop processing!

    #ignore control-c on this thread
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    setproctitle.setproctitle('datacopy: log writer')

    logFile = None
    statsFile = None
    dumpColNames = None
    dumpFile = None
    sLogFilePrefix = ''

    bKeepGoing=True
    while bKeepGoing:
        try:
            (msgLogLevel, sMsg) = shared.logStream.get( block=True, timeout = 1 )
        except queue.Empty:
            continue
        except OSError:
            if shared.DEBUG:
                print('writeLogFile: EOError exception. giving up.', file=sys.stderr, flush=True)
            break
        except Exception as error:
            if shared.DEBUG:
                print(f'writeLogFile: Exception at line ({sys.exc_info()[2].tb_lineno}): [{error}]', file=sys.stderr, flush=True)
            continue

        #print(f'logwriter: received message [{msgLogLevel}][{sMsg}]', file=sys.stderr, flush=True)
        match msgLogLevel:
            case shared.L_STATSONPROCNAME:
                setproctitle.setproctitle(f'datacopy: log writer [{sMsg}]')
                continue
            case shared.L_INFO:
                print(f'\n{str(datetime.now())}: {sMsg}', file=sys.stdout, flush=True)
                if logFile:
                    try:
                        print(f'{str(datetime.now())}: {sMsg}', file=logFile, flush=True)
                    except Exception:
                        pass
                continue

            case shared.L_DEBUG:
                print(sMsg, file=sys.stderr, flush=True)
                continue

            case shared.L_STATS:
                if statsFile:
                    try:
                        print(sMsg, file=statsFile, flush=True)
                    except Exception:
                        pass
                continue

            case shared.L_DUMPCOLS:
                dumpColNames = sMsg
                continue

            case shared.L_DUMPDATA:
                if shared.DEBUG or shared.DUMP_ON_ERROR:
                    try:
                        dumpFile = open( f'{sLogFilePrefix}.DUMP', 'w', encoding = 'utf-8')
                        dumper=csv.writer(dumpFile, delimiter = shared.DUMPFILE_SEP, quoting = csv.QUOTE_MINIMAL)
                        dumper.writerow(dumpColNames)
                        dumper.writerows(shared.encodeSpecialChars(sMsg))
                        dumpFile.close()
                    except Exception:
                        pass
                continue

            case shared.L_OPEN:
                try:
                    print(f'writeLogFile: opening [{sMsg}]', file=sys.stderr, flush=True)
                    sLogFilePrefix = sMsg
                    logFile = open( f'{sMsg}.running.log', 'a', encoding = 'utf-8')
                except Exception as error:
                    print(f'writeLogFile: could not open log file [{sMsg}]: [{error}]', file=sys.stderr, flush=True)
                try:
                    statsFile = open( f'{sMsg}.stats', 'a', encoding = 'utf-8')
                except Exception as error:
                    print(f'writeLogFile: could not open stats file [{sMsg}]: [{error}]', file=sys.stderr, flush=True)
                continue

            case shared.L_STREAM_START:
                shared.idleSecsObserved.value = 0
                rStart = timer()
                if statsFile:
                    try:
                        print(shared.statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), shared.executionID, 'streamStart', sMsg, 0, 0, shared.parallelReaders ), file=statsFile, flush=True)
                    except Exception:
                        pass
                continue

            case shared.L_STREAM_END:
                if statsFile:
                    try:
                        print(shared.statsFormat.format(datetime.now().strftime('%Y%m%d%H%M%S.%f'), shared.executionID, 'streamEnd', sMsg, shared.idleSecsObserved.value, timer()-rStart, 0 ), file=statsFile, flush=True)
                    except Exception:
                        pass
                continue

            case shared.L_CLOSE:
                if logFile:
                    logFile.close()
                    logFile=None
                    if shared.ErrorOccurred.value:
                        sLogFileFinalName = f'{sLogFilePrefix}.ERROR.log'
                    else:
                        sLogFileFinalName = f'{sLogFilePrefix}.ok.log'
                    try:
                        os.rename(f'{sLogFilePrefix}.running.log', sLogFileFinalName)
                    except Exception:
                        pass
                continue

            case shared.L_END:
                if shared.DEBUG:
                    print('writeLogFile: received stop message.', file=sys.stderr, flush=True)
                bKeepGoing = False

    if shared.DEBUG:
        print('writeLogFile: exiting...', file=sys.stderr, flush=True)
