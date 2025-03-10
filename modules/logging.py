'''log and stats stuff'''

#pylint: disable=invalid-name, broad-except, bare-except, line-too-long
import os
import sys
import setproctitle
import multiprocessing as mp
import queue
import inspect

from time import sleep
from timeit import default_timer as timer

import csv
from enum import Enum
from typing import Optional

import modules.shared as shared
import modules.utils as utils

#### Constants ##########################################################################

class logLevel(Enum):
    '''log levels'''
    INFO = 1
    DEBUG = 2
    ERROR = 4
    STATS = 8
    DUMP_SHARED = 16
    DUMP_COLS = 16 + 1
    DUMP_DATA = 16 + 2
    OPEN = 32
    CLOSE = 64
    STATSONPROCNAME = 128
    END = 255

#### Variables ##########################################################################

logThread:mp.Process

memoryStatsFile = None


#### "Public" stuff #####################################################################

def logPrint(p_message, p_logLevel:logLevel=logLevel.INFO, *, p_jobID:Optional[int]=None, p_threadID:Optional[int]=None, nested:bool=False, reportFrom:bool=False):
    ''' sends message to the queue that manages logging'''

    where:Optional[str] = None
    calledFrom:Optional[str] = None
    message:Optional[str] = None

    jobName:Optional[str] = None

    if p_logLevel == logLevel.DEBUG and not shared.DEBUG:
        return

    match p_logLevel:
        case logLevel.DUMP_COLS | logLevel.DUMP_DATA:
            message = p_message

        case logLevel.DUMP_SHARED:
            printSharedVariables(p_message)

        case _:
            if nested:
                where = whoAmI(3)
                if reportFrom:
                    calledFrom = whoAmI(4)
            else:
                where = whoAmI()
                if reportFrom:
                    calledFrom = whoAmI(3)
            message = p_message

            if p_jobID is None:
                jobName=None
            else:
                jobName=f'[{shared.getJobName(p_jobID)}]'

    if message is not None:
        shared.logQueue.put( ( p_logLevel, message, p_jobID, p_threadID, jobName, where, calledFrom ), block=True, timeout=shared.idleTimeoutSecs)

def statsPrint(p_type:str, p_jobID:Optional[int], p_recs:int, p_secs:float, p_threads:int):
    '''sends stat messages to the log queue'''

    jobName:str = 'global'
    if p_jobID is not None:
        jobName = shared.getJobName(p_jobID)

    sMsg = shared.statsFormat.format(shared.statsTimestampFunction(), shared.executionID, p_type, jobName, p_recs, p_secs, p_threads )
    shared.logQueue.put( ( logLevel.STATS, sMsg, None, None, None, None, None ) )

def processError(p_e:Optional[Exception]=None, p_stack:Optional[str]=None, p_message:Optional[str]=None, p_jobID:Optional[int]=None, p_stop:Optional[bool]=None, p_dontSendToStats:bool=False, p_threadID:Optional[int]=None, p_exitCode:Optional[int]=None) -> None:
    fromWhere = whoAmI()
    if p_threadID is not None:
        fromWhere=f'{fromWhere}#{p_threadID}'

    logPrint(f'@[{fromWhere}]: stop=[{p_stop}], jobID=[{p_jobID}], threadID=[{p_threadID}], exitCode=[{p_exitCode}]', logLevel.DEBUG, p_jobID=p_jobID)
    stackDetail:str=''
    if p_stack is not None:
        stackDetail=f'\n*** stack ***:\n{p_stack}\n*** end of stack ***'

    if p_dontSendToStats == False:
        statsPrint('ERROR', None, 0, 0, 0)

    if p_exitCode is not None:
        with shared.exitCode.get_lock():
            shared.exitCode.value = p_exitCode

    if p_e is None:
        logPrint(f'{p_message}', p_logLevel=logLevel.ERROR, p_jobID=p_jobID, p_threadID=p_threadID, nested=True)
    else:
        logPrint(f' Exception [{type(p_e).__name__}] occurred, message=[{p_message}]:\n{p_e}\n{stackDetail}', logLevel.ERROR, p_jobID=p_jobID, p_threadID=p_threadID, nested=True)

    with shared.ErrorOccurred.get_lock():
        shared.ErrorOccurred.value = True

    if p_stop is not None and p_stop:
        with shared.stopWhenEmpty.get_lock():
            shared.stopWhenEmpty.value = True

        with shared.Working.get_lock():
            shared.Working.value = False

        shared.eventQueue.put((shared.E_STOP, None, None, None))

    logPrint(f'@[{fromWhere}]: ended', logLevel.DEBUG, p_jobID=p_jobID)

def openLog():
    '''setups the log'''
    sLogFilePrefix = shared.logName
    shared.logQueue.put( (logLevel.OPEN, sLogFilePrefix, None, None, None, None, None) )

def closeLog():
    '''makes sure the log file is properly handled.'''

    def print_message(message:str):
        print(message, file=sys.stderr, flush=True)

    def process_remaining_logs(loopTimeout:int, stage:int):
        title=f'closeLog: waiting for empty log queue (stage {stage}):'
        while shared.logQueue.qsize() > 0:
            print_message(f'{title} logs to process=[{shared.logQueue.qsize()}]') #DISABLE_IN_PROD
            sleep(1)
            loopTimeout -= 1
            if  loopTimeout == 0:
                print_message(f'{title} timing out.') #DISABLE_IN_PROD
                break
        while shared.logQueue.qsize() > 0:
            dLogLevel, dMessage, dJobID, djobName, dWhere = shared.logQueue.get()
            print_message(f'{title} discarted LOG: [{dLogLevel.name}][{dMessage}][{dJobID}][{djobName}][{dWhere}]') #DISABLE_IN_PROD
        print_message(f'{title} exiting.') #DISABLE_IN_PROD

    def process_remaining_events(loopTimeout:int):
        title='closeLog: waiting for empty event queue:'
        while shared.eventQueue.qsize() > 0 and loopTimeout > 0:
            print_message(f'{title} events to process=[{shared.eventQueue.qsize()}]') #DISABLE_IN_PROD
            sleep(1)
            loopTimeout -= 1
        while shared.eventQueue.qsize() > 0:
            dType, dJobId, dRecs, dSecs = shared.eventQueue.get()
            print_message(f'{title} discarted EVENT: [{shared.eventsDecoder[dType]}][{dJobId}][{dRecs}][{dSecs}]') #DISABLE_IN_PROD

    try:
        print_message('\ncloseLog: called') #DISABLE_IN_PROD

        with shared.logIsAlreadyClosed.get_lock():
            if shared.logIsAlreadyClosed.value:
                print_message('\ncloseLog: was already closed.') #DISABLE_IN_PROD
                return
            shared.logIsAlreadyClosed.value = True

        process_remaining_events(3)

        process_remaining_logs(3, 1)

        print_message(f'\ncloseLog: sending CLOSE to write log thread') #DISABLE_IN_PROD
        shared.logQueue.put( (logLevel.CLOSE, None, None, None, None, None, None) )

        process_remaining_logs(3, 2)

        print_message('closeLog: sending END to write log thread') #DISABLE_IN_PROD
        shared.logQueue.put( (logLevel.END, None, None, None, None, None, None) )

        process_remaining_logs(3, 3)

    except Exception as e:
        print_message(f'closeLog: unexpected error ({sys.exc_info()[2].tb_lineno}): [{e}]') #DISABLE_IN_PROD
        pass #do not remove, because previous line is disabled in PROD mode

#### "Private" stuff #####################################################################

def whoAmI(stackLevel:int=2) -> str:
    '''
    returns modulename and function name. to use on logPrint or anywhere else.
    '''

    modName:str = '<unknown>'
    funcName:str = '<unknown>'
    className:str = ''
    lineNumber:int = 0
    try:
        frame = inspect.stack()[stackLevel]
        funcName = frame.function
        try:
            lineNumber = frame.lineno
        except:
            pass
        try:
            className = f".{frame.frame.f_locals['self'].__class__.__name__}"
        except:
            pass
        modName = inspect.getmodule(frame[0]).__name__ # type: ignore
    except:
        pass
    if lineNumber > 0:
        return f'{modName}{className}.{funcName}({lineNumber})'
    else:
        return f'{modName}{className}.{funcName}'

def monitor_memory():
    '''
        called by writeToLog, runs on the context of its thread, not on the main PID!
    '''

    global memoryStatsFile

    timestamp = shared.memoryTimestampFunction()
    totalMem:int = 0
    processName: str = 'not known yet'

    try:
        # Get memory usage of the main process
        mem = shared.collectMemoryMainProcessID.memory_info().rss / (1024 ** 2)
        totalMem += mem

        memStr = shared.memoryStatsFormat.format(timestamp, shared.executionID, mem, shared.collectMemoryMainProcessID.pid, shared.collectMemoryMainProcessID.status(), ' '.join(shared.collectMemoryMainProcessID.cmdline()))

        print(memStr, file=memoryStatsFile, flush=True)
    except Exception as e:
        logPrint(f'error on main process: [{e}]', logLevel.DEBUG)
        pass #do not remove as on production we delete the previous line

    # Get memory usage of child processes
    for child in shared.collectMemoryMainProcessID.children(recursive=True):
        try:
            mem = child.memory_info().rss / (1024 ** 2)
            totalMem += mem
            processName = ' '.join(child.cmdline())
            memStr = shared.memoryStatsFormat.format(timestamp, shared.executionID, mem, child.pid, child.status(), processName)
            print(memStr, file=memoryStatsFile, flush=True)
        except Exception as e:
            logPrint(f'error on [{processName}]: [{e}]', logLevel.DEBUG)
            pass #do not remove as on production we delete the previous line
    try:
        memStr = shared.memoryStatsFormat.format(timestamp, shared.executionID, totalMem, -1, '-', 'totalMemory')
        print(memStr, file=memoryStatsFile, flush=True)
    except Exception as e:
        logPrint(f'error on total mem: [{e}]', logLevel.DEBUG)
        pass #do not remove as on production we delete the previous line

    logPrint(f'memory stats: [{totalMem}]', logLevel.DEBUG)

def printSharedVariables(fromWhere:str):
    '''
        prints shared variables.
        it should be called from the logPrint and not writeToLog, so it runs on the thread that calls it, not on the logging thread!
    '''

    variables = list

    variables = dir(shared)
    logPrint(f'--- dump of {len(variables)} shared variables requested from [{fromWhere}]:', logLevel.DEBUG)

    for variable in variables:
        try:
            value = getattr(shared, variable)
            #dont want to know about constants or internal objects with _ in name
            if isinstance(value, (int, bool, str, float)) and variable[1:2] != '_':
                shared.logQueue.put( ( logLevel.DUMP_SHARED, value, None, None, None, f'{variable}({type(value).__name__})', None ) , block=True, timeout=shared.idleTimeoutSecs)

            if type(value).__name__ == 'Synchronized':
                shared.logQueue.put( ( logLevel.DUMP_SHARED, value.value, None, None, None, f'{variable}(mp.{type(value.value).__name__})', None ), block=True, timeout=shared.idleTimeoutSecs) # type: ignore

        except Exception as e:
            processError(p_e=e, p_message=f'something bad appened trying to dump shared variables: [{variable}]')
            return

    logPrint(f'--- end of dumping shared variables', logLevel.DEBUG)

def init_json_file(p_file, date_function=shared.timestamp_readable) -> None:
    if isinstance(date_function(),str):
        print(f'[{{"start":"{date_function()}"}},', file=p_file, flush=True)
    else:
        print(f'[{{"start":{date_function()}}},', file=p_file, flush=True)

def term_json_file(p_file, date_function=shared.timestamp_readable) -> None:
    if isinstance(date_function(),str):
        print(f'{{"stop":"{date_function()}"}}]', file=p_file, flush=True)
    else:
        print(f'{{"stop":{date_function()}}}]', file=p_file, flush=True)

def writeToLog_files():
    '''
        processes messages on the log queue and sends them to files, stdout, stderr acordingly
        runs on its own thread, does not run on the main PID!
    '''

    utils.block_signals()

    setproctitle.setproctitle('datacopy: log file writer')

    logFile = None
    statsFile = None
    global memoryStatsFile
    collectMemoryStatsNextTime = timer()+shared.collectMemoryStatsIntervalSecs
    dumpColNames = None
    dumpFile = None
    sLogFilePrefix = ''

    bKeepGoing=True
    idleCount=0
    while bKeepGoing:
        try:
            (logLevel, message, jobID, threadID, jobName, where, calledFrom) = shared.logQueue.get( block=True, timeout = .1 ) # type: ignore
            idleCount = 0

            if logLevel == logLevel.STATSONPROCNAME:
                setproctitle.setproctitle(f'datacopy: log writer [{message}]')
                continue

            timestamp = shared.logTimestampFunction()

            if jobName is None:
                jobName = ''

            if threadID is not None:
                sThreadID=f'#{threadID}'
            else:
                sThreadID=''

            if calledFrom is None:
                sCalledFrom=''
            else:
                sCalledFrom=f'<[{calledFrom}]'

            fullMessage = f'{timestamp}:{logLevel.name}:[{where}]{jobName}{sThreadID}{sCalledFrom}: {message}'

            if shared.SCREEN_STATS:
                fullScreenMessage=f'\n{fullMessage}'
            else:
                fullScreenMessage = fullMessage

            match logLevel:

                case logLevel.STATS:
                    if statsFile:
                        try:
                            print(message, file=statsFile, flush=True)
                        except Exception:
                            pass

                case logLevel.DEBUG:
                    if shared.DEBUG_TO_STDERR:
                        print(fullScreenMessage, file=sys.stderr, flush=True)
                    if shared.DEBUG_TO_LOG:
                        if logFile:
                            try:
                                print(fullMessage, file=logFile, flush=True)
                            except Exception:
                                pass

                case logLevel.INFO | logLevel.ERROR:
                    print(fullScreenMessage, file=sys.stdout, flush=True)
                    if logFile:
                        try:
                            print(fullMessage, file=logFile, flush=True)
                        except Exception:
                            pass

                case logLevel.DUMP_SHARED:
                    if shared.DEBUG_TO_STDERR:
                        print(f'{where}=[{message}]', file=sys.stderr, flush=True)
                    if shared.DEBUG_TO_LOG:
                        if logFile:
                            try:
                                print(f'{where}=[{message}]', file=logFile, flush=True)
                            except Exception:
                                pass

                case logLevel.DUMP_COLS:
                    dumpColNames = message

                case logLevel.DUMP_DATA:
                    if shared.DEBUG or shared.DUMP_ON_ERROR:
                        try:
                            dumpFile = open( f'{sLogFilePrefix}.DUMP', 'w', encoding = 'utf-8')
                            dumper=csv.writer(dumpFile, delimiter = shared.dumpFileSeparator, quoting = csv.QUOTE_MINIMAL)
                            dumper.writerow(dumpColNames) #type: ignore
                            dumper.writerows(utils.encodeSpecialChars(message))
                            dumpFile.close()
                        except Exception:
                            pass

                case logLevel.OPEN:
                    try:
                        if shared.DEBUG:
                            print(f'writeToLog_files: opening [{message}.running.log]', file=sys.stderr, flush=True)
                        sLogFilePrefix = message
                        #flush any previously open file
                        if logFile:
                            logFile.flush()
                            logFile.close()
                            logFile = None

                        logFile = open( f'{message}.running.log', 'a', encoding = 'utf-8')
                    except Exception as e:
                        print(f'writeToLog_files: could not open log file [{message}]: [{e}]', file=sys.stderr, flush=True)

                    try:

                        #flush any previously open file
                        if statsFile:
                            statsFile.flush()
                            statsFile.close()
                            statsFile = None

                        if shared.STATS_IN_JSON:
                            statsFile = open( f'{message}.stats.json', 'a', encoding = 'utf-8')
                            init_json_file(statsFile, shared.statsTimestampFunction)
                        else:
                            statsFile = open( f'{message}.stats.csv', 'a', encoding = 'utf-8')
                    except Exception as e:
                        print(f'writeToLog_files: could not open stats file [{message}]: [{e}]', file=sys.stderr, flush=True)

                    if shared.COLLECT_MEMORY_STATS:
                        logPrint('collect memory stats enabled', logLevel.DEBUG)
                        try:

                            #flush any previously open file
                            if memoryStatsFile:
                                memoryStatsFile.flush()
                                memoryStatsFile.close()
                                memoryStatsFile = None

                            if shared.MEMORY_STATS_IN_JSON:
                                memoryStatsFile = open( f'{message}.memory.json', 'a', encoding = 'utf-8')
                                init_json_file(memoryStatsFile, shared.memoryTimestampFunction)
                            else:
                                memoryStatsFile = open( f'{message}.memory.csv', 'a', encoding = 'utf-8')
                        except Exception as e:
                            print(f'writeToLog_files: could not open memory stats file [{message}]: [{e}]', file=sys.stderr, flush=True)

                case logLevel.CLOSE:
                    if shared.DEBUG:
                        print('writeToLog_files: received CLOSE message.', file=sys.stderr, flush=True)
                        setproctitle.setproctitle('datacopy: log file writer, closing')

                    if logFile:
                        logFile.flush()
                        logFile.close()
                        logFile = None

                        if shared.ErrorOccurred.value:
                            sLogFileFinalName = f'{sLogFilePrefix}.ERROR.log'
                        else:
                            sLogFileFinalName = f'{sLogFilePrefix}.ok.log'
                        try:
                            os.rename(f'{sLogFilePrefix}.running.log', sLogFileFinalName)
                        except Exception:
                            pass

                    if statsFile:
                        if shared.STATS_IN_JSON:
                            term_json_file(statsFile, shared.statsTimestampFunction)
                        statsFile.flush()
                        statsFile.close()
                        statsFile = None
                    if memoryStatsFile:
                        if shared.MEMORY_STATS_IN_JSON:
                            term_json_file(memoryStatsFile, shared.memoryTimestampFunction)
                        memoryStatsFile.flush()
                        memoryStatsFile.close()
                        memoryStatsFile = None

                case logLevel.END:
                    if shared.DEBUG:
                        print('writeToLog_files: received END message.', file=sys.stderr, flush=True)
                        setproctitle.setproctitle('datacopy: log file writer, ending')
                    bKeepGoing = False

        except queue.Empty:
            idleCount += 1
            if idleCount > 30:
                setproctitle.setproctitle(f'datacopy: log file writer, idleCount=[{idleCount}], Working=[{shared.Working.value}], eventQueueSize=[{shared.eventQueue.qsize()}], dataQueueSize=[{shared.dataQueue.qsize()}]')
        except OSError:
            if shared.DEBUG:
                print('writeToLog_files: EOError exception. giving up.', file=sys.stderr, flush=True)
            break
        except Exception as e:
            if shared.DEBUG:
                print(f'writeToLog_files: Exception at line ({sys.exc_info()[2].tb_lineno}): [{e}]', file=sys.stderr, flush=True)

        if shared.COLLECT_MEMORY_STATS and memoryStatsFile:
            if timer() > collectMemoryStatsNextTime:
                monitor_memory()
                collectMemoryStatsNextTime = timer()+shared.collectMemoryStatsIntervalSecs

    if shared.DEBUG:
        print('writeToLog_files: exiting...', file=sys.stderr, flush=True)
    setproctitle.setproctitle('datacopy: log file writer, ended')
