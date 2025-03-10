#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pylint: disable=invalid-name, wrong-import-position, line-too-long, broad-exception-caught, bare-except

'''
script to copy loads of data between databases
- reads by default connections.csv and jobs.csv
- parameters on command line:
    -- connections file (csv, tab delimited)
    -- jobs.csv file (csv, tab delimited)
    -- log file prefix

or by ENV vars, some examples:
    -- CONNECTIONS_FILE
    -- JOB_FILE
    -- LOG_NAME
    -- TEST_QUERIES (dry run, default no)
    -- QUEUE_SIZE (default 256)
    -- QUEUE_FB4NEWR (queue free before new read, when reuse_writers=yes, default 1/3 off queue)
    -- REUSE_WRITERS (default no)
    -- DUMP_ON_ERROR (default no)
    -- DUMPFILE_SEP (default '|')
    -- STATS_IN_JSON (default no)
    -- PARALLEL_READERS (default 1)

'''

import sys
import os
import signal

from time import sleep

import multiprocessing as mp
mp.set_start_method('fork')

import setproctitle

import modules.shared as shared
import modules.logging as logging
from modules.logging import logLevel as logLevel
import modules.connections as connections
import modules.jobs as jobs
import modules.jobshandler as jobshandler


def sig_handler(signum, frame):
    '''handles signals'''

    p = mp.current_process()
    print(f'process [{p.name}] received signal [{signum}]', file=sys.stderr, flush=True) #DISABLE_IN_PROD
    if p.name == 'MainProcess':
        logging.statsPrint('stopRequested', None, 0, 0, 0)
        logging.processError(p_message=f'sigHander: Error: stop signal received ({signum})', p_dontSendToStats=True, p_stop=True, p_exitCode=15)

# MAIN
def Main():
    '''entry point'''

    print ('start of Main()', file=sys.stderr, flush=True) #DISABLE_IN_PROD

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    #argv[3] is now interpreted on shared

    if len(sys.argv) < 3:
        q_filename = os.getenv('JOB_FILE','jobs.csv')
    else:
        q_filename = sys.argv[2]

    if len(sys.argv) < 2:
        c_filename = os.getenv('CONNECTIONS_FILE','connections.csv')
    else:
        c_filename = sys.argv[1]

    setproctitle.setproctitle(f'datacopy: main thread [{q_filename}]')
    shared.applicationName = 'datacopy[{q_filename}]'

    logging.logThread = mp.Process(target=logging.writeToLog_files)
    logging.logThread.start()

    logging.openLog()

    logging.logPrint(f"datacopy version [{os.getenv('BASE_VERSION','<unkown>')}][{os.getenv('VERSION','<unkown>')}] starting")
    logging.logPrint(f'executionID: [{shared.executionID}]')
    logging.logPrint('copyData()', logLevel.DUMP_SHARED) #DISABLE_IN_PROD

    if shared.Working.value:
        connections.loadConnections(c_filename)

    if shared.Working.value:
        jobs.loadJobs(q_filename)

    if shared.Working.value:
        jobs.preCheck()

    if shared.Working.value:
        cd = mp.Process(target=jobshandler.jobManager)
        cd.start()
        while shared.Working.value:
            sleep(2)
        cd.join()

    logging.logPrint('exited copydata!', logLevel.DEBUG)

    #pylint: disable=consider-using-dict-items
    logging.logPrint(f'making sure all read threads are terminated [{len(shared.readP)}]...', logLevel.DEBUG)
    for i in shared.readP:
        try:
            shared.readP[i].join(timeout=1)
            shared.readP[i].terminate()
            shared.readP[i].join(timeout=1)
        except Exception as error:
            logging.logPrint(f'error terminating read thread {i} ({sys.exc_info()[2].tb_lineno}): [{error}]', logLevel.DEBUG)
            continue

    logging.logPrint(f'making sure all write threads are terminated [{len(shared.writeP)}]...', logLevel.DEBUG)
    for i in shared.writeP:
        try:
            shared.readP[i].join(timeout=1)
            shared.writeP[i].terminate()
            shared.writeP[i].join(timeout=1)
        except Exception as error:
            logging.logPrint(f'error terminating write thread {i} ({sys.exc_info()[2].tb_lineno}): [{error}]', logLevel.DEBUG)
            continue

    logging.logPrint('all worker threads terminated and joined.', logLevel.DEBUG)

    if shared.ErrorOccurred.value and shared.exitCode.value == 0 :
        with shared.exitCode.get_lock():
            shared.exitCode.value = 32

    logging.closeLog()

    print('closeLog: making sure log thread is terminated...', file=sys.stderr, flush=True) #DISABLE_IN_PTOD
    try:
        logging.logThread.join(timeout=1)
        logging.logThread.terminate()
        logging.logThread.join(timeout=1)

    except Exception as e:
        print(f'closeLog: error terminating log thread ({sys.exc_info()[2].tb_lineno}): [{e}]', file=sys.stderr, flush=True)

    if shared.DEBUG:
        print('closeLog: log thread terminated and joined.', file=sys.stderr, flush=True)


    if shared.DEBUG:
        print ('end of Main()', file=sys.stderr, flush=True)

    sys.exit(shared.exitCode.value)

if __name__ == '__main__':
    Main()
