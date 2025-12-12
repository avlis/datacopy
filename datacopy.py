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

    check README.md for more info.

'''

import sys
import os
import signal
import argparse

from time import sleep

import multiprocessing as mp
mp.set_start_method('fork')

from setproctitle import setproctitle

from typing import Any

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

    parser = argparse.ArgumentParser(description='datacopy utility.')
    parser.add_argument('action', help='Action to perform. Can be "copy", "checkConfigOnly", "convertConfig2Json", "testQueriesOnly", "testConnectionsOnly", or "help". If "help" is passed, a description of these commands will be provided. Defaults to "copy".', default='copy', nargs='?')
    parser.add_argument('-l', '--logname', help='Name of the log file. Defaults to LOG_NAME environment variable or current timestamp if not set.', default=None)
    parser.add_argument('-j', '--jobfile', help='Name of the job file. Defaults to JOB_FILE environment variable or "job.csv" if not set.', default=None)
    parser.add_argument('-c', '--connectionsfile', help='Name of the connections file. Defaults to CONNECTIONS_FILE environment variable or "connections.csv" if not set.', default=None)
    args = parser.parse_args()

    if args.action == 'help':
        print('''
        "copy" - This is the default action. It will copy data according to the configuration.
        "checkConfigOnly" - This will only check the configuration files for errors.
        "convertConfig2Json" - This will convert the configuration files to JSON format.
        "testQueriesOnly" - This will test the queries in the configuration files.
        "testConnectionsOnly" - This will test the connections in the configuration files.
        ''')
        return

    if args.logname is None:
        shared.logName = os.getenv('LOG_NAME',shared.timestamp_compact())
    else:
        shared.logName = args.logname

    if args.jobfile is None:
        q_filename = os.getenv('JOB_FILE','job.csv')
    else:
        q_filename = args.jobfile

    if args.connectionsfile is None:
        c_filename = os.getenv('CONNECTIONS_FILE','connections.csv')
    else:
        c_filename = args.connectionsfile

    setproctitle(f'datacopy: main thread [{q_filename}]')
    shared.applicationName = 'datacopy[{q_filename}]'

    logging.logThread = mp.Process(target=logging.writeToLog_files)
    logging.logThread.start()

    logging.openLog()

    logging.logPrint(f"datacopy version [{os.getenv('BASE_VERSION','<unkown>')}][{os.getenv('VERSION','<unkown>')}] starting")
    logging.logPrint(f'executionID: [{shared.executionID}], mode: [{args.action}]')
    logging.logPrint('copyData()', logLevel.DUMP_SHARED) #DISABLE_IN_PROD

    raw_jobs:dict[int, dict[str, Any]] = {}
    raw_connections:dict[int, dict[str, Any]] = {}

    if shared.Working.value:
        raw_connections = connections.load(c_filename)

    shared.connections = connections.preCheck(raw_connections)

    if shared.Working.value:
        raw_jobs = jobs.load(q_filename)

    if shared.Working.value:
        shared.jobs = jobs.preCheck(raw_jobs)

    if args.action == 'checkConfigOnly':
        logging.logPrint('configuration is valid.')
        logging.closeLog()

    if args.action == 'testConnectionsOnly':
        connections.testConnections()
        logging.logPrint('testing connections only, exiting here.')
        logging.closeLog()

    if args.action == 'testQueriesOnly':
        shared.TEST_QUERIES = True

    if args.action in ('copy', 'testQueries'):
        if shared.Working.value:
            cd = mp.Process(target=jobshandler.jobManager)
            cd.start()
            while shared.Working.value:
                sleep(2)
            cd.join()

    logging.logPrint('exited copydata!', logLevel.DEBUG)

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
            shared.writeP[i].join(timeout=1)
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

    print('closeLog: making sure log thread is terminated...', file=sys.stderr, flush=True) #DISABLE_IN_PROD
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
