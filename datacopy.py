#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pylint: disable=invalid-name, wrong-import-position, line-too-long, broad-exception-caught

'''
script to copy loads of data between databases
- reads by default connections.csv and jobs.csv
- parameters on command line:
    -- connections file (csv, tab delimited)
    -- jobs.csv file (csv, tab delimited)
    -- log file prefix

or by ENV var:
    -- CONNECTIONS_FILE
    -- JOB_FILE
    -- LOG_FILE
    -- TEST_QUERIES (dry run, default no)
    -- QUEUE_SIZE (default 256)
    -- QUEUE_FB4NEWR (queue free before new read, when reuse_writers=yes, default 1/3 off queue)
    -- REUSE_WRITERS (default no)
    -- STOP_JOBS_ON_ERROR (default yes)
    -- DUMP_ON_ERROR (default no)
    -- DUMPFILE_SEP (default '|')
    -- STATS_IN_JSON (default no)
    -- PARALLEL_READERS (default 1)

'''

import sys
import os
import signal

import multiprocessing as mp
mp.set_start_method('fork')

import setproctitle

import modules.shared as shared
import modules.logging as logging
import modules.connections as connections
import modules.jobs as jobs
import modules.jobshandler as jobshandler

def sig_handler(signum, frame):
    '''handles signals'''

    p = mp.current_process()
    if p.name == 'MainProcess':
        logging.statsPrint('stopRequestedError', 'MainThread', 0, 0, 0)
        logging.logPrint(f'sigHander: Error: stop signal received ({signum},{frame}), signaling stop to threads...')
        shared.ErrorOccurred.value = True
        shared.Working.value = False

# MAIN
def Main():
    '''entry point'''

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    if len(sys.argv) < 4:
        shared.logFileName = os.getenv('LOG_FILE','')
    else:
        shared.logFileName = sys.argv[3]

    if len(sys.argv) < 3:
        q_filename = os.getenv('JOB_FILE','jobs.csv')
    else:
        q_filename = sys.argv[2]

    if len(sys.argv) < 2:
        c_filename = os.getenv('CONNECTIONS_FILE','connections.csv')
    else:
        c_filename = sys.argv[1]

    setproctitle.setproctitle(f'datacopy: main thread [{q_filename}]')

    logging.logThread = mp.Process(target=logging.writeLogFile)
    logging.logThread.start()

    connections.loadConnections(c_filename)
    jobs.loadJobs(q_filename)

    jobs.preCheck()
    jobshandler.copyData()

    logging.logPrint('main: exited copydata!', shared.L_DEBUG)

    #pylint: disable=consider-using-dict-items
    logging.logPrint(f'main: making sure all read threads are terminated [{len(shared.readP)}]...', shared.L_DEBUG)
    for i in shared.readP:
        try:
            shared.readP[i].join(timeout=1)
            shared.readP[i].terminate()
            shared.readP[i].join(timeout=1)
        except Exception as error:
            logging.logPrint(f'main: error terminating read thread {i} ({sys.exc_info()[2].tb_lineno}): [{error}]', shared.L_DEBUG)
            continue

    logging.logPrint(f'main: making sure all write threads are terminated [{len(shared.writeP)}]...', shared.L_DEBUG)
    for i in shared.writeP:
        try:
            shared.readP[i].join(timeout=1)
            shared.writeP[i].terminate()
            shared.writeP[i].join(timeout=1)
        except Exception as error:
            logging.logPrint(f'main: error terminating write thread {i} ({sys.exc_info()[2].tb_lineno}): [{error}]', shared.L_DEBUG)
            continue

    logging.logPrint('main: all worker threads terminated and joined.', shared.L_DEBUG)


    if shared.ErrorOccurred.value:
        logging.closeLogFile(6)

    else:
        logging.closeLogFile(0)

    if shared.DEBUG:
        print ('main: end of Main()', file=sys.stderr, flush=True)

if __name__ == '__main__':
    Main()
