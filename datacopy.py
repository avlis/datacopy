#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pylint: disable=invalid-name,wrong-import-position

"""
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

"""

import sys
import os
import signal

import multiprocessing as mp
mp.set_start_method('fork')

import modules.shared as shared
import modules.logging as logging
import modules.connections as connections
import modules.jobs as jobs
import modules.jobshandler as jobshandler

def sig_handler(signum, frame):
    '''handles signals'''

    p = mp.current_process()
    if p.name == "MainProcess":
        logging.statsPrint('ERROR', 'CONTROL-C', 0, 0, 0)
        logging.logPrint("sigHander: Error: break signal received ({0},{1}), signaling stop to threads...".format(signum, frame))
        shared.ErrorOccurred.value = True
        shared.Working.value = False

# MAIN
def Main():
    '''entry point'''

    signal.signal(signal.SIGINT, sig_handler)

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

    logProcessor=mp.Process(target=logging.writeLogFile)
    logProcessor.start()

    logging.logPrint('datacopy version [{0}] starting'.format(os.getenv('VERSION','<unkown>')))

    connections.loadConnections(c_filename)
    jobs.loadJobs(q_filename)

    jobs.preCheck()
    jobshandler.copyData()
    print ("exited copydata!")
    if shared.ErrorOccurred.value:
        logging.closeLogFile(6)

    else:
        logging.closeLogFile(0)

if __name__ == '__main__':
    Main()
