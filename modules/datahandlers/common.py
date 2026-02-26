'''common utilities for data handlers'''
import os
import traceback

from timeit import default_timer as timer

from typing import Optional

from multiprocessing import Queue
from queue import Empty as queueEmpty

from setproctitle import setproctitle

import modules.shared as shared
import modules.utils as utils
import modules.logging as logging
from modules.logging import logLevel as logLevel

def playNice():
    try:
        os.setpriority(os.PRIO_PROCESS, 0, shared.parallelProcessesNiceness)
    except:
        pass

def executeStatement(p_jobID:int, p_connection, p_cursor, p_target:str, p_query:str):
    '''executes commands on sources or destinations'''

    playNice()

    utils.block_signals()

    logging.logPrint(f'Started, with cursor=[{id(p_cursor)}], target=[{p_target}]', logLevel.DEBUG, p_jobID=p_jobID)
    tStart = timer()

    jobName = shared.getJobName(p_jobID)
    processTitlePrefix:str =f'datacopy: executeStatement[{p_target}]'

    try:
        setproctitle(f'{processTitlePrefix}(query) [{jobName}]')

        shared.eventQueue.put( (shared.E_CMD_START, p_jobID,  None, None) )

        p_cursor.execute(p_query)

    except Exception as e:
        setproctitle(f'{processTitlePrefix}(error@query) [{jobName}]')
        logging.processError(p_e=e, p_message=f'executing query, conn=[{p_connection}]', p_jobID=p_jobID, p_dontSendToStats=True)
        shared.eventQueue.put( (shared.E_CMD_ERROR, p_jobID, None, (timer() - tStart)) )

    setproctitle(f'{processTitlePrefix}(closing cursor) [{jobName}]')
    try:
        p_cursor.close()
    except Exception:
        pass

    setproctitle(f'{processTitlePrefix}(closing connection) [{jobName}]')
    try:
        p_connection.close()
    except Exception:
        pass

    shared.eventQueue.put( (shared.E_CMD_END, p_jobID,  None, (timer() - tStart)) )

    setproctitle(f'{processTitlePrefix}(finished) [{jobName}]')
    logging.logPrint('Ended', logLevel.DEBUG, p_jobID=p_jobID)
