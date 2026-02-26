'''writing related event handlers for jobManager'''
from timeit import default_timer as timer
import modules.shared as shared
import modules.logging as logging
from modules.logging import logLevel as logLevel

def handle_write_start(eJobID, recs, secs, context):
    pass # Already logged in read_start usually? No, E_WRITE_START is empty in jobmanager.py

def handle_write(eJobID, recs, secs, context):
    context.iTotalDataLinesWritten += recs
    context.fTotalWrittenSecs += secs

def handle_write_error(eJobID, recs, secs, context):
    logging.statsPrint('writeDataError', eJobID, context.iTotalDataLinesWritten, -1, context.iRunningWriters)
    logging.processError(p_message='WRITE ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_threadID=recs, p_stop=True, p_exitCode=7)

def handle_write_end(eJobID, recs, secs, context):
    context.iRunningWriters -= 1
    try:
        shared.writeP[eJobID].join(timeout=1)
    except:
        pass
