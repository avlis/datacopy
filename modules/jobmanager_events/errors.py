'''error related event handlers for jobManager'''
from timeit import default_timer as timer
import modules.shared as shared
import modules.logging as logging

def handle_read_error(eJobID, recs, secs, context):
    logging.statsPrint('readDataError', eJobID, context.iDataLinesRead[eJobID], secs, context.iRunningReaders)
    logging.processError(p_message='READ ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_stop=True, p_exitCode=6)

def handle_query_error(eJobID, recs, secs, context):
    logging.processError(p_message='QUERY ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_stop=True, p_exitCode=6)

def handle_cmd_error(eJobID, recs, secs, context):
    context.tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval
    context.iRunningStatements -= 1
    context.iActiveJobsOnThisStream -= 1
    logging.statsPrint(p_type='execStatementError', p_jobID=eJobID, p_recs=secs, p_secs=0, p_threads=0)
    logging.processError(p_message='Statement ERROR event', p_dontSendToStats=True, p_jobID=eJobID, p_stop=True, p_exitCode=8)
