'''command related event handlers for jobManager'''
import modules.shared as shared
import modules.logging as logging

def handle_cmd_start(eJobID, recs, secs, context):
    context.tParallelReadersNextCheck = float('inf')
    context.iRunningStatements += 1
    logging.statsPrint(p_type='execStatementStart', p_jobID=eJobID, p_recs=0, p_secs=0, p_threads=1)

def handle_cmd_end(eJobID, recs, secs, context):
    from timeit import default_timer as timer
    context.tParallelReadersNextCheck = timer() + shared.parallelReadersLaunchInterval
    context.iRunningStatements -= 1
    logging.statsPrint(p_type='execStatementEnd', p_jobID=eJobID, p_recs=0, p_secs=secs, p_threads=0)
    context.iActiveJobsOnThisStream -= 1
