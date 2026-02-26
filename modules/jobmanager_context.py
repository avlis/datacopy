'''state management for jobManager'''
from typing import Dict, Optional, Any

class StreamContext:
    def __init__(self):
        # Global stream counters
        self.jobID: int = 1
        self.iWriters: int = 0
        self.iRunningReaders: int = 0
        self.iReadingReaders: int = 0
        self.iRunningQueries: int = 0
        self.iRunningStatements: int = 0
        self.tParallelReadersNextCheck: float = 0
        self.iIdleTimeout: int = 0
        self.iActiveJobsOnThisStream: int = 0
        self.bKeepGoing: bool = True
        self.bStopRequested: bool = False
        
        # Performance/Logging metrics
        self.iDataLinesRead: Dict[int, int] = {}
        self.fReadSecs: Dict[int, float] = {}
        self.iDetailsQueriesSecs: Dict[int, float] = {}
        
        # Per-job state (re-initialized for each job stream)
        self.thisJob: Any = None
        self.jobName: str = '<unknown>'
        self.sWriteFileMode: str = 'w'
        self.sCSVHeader: str = ''
        
        self.iTotalDataLinesRead: int = 0
        self.fTotalReadSecs: float = .001
        
        self.iRunningWriters: int = 0
        self.iTotalDataLinesWritten: int = 0
        self.fTotalWrittenSecs: float = .001
        
        self.writersNotStartedYet: bool = True
        self.oMaxAlreadyInsertedData: Any = None
        self.bEndOfJobs: bool = False
        
        # Internal loop control
        self.bReadyToStop: bool = False
        self.dumpedPackets: int = 0
        self.emptyQueueTimeout: int = 5 # default from jobmanager.py

    def reset_job_state(self, p_job):
        '''resets the context for a new destination table/job stream'''
        self.thisJob = p_job
        self.jobName = p_job.jobName
        
        self.sWriteFileMode = 'w'
        self.sCSVHeader = ''
        
        self.iActiveJobsOnThisStream = 0
        self.iTotalDataLinesRead = 0
        self.fTotalReadSecs = .001
        
        self.iRunningWriters = 0
        self.iTotalDataLinesWritten = 0
        self.fTotalWrittenSecs = .001
        
        self.iDataLinesRead[self.jobID] = 0
        self.fReadSecs[self.jobID] = .001
        self.iDetailsQueriesSecs[self.jobID] = 0
        
        self.writersNotStartedYet = True
        self.oMaxAlreadyInsertedData = None
        self.bEndOfJobs = False
        
        self.bReadyToStop = False
        self.dumpedPackets = 0
        self.emptyQueueTimeout = 5
