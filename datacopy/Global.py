
class Global:
    def __init__(self):
        None
    # GLOBAL VARS

    expected_conns_columns=("name","driver","server","database","user","password")
    expected_query_columns=("source","dest","mode","query","table")

    g_dataBuffer=queue.Queue(16)
    g_readRecords=queue.Queue()
    g_writtenRecords=queue.Queue()

    g_defaultFetchSize=1000

    g_fetchSize=g_defaultFetchSize

    g_readT = False
    g_writeT = False
    g_Working = True

    g_ErrorOccurred=False