class Reader:
    def __init__(self):
        None   
        
    def DB(self, p_index, p_connection, p_cursor):
        global g_Working
        global g_dataBuffer
        global g_readRecords
        global g_fetchSize
        global g_ErrorOccurred


        print("readData({0}): Started".format(p_index), flush=True)
        while g_Working:
            try:
                rStart=timer()
                bData = p_cursor.fetchmany(g_fetchSize)
            except (Exception) as error:
                logPrint("ReadData({0}): DB Error: [{1}]".format(p_index, error))
                g_ErrorOccurred=True
                break
            g_readRecords.put( (p_cursor.rowcount, (timer()-rStart)) )
            g_dataBuffer.put(bData, block=True)
            if not bData:
                break
        g_dataBuffer.put(False)
        g_readRecords.put((False,float(0)))
        print("\nreadData({0}): Ended".format(p_index), flush=True)

    def CSV(self):
        None

    def Elastic(self):
        None
