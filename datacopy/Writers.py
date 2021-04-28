class writeDB(p_index, p_connection, p_cursor, p_iQuery):
    global g_Working
    global g_dataBuffer
    global g_writtenRecords
    global g_ErrorOccurred

    print("writeData({0}): Started".format(p_index), flush=True)
    while g_Working:
        try:
            bData = g_dataBuffer.get(block=False,timeout=2)
        except queue.Empty:
            continue
        if not bData:
            print ("\nwriteData({0}): 'no more data' message received".format(p_index), flush=True)
            break
        iStart=timer()
        try:
            iResult  = p_cursor.executemany(p_iQuery, bData)
            p_connection.commit()
        except (Exception) as error:
            logPrint("writeData({0}): DB Error: [{1}]".format(p_index,error))
            p_connection.rollback()
            g_ErrorOccurred=True
            break
        g_writtenRecords.put( (p_cursor.rowcount, (timer()-iStart)) )

    g_writtenRecords.put( (False, float(0)) )
    print("\nwriteData({0}): Ended".format(p_index), flush=True)