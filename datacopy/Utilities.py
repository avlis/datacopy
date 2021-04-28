def logPrint(psErrorMessage, p_logfile=''):
    sMsg = "{0}: {1}".format(str(datetime.now()), psErrorMessage)
    print(sMsg, file = sys.stderr, flush=True)
    if p_logfile!='':
        print(sMsg, file = p_logfile, flush=True)

def sig_handler(signum, frame):
    global g_readT
    global g_writeT
    global g_Working
    logPrint("\nsigHander: break received, signaling stop to threads...")
    g_Working = False
    g_ErrorOccurred = True    
    while True:
        try:
            dummy=g_dataBuffer.get(block=False, timeout=1)
        except queue.Empty:
            break
    try:
        if g_readT:
            print("sigHandler: waiting for read thread...", flush=True)
            g_readT.join()
        if g_writeT:
            print("sigHandler: waiting for write thread...", flush=True)
            g_writeT.join()
    finally:
        logPrint("sigHandler: thread cleanup finished.")        