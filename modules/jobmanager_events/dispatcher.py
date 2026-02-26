'''event dispatcher for jobManager'''
import modules.shared as shared
from modules.jobmanager_events import system, reading, writing, errors, commands

handlers = {
    shared.E_NOOP:                  system.handle_noop,
    shared.E_STOP:                  system.handle_stop,
    shared.E_BOOT:                  system.handle_boot,
    shared.E_BOOT_READER:           system.handle_boot_reader,
    shared.E_BOOT_CMD:              system.handle_boot_cmd,
    
    shared.E_QUERY_START:           reading.handle_query_start,
    shared.E_QUERY_END:             reading.handle_query_end,
    shared.E_KEYS_QUERY_END:        reading.handle_keys_query_end,
    shared.E_DETAIL_QUERY_END:      reading.handle_detail_query_end,
    shared.E_READ_START:            reading.handle_read_start,
    shared.E_READ:                  reading.handle_read,
    shared.E_READ_END:              reading.handle_read_end,
    shared.E_KEYS_READ_START:       reading.handle_keys_read_start,
    shared.E_KEYS_READ_END:         reading.handle_keys_read_end,
    
    shared.E_WRITE_START:           writing.handle_write_start,
    shared.E_WRITE:                 writing.handle_write,
    shared.E_WRITE_END:             writing.handle_write_end,
    shared.E_WRITE_ERROR:           writing.handle_write_error,
    
    shared.E_READ_ERROR:            errors.handle_read_error,
    shared.E_QUERY_ERROR:           errors.handle_query_error,
    shared.E_CMD_ERROR:             errors.handle_cmd_error,
    
    shared.E_CMD_START:             commands.handle_cmd_start,
    shared.E_CMD_END:               commands.handle_cmd_end,
}

def dispatch(eType, eJobID, recs, secs, context):
    '''routes an event to its handler'''
    handler = handlers.get(eType)
    if handler:
        handler(eJobID, recs, secs, context)
    else:
        import modules.logging as logging
        logging.logPrint(f'unknown event in dispatcher ({eType}), should not happen!', p_jobID=eJobID)
