from client.communication.handlers.hv_handlers import *
from client.communication.handlers.rc_handlers import *
from client.communication.handlers.system_handlers import *

COMMAND_MAP = {
    #System Handlers
    "server_shutdown": handle_server_shutdown,
    
    #HV Handlers
    "hv_on": handle_hv_on,
    "hv_off": handle_hv_off,
    
    "set_common_voltage": handle_hv_set_common_voltage,
    "set_common_threshold": handle_hv_set_common_threshold,
    
    "set_hv_sync": handle_hv_set_hv_sync,
    
    #RC Handlert
    "rc_acq_start": handle_rc_start_acquisition_mode,
}