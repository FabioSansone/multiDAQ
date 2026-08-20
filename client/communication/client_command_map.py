from client.communication.handlers.hv_handlers import *
from client.communication.handlers.rc_handlers import *
from client.communication.handlers.system_handlers import *
from client.communication.handlers.acquisition_handler import *
from client.communication.handlers.feb_handlers import *
from client.communication.handlers.main_handlers import *

COMMAND_MAP = {
    #System Handlers
    "server_shutdown": handle_server_shutdown,
    'set_acq_mode_sync': handle_set_acq_mode_sync,
    
    #HV Handlers
    "hv_on": handle_hv_on,
    "hv_off": handle_hv_off,
    
    "set_common_voltage": handle_hv_set_common_voltage,
    "set_common_threshold": handle_hv_set_common_threshold,
    "set_acquisition_configuration": handle_hv_set_acquisition_configuration,
    
    "set_hv_sync": handle_hv_set_hv_sync,

    "hv_on_and_wait": handle_hv_set_on_and_wait,
    "hv_off_and_wait": handle_hv_off_and_wait,

    "mark_bad": handle_hv_set_user_bad,
    "unmark_bad": handle_hv_unset_user_bad,
    
    "set_pmt_serials": handle_hv_set_pmt_serials,
    
    "get_serial_map": handle_get_serial_map,
    
    "hv_monitor_snapshot": handle_hv_monitor_snapshot,
    
    #RC Handlers
    "rc_acq_start": handle_rc_start_acquisition_mode,
    "rc_boot": handle_rc_boot_mode,
    "rc_reset": handle_rc_reset,

    "rc_read_register": handle_rc_read_register,
    "rc_write_register": handle_rc_write_register,
    "rc_read_acq_registers": handle_rc_read_acq_registers,
    "set_rc_acq": handle_rc_set_acq_registers,
    
    "rc_all_rate_monitoring": handle_rc_all_rate_monitoring,
    
    #FEB Handlers
    "feb_program": handle_feb_program,
    
    #Main Handlers
    "main_read_snapshot": handle_main_read_snapshot,
}