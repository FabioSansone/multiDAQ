"""Timeouts of the commands between server and client, in one place so the layers stay aligned:

    hardware wait (interface)  <  client budget (handler)  <  server wait

- The hardware layer waits at most RAMP_WAIT_S for the end of a HV ramp: RAMP_S is the
  worst ramp seen on the real hardware, with a margin for polling and settling.
- The client budget (CLIENT_TIMEOUT_S) adds HW_MARGIN_S for the HV queue, the Modbus I/O
  and the reply.
- The server waits SERVER_MARGIN_S more, so that the error reply of a client that ran out
  of time arrives before the server gives up (and is not discarded as a late reply).

Keys are the command names on the wire (the ones of client_command_map).
"""

RAMP_S = 300.0
RAMP_WAIT_S = RAMP_S + 60.0
HW_MARGIN_S = 60.0
SERVER_MARGIN_S = 30.0

DEFAULT_CLIENT_TIMEOUT_S = 35.0

# connect waits for the clients until this long passes without a new one arriving
CONNECT_CLIENT_WAIT_S = 20.0

# The server sends an empty frame to every registered client at this interval: with
# ROUTER_MANDATORY the send fails as soon as the connection of the client is gone.
LIVENESS_PROBE_INTERVAL_S = 10.0
HANDSHAKE_STEP_TIMEOUT_S = 20.0

# The client says hello on the acquisition and monitoring planes right after its control handshake, but
# the server admits it on the control plane in another thread: the hello can arrive first. The server keeps
# it this long and tries again at every turn of the I/O loop.
LATE_HELLO_GRACE_S = 3.0

# The client says hello on the acquisition and monitoring planes right after its control handshake, but a
# server running connect starts serving them only when the wait for the other clients is over: the client
# must wait for the ready longer than that, or it gives up just before the server answers.
PLANE_HELLO_WAIT_S = CONNECT_CLIENT_WAIT_S + HANDSHAKE_STEP_TIMEOUT_S

# A client that comes back on the monitoring plane gets its sample streams started again. The command can
# reach it before its own command thread is up: the reconciliation is tried a few times, a few seconds apart.
STREAM_RECONCILE_ATTEMPTS = 3
STREAM_RECONCILE_RETRY_S = 5.0

# Commands that wait for a ramp (hardware wait RAMP_WAIT_S).
_RAMP_COMMAND_S = RAMP_WAIT_S + HW_MARGIN_S
_HV_SYNC_S = 90.0
_HV_ON_S = 90.0

CLIENT_TIMEOUT_S = {
    # HV
    "set_common_voltage": 35.0,
    "set_common_threshold": 35.0,
    "set_acquisition_configuration": _RAMP_COMMAND_S,
    "get_voltage_channels": 30.0,
    "get_threshold_channels": 30.0,
    "get_volt_thr_channels": 40.0,
    "get_power_state_channels": _RAMP_COMMAND_S,
    "hv_on": _HV_ON_S,
    "hv_off": 90.0,
    "hv_force_off": 90.0,
    "set_hv_sync": _HV_SYNC_S,
    "hv_on_and_wait": _RAMP_COMMAND_S,
    "hv_off_and_wait": _RAMP_COMMAND_S,
    "mark_bad": 150.0,
    "unmark_bad": 150.0,
    "set_pmt_serials": 60.0,
    "get_serial_map": 60.0,
    "hv_monitor_snapshot": 60.0,
    # RC
    "rc_acq_start": 30.0,
    "rc_boot": 30.0,
    "rc_reset": 30.0,
    "rc_read_register": 30.0,
    "rc_write_register": 30.0,
    "rc_read_acq_registers": 30.0,
    "set_rc_acq": 30.0,
    "rc_all_rate_monitoring": 30.0,
    "feb_program": 600.0,
    # Main board and monitoring
    "main_read_snapshot": 30.0,
    "main_sensor_status": 30.0,
    "sample_start": 10.0,
    "sample_stop": 10.0,
    # Composite commands: the budget is the sum of the steps the client can run.
    # get_multipmt_configuration also reads the power state, which can wait for a ramp.
    "get_multipmt_configuration": _RAMP_COMMAND_S,
    # fixed-bad update (off_and_wait + sync), then off_and_wait, set_acquisition_configuration, hv_on
    "set_acq_mode_sync": (
        _RAMP_COMMAND_S + _HV_SYNC_S
        + _RAMP_COMMAND_S + _RAMP_COMMAND_S + _HV_ON_S
    ),
}


def client_timeout(command: str) -> float:
    """Time the client allows itself for `command`."""
    return CLIENT_TIMEOUT_S.get(command, DEFAULT_CLIENT_TIMEOUT_S)


def server_timeout(command: str) -> float:
    """Time the server waits for the reply to `command`."""
    return client_timeout(command) + SERVER_MARGIN_S
