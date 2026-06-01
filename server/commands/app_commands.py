import argparse
import cmd2
from server.utils.logger import get_logger
import time



POSSIBLE_MODES = ['test', 'calibration', 'multipmt']
logger = get_logger('generic_commands')


##################
#GENERIC COMMANDS#
##################

mode_parser = argparse.ArgumentParser()
mode_parser.add_argument("mode", action="store", type=str, help="Acquisition Mode (test (no HV); calibration, multiPMT)", default="test")

@cmd2.with_argparser(mode_parser)
@cmd2.with_category("Generic Commands")
def do_change_mode(self, args: argparse.Namespace) -> None:
    """Change the acquisition mode at runtime
       Test: no HV connected
       Calibration: PMTs characterization
       MultiPMT: Full acquisition
       Default: Test
    """
    mode = args.mode.lower()
    if mode not in POSSIBLE_MODES:
        self.poutput(f"Invalid mode. Choose from: {POSSIBLE_MODES}")
        logger.warning(f"Invalid mode: {mode}. Choose from: {POSSIBLE_MODES}")
        return
    if mode == self.mode:
        self.poutput(f"Already in {mode} mode")
        logger.info(f"Already in {mode} mode")
        return
    old_mode = self.mode
    self.mode = mode
    self.prompt = f"Server[{mode}]> "
    self.poutput(f"Mode changed from {old_mode} to {mode}")
    logger.info(f"Mode changed from {old_mode} to {mode}")
    
    if mode == 'test':
        self.poutput("Test mode: no HV connected")
    elif mode == 'calibration':
        self.poutput("Calibration mode: PMTs characterization")
    elif mode == 'multipmt':
        self.poutput("MultiPMT mode: Full acquisition")


@cmd2.with_category("Generic Commands")
def do_quit(self,_) -> bool:
    """Send quit command to all connected clients"""

    success = self.control_manager.notify_shutdown_to_all_clients()
    if not success:
        self.poutput("Failed to send quit command to all the clients")
        return False
    
    list_connected_clients = self.control_manager.connected_clients
    if list_connected_clients:
        self.poutput("Sent quit command to all connected clients.\n Quitting server.")
    else:
        self.poutput("Server shutting down ...")
    return True


#####################
#CONNECTION COMMANDS#
#####################

connect_parser = argparse.ArgumentParser()
connect_parser.add_argument("--num_clients", type=int, help="The number of clients expected to connect", default=1)
connect_parser.add_argument("--port", type=int, help="Selects the port to establish the connection", default=8888)

@cmd2.with_argparser(connect_parser)
@cmd2.with_category("Connection Commands")
def do_connect(self, args: argparse.Namespace) -> None:
    """Wait for one or more clients to connect through the control plane."""

    requested_clients = args.num_clients

    logger.info(f"Connect command received: num_clients={requested_clients}")

    if requested_clients <= 0:
        self.poutput("num_clients must be greater than 0")
        logger.warning(f"Invalid num_clients value: {requested_clients}")
        return

    listener_was_running = (
        self.control_manager.listener_thread is not None
        and self.control_manager.listener_thread.is_alive()
    )

    if listener_was_running:
        self.poutput("Stopping control listener temporarily to accept new handshakes...")
        self.control_manager.stop_listener()

    if self.control_manager.socket is None:
        started = self.control_manager.start_connection(port=args.port)
        if not started:
            self.poutput("Failed to start control connection.")
            logger.error(f"Failed to start control connection on port {args.port}")

            if listener_was_running:
                self.control_manager.start_listener()

            return

    already_connected = len(self.control_manager.list_connected_clients())
    target_total = max(requested_clients, already_connected)

    self.control_manager.num_multi_clients = target_total
    self.control_manager.clear_queues()

    self.poutput(
        f"Trying to reach {target_total} connected client(s). "
        f"Currently connected: {already_connected}."
    )

    self.control_manager.handshake()

    connected = self.control_manager.list_connected_clients()
    decoded_clients = [cid.decode(errors="ignore") for cid in connected]

    if connected:
        self.poutput(
            f"Control plane ready with {len(connected)}/{target_total} client(s)."
        )
        self.poutput(f"Connected clients: {decoded_clients}")
        logger.info(
            f"Control plane ready with {len(connected)}/{target_total} clients: {decoded_clients}"
        )
    else:
        self.poutput("No clients connected.")
        logger.warning("No clients connected after connect command.")

    self.control_manager.clear_queues()

    if not self.control_manager.start_listener():
        logger.error("Failed to start control listener")
        self.poutput("Failed to start control listener")
        return


#########################
#HANDLING EVENT MESSAGES#
#########################

def _print_bad_channels_event(self, payload):
    self.poutput("\n[WARNING] HV channels became BAD")

    for item in payload.get("details", []):
        ch = item.get("channel")
        status = item.get("status")
        alarm = item.get("alarm")
        action = item.get("action")

        self.poutput(
            f"  CH {ch}: status={status}, alarm={alarm}, action={action}"
        )

def _print_recovered_channels_event(self, payload):
    self.poutput("\n[INFO] HV channels recovered")

    details = payload.get("details", {})

    recovered = details.get("recovered_channels", [])
    recovered_on = details.get("recovered_on_channels", [])
    recovered_off = details.get("recovered_off_channels", [])

    for ch in recovered:
        state = "unknown"

        if ch in recovered_on:
            state = "ON"

        elif ch in recovered_off:
            state = "OFF"

        self.poutput(
            f"  CH {ch}: recovered and moved to OK ({state})"
        )

def _print_power_alignment_event(self, payload):
    self.poutput("\n[INFO] HV power state aligned")

    moved_to_on = payload.get("moved_to_on_channels", [])
    moved_to_off = payload.get("moved_to_off_channels", [])

    for ch in moved_to_on:
        self.poutput(
            f"  CH {ch}: software OFF -> hardware UP, moved to ON"
        )

    for ch in moved_to_off:
        self.poutput(
            f"  CH {ch}: software ON -> hardware DOWN, moved to OFF"
        )

def handle_event(self, message):
    payload = message.payload
    event = payload.get("event", "unknown")

    if event == "hv_channels_became_bad":
        _print_bad_channels_event(self, payload)

    elif event == "hv_channels_recovered":
        _print_recovered_channels_event(self, payload)

    elif event == "hv_power_state_aligned":
        _print_power_alignment_event(self, payload)

    else:
        self.poutput(f"\n[INFO] {event}: {payload}")
            
