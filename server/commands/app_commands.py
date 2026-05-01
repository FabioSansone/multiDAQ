import argparse
import cmd2
from experimental.server.utils.logger import get_logger



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
def do_quit(self,) -> None:
    """Send quit command to all connected clients"""

    success = self.control_manager.notify_shutdown_to_all_clients()
    if not success:
        self.poutput("Failed to sendo quit command to all the clients")
        return False
    
    self.poutput("Sent quit command to all connected clients.")
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

    num_clients = args.num_clients

    logger.info(
        f"Connect command received: num_clients={num_clients}"
    )

    if num_clients <= 0:
        self.poutput("num_clients must be greater than 0")
        logger.warning(f"Invalid num_clients value: {num_clients}")
        return
    
    started = self.control_manager.start_connection(port=args.port)
    if not started:
        self.poutput("Failed to start control connection.")
        logger.error(f"Failed to start control connection on port {args.port}")
        return

    self.poutput(
        f"Waiting for {num_clients} client(s) to connect..."
    )

    
    self.control_manager.num_multi_clients = num_clients

    success = self.control_manager.handshake()

    if success:
        connected = self.control_manager.list_connected_clients()
        decoded_clients = [cid.decode(errors="ignore") for cid in connected]

        self.poutput("Handshake completed successfully.")
        self.poutput(f"Connected clients: {decoded_clients}")

        logger.info(
            f"Handshake completed successfully. Connected clients: {decoded_clients}"
        )
    else:
        self.poutput("Handshake failed.")
        logger.error("Handshake failed during connect command")
