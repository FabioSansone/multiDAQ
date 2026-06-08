import argparse
import cmd2
from server.utils.logger import get_logger
from common.message_handler import Channel
from server.utils.json_parser import JsonParser


POSSIBLE_MODES = ['test', 'calibration', 'multipmt']
logger = get_logger('generic_commands')


##################
#GENERIC COMMANDS#
##################

mode_parser = argparse.ArgumentParser()
mode_parser.add_argument(
    "mode",
    action="store",
    type=str,
    help="Acquisition Mode (test, calibration, multipmt)",
    default="test",
)


@cmd2.with_argparser(mode_parser)
@cmd2.with_category("Generic Commands")
def do_change_mode(self, args):

    new_mode = args.mode.lower()

    logger.info(f"Requested acquisition mode change to '{new_mode}'")

    if new_mode not in POSSIBLE_MODES:
        self.poutput(f"Invalid acquisition mode: {new_mode}")
        logger.error(f"Invalid acquisition mode requested: {new_mode}")

    client_ids = self.control_manager.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")
        logger.warning(
            f"Cannot change mode to '{new_mode}': no connected clients"
        )

    successful_clients = 0
    failed_clients = 0

    for client_id in client_ids:
        client_name = client_id.decode(errors="ignore")

        identity = self.control_manager.identity_by_client_id.get(client_id)

        if identity is None:
            failed_clients += 1
            logger.error(f"No identity found for client {client_name}")
            self.poutput(f"Client {client_name}: missing identity")
            continue

        multipmt_id = identity.get("multipmt_id")
        batch_id = identity.get("batch_id")

        if not multipmt_id or not batch_id:
            failed_clients += 1
            logger.error(
                f"Incomplete identity for client {client_name}: {identity}"
            )
            self.poutput(f"Client {client_name}: incomplete identity")
            continue

        pe_thr = None
        acq_info = None

        if new_mode == "multipmt":
            pe_thr = 1

            logger.info(
                f"Building multipmt config for client {client_name} "
                f"(multipmt_id={multipmt_id}, batch_id={batch_id})"
            )

            config_file_service = JsonParser(
                multipmt_id=multipmt_id,
                batch_id=batch_id,
            )

            acq_info = config_file_service.get_ch_configuration(pe_thr=pe_thr)

            if acq_info is None:
                failed_clients += 1
                logger.error(
                    f"Cannot build multipmt configuration for client "
                    f"{client_name}, multipmt_id={multipmt_id}, "
                    f"batch_id={batch_id}"
                )
                self.poutput(f"Client {client_name}: cannot build multipmt config")
                continue

        mode_sync_command = self.control_manager.message_handler.create_command(
            channel=Channel.ACQUISITION,
            command="set_acq_mode_sync",
            payload={
                "acq_mode": new_mode,
                "pe_thr": pe_thr,
                "acquisition_configuration": acq_info,
            },
            sender="server",
        )

        self.control_manager.queue_message(client_id, mode_sync_command)

        logger.info(
            f"Queued mode sync command for client {client_name}: mode={new_mode}"
        )

        reply, reason = self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=mode_sync_command.request_id,
            timeout_s=90.0,
        )

        if reply is None:
            failed_clients += 1
            logger.error(
                f"Mode sync timeout/failure for client {client_name}: {reason}"
            )
            self.poutput(f"Client {client_name}: no reply ({reason})")
            continue

        payload = reply.payload or {}

        reply_status = payload.get("status")
        reply_mode = payload.get("acq_mode")
        error = payload.get("error")

        if reply_status != "ok" or error:
            failed_clients += 1
            logger.error(
                f"Client {client_name} failed mode sync: "
                f"status={reply_status}, mode={reply_mode}, error={error}"
            )
            self.poutput(
                f"Client {client_name}: mode sync failed "
                f"(mode={reply_mode}, error={error})"
            )
            continue

        successful_clients += 1

        logger.info(
            f"Client {client_name} synchronized to mode '{reply_mode}'"
        )

        self.poutput(
            f"Client {client_name}: mode synchronized to {reply_mode}"
        )

    self.poutput(
        f"Mode synchronization completed. "
        f"Successful clients: {successful_clients}, "
        f"Failed clients: {failed_clients}"
    )

    logger.info(
        f"Mode synchronization completed for mode '{new_mode}'. "
        f"Successful clients: {successful_clients}, "
        f"Failed clients: {failed_clients}"
    )

    if successful_clients == 0:
        logger.error(
            f"Mode change to '{new_mode}' failed on all clients. "
            f"Server mode remains '{self.mode}'"
        )
        self.poutput(
            f"Mode change failed on all clients. "
            f"Server mode remains '{self.mode}'."
        )

    if failed_clients > 0:
        logger.warning(
            f"{failed_clients} client(s) are not synchronized with "
            f"server acquisition mode '{new_mode}'"
        )
        self.poutput(
            f"Warning: {failed_clients} client(s) are not synchronized."
        )

    if self.set_mode(new_mode):
        self.poutput(f"Server mode changed to {new_mode}")
        logger.info(f"Server mode changed to '{new_mode}'")

    logger.error(f"Failed to update server mode to '{new_mode}'")
    self.poutput(f"Failed to update server mode to {new_mode}")


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

##################
#FORCE COMMANDS#
##################

########HELPERS###############
def _hv_to_user(channels):
    return [ch - 1 for ch in channels]


def _print_hv_lists(self, client_name: str, result: dict):
    self.poutput(f"\nClient {client_name}: HV sync completed.")
    self.poutput(f"  OK  channels: {_hv_to_user(result.get('ok_channels', []))}")
    self.poutput(f"  BAD channels: {_hv_to_user(result.get('bad_channels', []))}")
    self.poutput(f"  ON  channels: {_hv_to_user(result.get('on_channels', []))}")
    self.poutput(f"  OFF channels: {_hv_to_user(result.get('off_channels', []))}")
    
########HELPERS###############

force_parser = argparse.ArgumentParser()
force_subparsers = force_parser.add_subparsers(dest="command", required=True)

force_subparsers.add_parser(
    "quit",
    help="Force server shutdown regardless of client response",
)

force_subparsers.add_parser(
    "hv_sync",
    help="Force HV bad recovery and power-state synchronization",
)

@cmd2.with_argparser(force_parser)
@cmd2.with_category("Generic Commands")
def do_force(self, args: argparse.Namespace) -> bool:
    """
    Force operations that do not depend on client replies.
    """

    if args.command == "quit":

        try:
            self.control_manager.notify_shutdown_to_all_clients()

            connected_clients = self.control_manager.list_connected_clients()

            if connected_clients:
                self.poutput(
                    "Force quit command sent to all connected clients."
                )
            else:
                self.poutput(
                    "No connected clients. Forcing server shutdown."
                )

        except Exception as e:
            self.logger.warning(
                f"Force quit: failed to notify clients: {e}"
            )
            self.poutput(
                f"Warning: failed to notify clients: {e}"
            )

        self.poutput("Forcing server shutdown...")
        return True



    if args.command == "hv_sync":
        client_ids = self.control_manager.list_connected_clients()

        if not client_ids:
            self.poutput("No connected clients.")

        for client_id in client_ids:
            hv_sync_command = self.control_manager.message_handler.create_command(
                channel=Channel.HV,
                command="set_hv_sync",
                payload={"channels": "all"},
                sender="server",
            )

            self.control_manager.queue_message(client_id, hv_sync_command)

            reply, reason = self.control_manager.wait_for_reply(
                client_id=client_id,
                in_reply_to=hv_sync_command.request_id,
                timeout_s=90.0,
            )

            client_name = client_id.decode(errors="ignore")

            if reply is None:
                logger.error(
                    f"HV sync failed for client {client_name}: {reason}"
                )
                self.poutput(f"No reply from client {client_name}. Reason: {reason}")
                continue

            payload = reply.payload
            result = payload.get("result", {})
            error = payload.get("error")

            if error:
                logger.error(f"HV sync error from client {client_name}: {error}")
                self.poutput(f"Client {client_name}: HV sync error: {error}")

            _print_hv_lists(self, client_name, result)




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


#####################################
#HANDLING EVENT MESSAGES FROM CLIENT#
#####################################

########HELPERS###############

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

########HELPERS###############

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
            
