import argparse
import cmd2
from server.utils.logger import get_logger
from common.message_handler import Channel
from server.utils.json_parser import JsonParser
from server.core.server_state import command_guard, ServerFSM, ServerFSMEvent


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
@command_guard([ServerFSM.DISCONNECTED, ServerFSM.CONNECTED, ServerFSM.READY])
def do_change_mode(self, args):

    new_mode = args.mode.lower()

    logger.info(f"Requested acquisition mode change to '{new_mode}'")

    if new_mode not in POSSIBLE_MODES:
        self.poutput(f"Invalid acquisition mode: {new_mode}")
        logger.error(f"Invalid acquisition mode requested: {new_mode}")
        return

    client_ids = self.server_state.list_common_plane_clients()

    if not client_ids:
        if self.set_mode(new_mode):
            self.poutput(
                f"Server mode changed to {new_mode}. "
                "No clients connected; mode will be used at next handshake."
            )
            logger.info(
                f"Server mode changed to '{new_mode}' with no connected clients"
            )
        else:
            self.poutput(f"Failed to update server mode to {new_mode}")
            logger.error(f"Failed to update server mode to '{new_mode}'")
        return
    
    started = self.server_state.process_event(
        event = ServerFSMEvent.CONFIGURATION_STARTED,
        reason = f"Changing mode to '{new_mode}'",
        source = "do_change_mode",
        metadata = {"target_clients": client_ids},
    )
    
    if not started:
        self.poutput("Cannot start mode change: invalid FSM transition.")
        logger.error("CONFIGURATION_STARTED rejected by FSM")
        return

    successful_client_ids: list[bytes] = []
    failed_client_ids: list[bytes] = []
    
    
    for client_id in client_ids:
        client_name = client_id.decode(errors="ignore")
        
        identity = self.server_state.get_identity(client_id)

        if identity is None:
            failed_client_ids.append(client_id)
            self.poutput(f"Client {client_name}: missing identity")
            logger.error(f"No identity found for client {client_name}")
            continue

        multipmt_id = identity.get("multipmt_id")
        batch_id = identity.get("batch_id")

        if not multipmt_id or not batch_id:
            failed_client_ids.append(client_id)
            self.poutput(f"Client {client_name}: incomplete identity")
            logger.error(f"Incomplete identity for client {client_name}: {identity}")
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
                failed_client_ids.append(client_id)
                self.poutput(
                    f"Client {client_name}: cannot build multipmt config "
                    f"(multipmt_id={multipmt_id}, batch_id={batch_id})"
                )
                logger.error(
                    f"Cannot build multipmt configuration for client {client_name}, "
                    f"multipmt_id={multipmt_id}, batch_id={batch_id}"
                )
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

        reply, reason = self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=mode_sync_command.request_id,
            timeout_s=90.0,
        )

        if reply is None:
            failed_client_ids.append(client_id)
            self.poutput(f"Client {client_name}: no reply ({reason})")
            logger.error(f"Mode sync failed for client {client_name}: {reason}")
            continue

        payload = reply.payload or {}
        reply_status = payload.get("status")
        reply_mode = payload.get("acq_mode")
        error = payload.get("error")

        if reply_status != "ok" or error:
            failed_client_ids.append(client_id)
            self.poutput(
                f"Client {client_name}: mode sync failed "
                f"(mode={reply_mode}, error={error})"
            )
            logger.error(
                f"Client {client_name} failed mode sync: "
                f"status={reply_status}, mode={reply_mode}, error={error}"
            )
            continue

        successful_client_ids.append(client_id)
        self.poutput(f"Client {client_name}: mode synchronized to {reply_mode}")
        logger.info(f"Client {client_name} synchronized to mode '{reply_mode}'")

    self.poutput(
        f"Mode synchronization completed. "
        f"Successful clients: {len(successful_client_ids)}, "
        f"Failed clients: {len(failed_client_ids)}"
    )
    
    if not successful_client_ids:
        self.server_state.process_event(
            event=ServerFSMEvent.CONFIGURATION_FAILED,
            reason=f"Mode change to '{new_mode}' failed on all clients",
            source="do_change_mode",
            metadata={"failed_clients": failed_client_ids},
        )
        self.poutput(
            f"Mode change failed on all connected clients. "
            f"Server mode remains '{self.mode}'."
        )
        logger.error(f"Mode change to '{new_mode}' failed on all connected clients")
        return
    
    self.server_state.process_event(
        event=ServerFSMEvent.CONFIGURATION_SUCCEEDED,
        reason=f"Mode change to '{new_mode}' completed",
        source="do_change_mode",
        metadata={
            "successful_clients": successful_client_ids,
            "failed_clients": failed_client_ids,
        },
    )

    if failed_client_ids:
        self.poutput(f"Warning: {len(failed_client_ids)} client(s) are not synchronized.")
        logger.warning(
            f"{len(failed_client_ids)} client(s) are not synchronized with "
            f"server acquisition mode '{new_mode}'"
        )

    if self.set_mode(new_mode):
        self.poutput(f"Server mode changed to {new_mode}")
        logger.info(f"Server mode changed to '{new_mode}'")
        return

    self.poutput(f"Failed to update server mode to {new_mode}")
    logger.error(f"Failed to update server mode to '{new_mode}'")


@cmd2.with_category("Generic Commands")
@command_guard([ServerFSM.DISCONNECTED, ServerFSM.CONNECTED, ServerFSM.READY, ServerFSM.ACQUIRING, ServerFSM.ERROR])
def do_quit(self, _) -> bool:
    """Send quit command to all connected clients"""

    requested = self.server_state.process_event(
        event=ServerFSMEvent.DISCONNECT_REQUESTED,
        reason="Quit command issued",
        source="do_quit",
    )
    
    if not requested:
        self.poutput("Cannot shut down: invalid FSM transition.")
        logger.error("DISCONNECT_REQUESTED rejected by FSM")
        return False
    
    self.shutdown_service.power_off_hv_on_shutdown()
    self.shutdown_service.zero_rc_registers_on_shutdown()

    success = self.control_manager.notify_shutdown_to_all_clients()
    if not success:
        self.poutput("Failed to send quit command to all the clients")
        return False

    list_connected_clients = self.server_state.list_common_plane_clients()

    if list_connected_clients:
        self.poutput("Sent quit command to all connected clients.\n Quitting server.")
    else:
        self.poutput("Server shutting down ...")
    
    #ATTENZIONE: QUESTO PEZZO VA CAPITO PER GESTIRE BENE LA FASE DI FINALIZING PRIMA DI CHIUDER
    #IN QUESTO MOMENTO SE STO ACQUISENDO NON FERMO L'ACQUISIZIONE
    if self.server_state.get_server_state() == ServerFSM.FINALIZING:
        self.server_state.process_event(
            event=ServerFSMEvent.FINALIZATION_SUCCEEDED,
            reason="Shutdown finalization completed",
            source="do_quit",
        )
    
    self.server_state.reset_to_disconnected()

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

            connected_clients = self.control_manager.server_state.list_connected_clients()

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
        client_ids = self.control_manager.server_state.list_connected_clients()

        if not client_ids:
            self.poutput("No connected clients.")
            return

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

        return


#####################
#CONNECTION COMMANDS#
#####################

connect_parser = argparse.ArgumentParser()
connect_parser.add_argument("--num_clients", type=int, help="The number of clients expected to connect", default=1)
connect_parser.add_argument("--control_port", type=int, help="Selects the port to establish the connection", default=8888)
connect_parser.add_argument("--acq_port", type=int, help="Selects the port to establish the acquisition connection", default=8889)

@cmd2.with_argparser(connect_parser)
@cmd2.with_category("Connection Commands")
def do_connect(self, args: argparse.Namespace) -> None:
    """Wait for one or more clients to connect through the control and acquisition planes."""

    requested_clients = args.num_clients

    logger.info(f"Connect command received: num_clients={requested_clients}")

    if requested_clients <= 0:
        self.poutput("num_clients must be greater than 0")
        logger.warning(f"Invalid num_clients value: {requested_clients}")
        return

    control_listener_was_running = (
        self.control_manager.listener_thread is not None
        and self.control_manager.listener_thread.is_alive()
    )

    acq_listener_was_running = (
        self.acq_manager.acq_listener_thread is not None
        and self.acq_manager.acq_listener_thread.is_alive()
    )

    if control_listener_was_running:
        self.poutput("Stopping control listener temporarily to accept new handshakes...")
        self.control_manager.stop_listener()

    if acq_listener_was_running:
        self.poutput("Stopping acquisition listener temporarily to accept new handshakes...")
        self.acq_manager.stop_listener()

    if self.control_manager.socket is None:
        started = self.control_manager.start_connection(port=args.control_port)
        if not started:
            self.poutput("Failed to start control connection.")
            logger.error(
                f"Failed to start control connection on port {args.control_port}"
            )

            if control_listener_was_running:
                self.control_manager.start_listener()

            if acq_listener_was_running:
                self.acq_manager.start_listener()

            return

    already_connected = len(
        self.control_manager.server_state.list_connected_clients()
    )
    target_total = max(requested_clients, already_connected)

    self.control_manager.num_multi_clients = target_total
    self.control_manager.clear_queues()

    self.poutput(
        f"Trying to reach {target_total} connected client(s). "
        f"Currently connected: {already_connected}."
    )

    control_ready = self.control_manager.handshake()

    connected = self.control_manager.server_state.list_connected_clients()
    decoded_clients = [cid.decode(errors="ignore") for cid in connected]

    if not control_ready or not connected:
        self.poutput("No clients connected.")
        logger.warning("No clients connected after control-plane handshake.")

        if control_listener_was_running:
            self.control_manager.start_listener()

        if acq_listener_was_running:
            self.acq_manager.start_listener()

        return

    self.poutput(
        f"Control plane ready with {len(connected)}/{target_total} client(s)."
    )
    self.poutput(f"Connected clients: {decoded_clients}")

    logger.info(
        f"Control plane ready with {len(connected)}/{target_total} clients: "
        f"{decoded_clients}"
    )

    if self.acq_manager.socket is None:
        started = self.acq_manager.start_connection(port=args.acq_port)
        if not started:
            self.poutput("Failed to start acquisition connection.")
            logger.error(
                f"Failed to start acquisition connection on port {args.acq_port}"
            )

            if control_listener_was_running:
                self.control_manager.start_listener()

            if acq_listener_was_running:
                self.acq_manager.start_listener()

            return

    self.acq_manager.clear_queues()

    self.poutput("Starting acquisition-plane handshake...")

    acq_ready = self.acq_manager.handshake()

    if not acq_ready:
        self.poutput("Acquisition plane handshake failed.")
        logger.error("Acquisition plane handshake failed.")

        if control_listener_was_running:
            self.control_manager.start_listener()

        if acq_listener_was_running:
            self.acq_manager.start_listener()

        return

    self.poutput(
        f"Acquisition plane ready with "
        f"{len(self.acq_manager.acquisition_clients)}/{len(connected)} client(s)."
    )

    logger.info(
        f"Acquisition plane ready with "
        f"{len(self.acq_manager.acquisition_clients)}/{len(connected)} clients."
    )

    self.control_manager.clear_queues()
    self.acq_manager.clear_queues()

    if not self.control_manager.start_listener():
        logger.error("Failed to start control listener")
        self.poutput("Failed to start control listener")
        return

    if not self.acq_manager.start_listener():
        logger.error("Failed to start acquisition listener")
        self.poutput("Failed to start acquisition listener")
        return

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
            
