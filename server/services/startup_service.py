from server.utils.logger import get_logger
from server.utils.json_parser import JsonParser
from server.core.server_state import ServerFSMEvent
from common.message_handler import Channel


class StartupService:
    """
    Applies the acquisition-mode hardware configuration to a set of clients,
    driving the server FSM through CONNECTED -> CONFIGURING -> READY
    (or back to CONNECTED if every client fails).

    A failure on one client never blocks the others: every client gets an
    explicit successful/failed outcome before the batch is considered closed.
    """

    def __init__(self, control_manager, server_state, output_func=None) -> None:
        self.control_manager = control_manager
        self.server_state = server_state
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("startup_service")
        self.logger.debug("Startup Service initialized")

    def _sync_one_client(self, client_id: bytes, mode: str) -> bool:
        client_name = client_id.decode(errors="ignore")

        identity = self.server_state.get_identity(client_id)

        if identity is None:
            self.poutput(f"Client {client_name}: missing identity")
            self.logger.error(f"No identity found for client {client_name}")
            return False

        multipmt_id = identity.get("multipmt_id")
        batch_id = identity.get("batch_id")

        if not multipmt_id or not batch_id:
            self.poutput(f"Client {client_name}: incomplete identity")
            self.logger.error(f"Incomplete identity for client {client_name}: {identity}")
            return False

        pe_thr = None
        acq_info = None

        if mode == "multipmt":
            pe_thr = 1

            config_file_service = JsonParser(multipmt_id=multipmt_id, batch_id=batch_id)
            acq_info = config_file_service.get_ch_configuration(pe_thr=pe_thr)

            if acq_info is None:
                self.poutput(
                    f"Client {client_name}: cannot build multipmt config "
                    f"(multipmt_id={multipmt_id}, batch_id={batch_id})"
                )
                self.logger.error(
                    f"Cannot build multipmt configuration for client {client_name}, "
                    f"multipmt_id={multipmt_id}, batch_id={batch_id}"
                )
                return False

        mode_sync_command = self.control_manager.message_handler.create_command(
            channel=Channel.ACQUISITION,
            command="set_acq_mode_sync",
            payload={
                "acq_mode": mode,
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
            self.poutput(f"Client {client_name}: no reply ({reason})")
            self.logger.error(f"Mode sync failed for client {client_name}: {reason}")
            return False

        payload = reply.payload or {}
        reply_status = payload.get("status")
        reply_mode = payload.get("acq_mode")
        error = payload.get("error")

        if reply_status != "ok" or error:
            self.poutput(
                f"Client {client_name}: mode sync failed "
                f"(mode={reply_mode}, error={error})"
            )
            self.logger.error(
                f"Client {client_name} failed mode sync: "
                f"status={reply_status}, mode={reply_mode}, error={error}"
            )
            return False

        self.poutput(f"Client {client_name}: mode synchronized to {reply_mode}")
        self.logger.info(f"Client {client_name} synchronized to mode '{reply_mode}'")
        return True

    def configure_clients(self, client_ids: list[bytes], mode: str) -> bool:
        if not client_ids:
            self.poutput("No clients to configure.")
            self.logger.warning("configure_clients called with no clients")
            return False

        started = self.server_state.process_event(
            event=ServerFSMEvent.CONFIGURATION_STARTED,
            reason=f"Startup configuration for mode '{mode}'",
            source="startup_service",
            metadata={"target_clients": client_ids},
        )

        if not started:
            self.poutput("Cannot start startup configuration: invalid FSM transition.")
            self.logger.error("CONFIGURATION_STARTED rejected by FSM at startup")
            return False

        successful_client_ids: list[bytes] = []
        failed_client_ids: list[bytes] = []

        for client_id in client_ids:
            if self._sync_one_client(client_id, mode):
                successful_client_ids.append(client_id)
            else:
                failed_client_ids.append(client_id)

        self.poutput(
            f"Startup configuration completed. "
            f"Successful clients: {len(successful_client_ids)}, "
            f"Failed clients: {len(failed_client_ids)}"
        )

        if not successful_client_ids:
            self.server_state.process_event(
                event=ServerFSMEvent.CONFIGURATION_FAILED,
                reason=f"Startup configuration for mode '{mode}' failed on all clients",
                source="startup_service",
                metadata={"failed_clients": failed_client_ids},
            )
            self.poutput("Startup configuration failed on all connected clients.")
            self.logger.error(f"Startup configuration for mode '{mode}' failed on all clients")
            return False

        self.server_state.process_event(
            event=ServerFSMEvent.CONFIGURATION_SUCCEEDED,
            reason=f"Startup configuration for mode '{mode}' completed",
            source="startup_service",
            metadata={
                "successful_clients": successful_client_ids,
                "failed_clients": failed_client_ids,
            },
        )

        if failed_client_ids:
            self.poutput(f"Warning: {len(failed_client_ids)} client(s) are not synchronized.")
            self.logger.warning(
                f"{len(failed_client_ids)} client(s) are not synchronized with "
                f"startup mode '{mode}'"
            )

        return True