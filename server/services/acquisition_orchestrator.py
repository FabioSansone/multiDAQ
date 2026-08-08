import threading

from server.utils.logger import get_logger
from server.services.client_command_service import CommandPlane
from server.core.server_state import ServerFSM, ServerFSMEvent


class AcquisitionOrchestrator:
    def __init__(
        self,
        acquisition_service,
        channel_selection_service,
        server_state,
        get_mode,
        output_func=None,
    ) -> None:
        self.acquisition_service = acquisition_service
        self.channel_selection_service = channel_selection_service
        self.get_mode = get_mode
        self.server_state = server_state
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("acquisition_orchestrator")
        self.logger.debug("Acquisition Orchestrator initialized")

    def start(self, args) -> None:
        self.poutput("Acquisition start command received.")

        current_state = self.server_state.get_server_state()
        if current_state != ServerFSM.READY:
            self.poutput(f"Cannot start acquisition: server is '{current_state.value}', expected READY.")
            return

        operational = set(self.server_state.get_operational_clients())
        client_ids = [
            cid for cid in self.acquisition_service.get_connected_clients(plane=CommandPlane.ACQUISITION)
            if cid in operational
        ]

        if not client_ids:
            self.poutput("No operational clients ready for acquisition.")
            return

        started = self.server_state.process_event(
            event=ServerFSMEvent.CONFIGURATION_STARTED,
            reason="Preparing RC/HV channels for acquisition",
            source="acquisition_orchestrator",
            metadata={"target_clients": client_ids},
        )

        if not started:
            self.poutput("Cannot start acquisition: invalid FSM transition (CONFIGURATION_STARTED).")
            self.logger.error("CONFIGURATION_STARTED rejected by FSM in acquisition start")
            return

        rc_ready_clients = []
        mode = self.get_mode()

        if mode == "test":
            self.poutput(
                "Server is in test mode: skipping HV power commands. "
                "RC channels will be enabled from HV/FEB presence if available."
            )

            for client_id in client_ids:
                client_name = client_id.decode(errors="ignore")
                channels = self.channel_selection_service.get_test_rc_channels(
                    client_id=client_id, plane=CommandPlane.ACQUISITION
                )

                rc_ok = self.acquisition_service.enable_rc_channels(client_id=client_id, channels=channels)

                if not rc_ok:
                    self.poutput(f"Client {client_name}: RC channel enable failed. Skipping acquisition start.")
                    continue

                rc_ready_clients.append(client_id)

        else:
            enabled_channels_by_client = (
                self.channel_selection_service.prepare_hv_channels_for_acquisition(plane=CommandPlane.ACQUISITION)
            )

            if not enabled_channels_by_client:
                self.poutput("No HV channels available for acquisition.")
                self.server_state.process_event(
                    event=ServerFSMEvent.CONFIGURATION_FAILED,
                    reason="No HV channels available for acquisition",
                    source="acquisition_orchestrator",
                    metadata={"failed_clients": client_ids},
                )
                return

            for client_id, channels in enabled_channels_by_client.items():
                client_name = client_id.decode(errors="ignore")

                if not channels:
                    self.poutput(f"Client {client_name}: no channels available for RC enable.")
                    continue

                rc_ok = self.acquisition_service.enable_rc_channels(client_id=client_id, channels=channels)

                if not rc_ok:
                    self.poutput(f"Client {client_name}: RC channel enable failed. Skipping acquisition start.")
                    continue

                rc_ready_clients.append(client_id)

        failed_clients = [cid for cid in client_ids if cid not in rc_ready_clients]

        if not rc_ready_clients:
            self.server_state.process_event(
                event=ServerFSMEvent.CONFIGURATION_FAILED,
                reason="No clients ready for acquisition after RC/HV preparation",
                source="acquisition_orchestrator",
                metadata={"failed_clients": failed_clients},
            )
            self.poutput("No clients ready for acquisition.")
            return

        self.server_state.process_event(
            event=ServerFSMEvent.CONFIGURATION_SUCCEEDED,
            reason="RC/HV channels prepared for acquisition",
            source="acquisition_orchestrator",
            metadata={"successful_clients": rc_ready_clients, "failed_clients": failed_clients},
        )

        resolved_batch_id = self.acquisition_service.resolve_batch_id(rc_ready_clients)

        if resolved_batch_id is None:
            self.poutput("Cannot start acquisition: missing batch_id and multipmt_id.")
            self.acquisition_service.disable_rc_channels(client_ids=rc_ready_clients)
            return

        started_acq = self.server_state.process_event(
            event=ServerFSMEvent.ACQUISITION_STARTED,
            reason="Acquisition started",
            source="acquisition_orchestrator",
            metadata={"active_clients": rc_ready_clients},
        )

        if not started_acq:
            self.poutput("FSM rejected acquisition start.")
            self.logger.error("ACQUISITION_STARTED rejected by FSM")
            self.acquisition_service.disable_rc_channels(client_ids=rc_ready_clients)
            return

        self.acquisition_service.begin_session()

        owner_thread = threading.Thread(
            target=self.acquisition_service.run_acquisition_session,
            kwargs={
                "client_ids": rc_ready_clients,
                "acq_type": args.acq_type,
                "file_format": args.file_format,
                "suffix": args.suffix,
                "run_id": args.run_id,
                "duration": args.duration,
            },
            daemon=True,
        )
        owner_thread.start()

        self.poutput(
            "Acquisition running in background. The server remains responsive; "
            "use 'acquisition stop' to stop it."
        )

    def stop(self) -> None:
    
        current_state = self.server_state.get_server_state()
        

        if current_state == ServerFSM.ACQUIRING:
            stop_requested = self.server_state.process_event(
                event=ServerFSMEvent.STOP_REQUESTED,
                reason="Manual stop command",
                source="acquisition_orchestrator",
            )
            if not stop_requested:
                self.poutput("Cannot register stop in FSM.")
                self.logger.error("STOP_REQUESTED rejected by FSM")
                return
            self.poutput("Stop requested; finalization is running in the background.")
            
            self.acquisition_service.request_stop()
        else:
            self.poutput(f"Nothing to stop: server is '{current_state.value}'.")


   

    