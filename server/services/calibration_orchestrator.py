import time
import threading
from server.services.client_command_service import CommandPlane
from server.core.server_state import ServerFSM, ServerFSMEvent
from server.utils.logger import get_logger


class CalibrationOrchestrator:

    def __init__(self, server_state, acquisition_service, channel_selection_service, command_service, get_mode, output_func=None) -> None:
        self.acquisition_service = acquisition_service
        self.channel_selection_service = channel_selection_service
        self.command_service = command_service
        self.poutput = output_func or (lambda message: None)
        self.get_mode = get_mode
        self.server_state = server_state
        self.logger = get_logger("calibration_orchestrator")
        self.logger.debug("Calibration Orchestrator initialized")

    ####################################
    #Time-To-Peak Calibration Procedure#
    ####################################

    def _parse_ttp_values(self, args) -> list[int]:
        if args.values is not None:
            values = []
            for item in args.values.split(","):
                item = item.strip()
                if not item:
                    continue
                values.append(int(item))
            return values

        start, stop, step = args.range
        if step == 0:
            raise ValueError("TTP range step cannot be zero")
        if step > 0:
            return list(range(start, stop + 1, step))
        return list(range(start, stop - 1, step))

    def _resolve_scan_channels(self, args, client_ids: list[bytes]) -> dict[bytes, list[int]]:
        """
        Derive RC channels for every client ONCE, before the scan starts.
        Each call triggers a full HV/Modbus scan (set_hv_sync, up to 90s per
        client) — doing this once per scan instead of once per point keeps
        the participating channel set stable and consistent across the
        whole scan.
        """
        channels_by_client: dict[bytes, list[int]] = {}

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            channels = self.channel_selection_service.get_test_rc_channels(
                client_id=client_id, requested_channels=args.channels, plane=CommandPlane.ACQUISITION
            )

            if not channels:
                self.poutput(f"Client {client_name}: no requested channels available for this scan.")
                continue

            rc_ok = self.acquisition_service.enable_rc_channels(client_id=client_id, channels=channels)

            if not rc_ok:
                self.poutput(f"Client {client_name}: RC channel enable failed.")
                continue

            channels_by_client[client_id] = channels

        return channels_by_client

    def _set_ttp_register(self, client_ids: list[bytes], ttp_value: int) -> list[bytes]:
        ready_clients = []
        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")
            ok = self.command_service.write_rc_register(
                client_id=client_id, address=10, value=ttp_value, plane=CommandPlane.ACQUISITION
            )
            if not ok:
                self.poutput(f"Client {client_name}: failed to write TTP register 10 = {ttp_value}")
                continue
            self.poutput(f"Client {client_name}: RC register 10 set to TTP value {ttp_value}")
            ready_clients.append(client_id)
        return ready_clients

    def scan_ttp(self, args) -> None:
        self.poutput("TTP scan command received.")

        mode = self.get_mode()
        if mode != "test":
            self.poutput(f"TTP scan currently implemented only in test mode. Current mode is '{mode}'.")
            return

        current_state = self.server_state.get_server_state()
        if current_state != ServerFSM.READY:
            self.poutput(f"Cannot start TTP scan: server is '{current_state.value}', expected READY.")
            return

        operational = set(self.server_state.get_operational_clients())
        client_ids = [
            cid for cid in self.acquisition_service.get_connected_clients(plane=CommandPlane.ACQUISITION)
            if cid in operational
        ]

        if not client_ids:
            self.poutput("No connected clients.")
            return


        try:
            ttp_values = self._parse_ttp_values(args)
        except Exception as e:
            self.poutput(f"Invalid TTP scan values: {e}")
            self.logger.error(f"Invalid TTP scan values: {e}")
            return

        if not ttp_values:
            self.poutput("No TTP values selected.")
            return

        # Derivazione canali UNA VOLTA SOLA per l'intero scan.
        channels_by_client = self._resolve_scan_channels(args, client_ids)
        rc_ready_clients = list(channels_by_client.keys())

        if not rc_ready_clients:
            self.poutput("No clients ready for TTP scan.")
            return

        self.poutput(f"Starting TTP scan in test mode: values={ttp_values}")

        resolved_batch_id = self.acquisition_service.resolve_batch_id(args, rc_ready_clients)
        if resolved_batch_id is None:
            self.poutput("Cannot start TTP scan: missing batch_id and multipmt_id.")
            self.acquisition_service.disable_rc_channels(client_ids=rc_ready_clients)
            return

        scan_run_folder = self.acquisition_service.get_acquisition_run_folder(
            resolved_batch_id=resolved_batch_id, args=args
        )

        started = self.server_state.process_event(
            event=ServerFSMEvent.ACQUISITION_STARTED,
            reason=f"TTP scan started, values={ttp_values}",
            source="calibration_orchestrator",
            metadata={"active_clients": rc_ready_clients},
        )

        if not started:
            self.poutput("Cannot start TTP scan: invalid FSM transition (ACQUISITION_STARTED).")
            self.logger.error("ACQUISITION_STARTED rejected by FSM at TTP scan start")
            self.acquisition_service.disable_rc_channels(client_ids=rc_ready_clients)
            return

        self.acquisition_service.begin_session()

        scan_thread = threading.Thread(
            target=self._run_ttp_scan_loop,
            args=(args, rc_ready_clients, ttp_values, scan_run_folder, resolved_batch_id),
            daemon=True,
        )
        scan_thread.start()

        self.poutput(
            "TTP scan running in background. The server remains responsive; "
            "use 'acquisition stop' to abort early."
        )

    def _run_ttp_scan_loop(self, args, client_ids, ttp_values, scan_run_folder, resolved_batch_id) -> None:
        overall_success = True

        for ttp_value in ttp_values:
            if self.server_state.get_server_state() != ServerFSM.ACQUIRING:
                self.poutput(f"Scan interrupted before TTP={ttp_value}: external stop.")
                break

            self.poutput(f"\nStarting TTP scan point: register 10 = {ttp_value}")
            ttp_ready_clients = self._set_ttp_register(client_ids=client_ids, ttp_value=ttp_value)
            if not ttp_ready_clients:
                self.poutput(f"No clients accepted TTP={ttp_value}. Skipping point.")
                continue

            receiver_info = self.acquisition_service.acquisition_receiver_start(
                duration=args.duration, suffix=f"ttp_{ttp_value}", acq_type=args.acq_type,
                run_id=args.run_id, batch_id=resolved_batch_id,
                force_compile=args.force_compile, run_folder=scan_run_folder,
            )

            if receiver_info is None:
                self.poutput(f"Failed to start data receiver for TTP={ttp_value}.")
                continue

            self.poutput(f"TTP={ttp_value}: data receiver started. PID={receiver_info['pid']}")

            while (
                self.acquisition_service.check_acquisition_running()
                and self.server_state.get_server_state() == ServerFSM.ACQUIRING
            ):
                time.sleep(0.5)

            point_success = self.acquisition_service.run_hardware_stop_and_flush(
                ttp_ready_clients, reason=f"TTP scan point completed, ttp={ttp_value}"
            )
            if not point_success:
                overall_success = False

        if self.server_state.get_server_state() == ServerFSM.ACQUIRING:
            self.server_state.process_event(
                event=ServerFSMEvent.STOP_REQUESTED,
                reason="TTP scan completed",
                source="calibration_orchestrator",
            )

        self.acquisition_service.close_session(success=overall_success, reason="TTP scan completed")
        self.poutput("TTP scan completed.")