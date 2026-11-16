import time
import threading
from server.services.acquisition_service import TriggerConfiguration
from server.services.client_command_service import CommandPlane
from server.services.calibration_service import CalibrationMode, CalibrationScanSpec, HardwareSnapshotSpec, ScanPoint
from server.core.server_state import ServerFSM, ServerFSMEvent
from server.utils.logger import get_logger


class CalibrationOrchestrator:

    def __init__(self, server_state, acquisition_service, channel_selection_service, command_service, calibration_service, calibration_engine, optical_service, get_mode, output_func=None) -> None:
        self.acquisition_service = acquisition_service
        self.channel_selection_service = channel_selection_service
        self.command_service = command_service
        self.calibration_service = calibration_service
        self.calibration_engine = calibration_engine
        self.optical_service = optical_service
        self.poutput = output_func or (lambda message: None)
        self.get_mode = get_mode
        self.server_state = server_state
        self.logger = get_logger("calibration_orchestrator")
        self.logger.debug("Calibration Orchestrator initialized")

    ####################
    #Common to the runs#
    ####################

    def _resolve_calibration_targets(self, *, multipmt_id: str | None = None, batch_id: str | None = None, all_clients: bool = False, channels: str | int | list[int] = "all") -> dict[bytes, list[int]]:
        """Clients and requested channels of a calibration; empty dictionary (and a message
        for the user) if there is nothing to calibrate."""

        resolution = self.channel_selection_service.resolve_calibration_targets(
            multipmt_id=multipmt_id,
            batch_id=batch_id,
            all_clients=all_clients,
            channels=channels,
            require_acquisition_plane=True,
        )

        if resolution.unresolved_target:
            self.logger.error(f"Calibration target resolution failed: {resolution.unresolved_target}")
            self.poutput(f"Calibration target resolution failed: {resolution.unresolved_target}")
            return {}

        for rejected_client, reason in resolution.rejected_clients.items():
            client_name = rejected_client.decode(errors="ignore")
            self.logger.warning(f"Calibration target rejected: client={client_name}, reason={reason}")
            self.poutput(f"Client {client_name} excluded from calibration: {reason}")

        if not resolution.targets:
            self.poutput("No eligible clients remain for calibration.")
            return {}

        return resolution.targets

    ################################
    #Pedestal Calibration Procedure#
    ################################

    def _fail_pedestal_client(self, client_id: bytes, error: str) -> None:
        """Excludes a client from the run; the others go on."""

        client_name = client_id.decode(errors="ignore")
        self.logger.error(f"Pedestal preparation failed for {client_name}: {error}")

        if not self.calibration_service.mark_client_failed(client_id=client_id, error=error):
            self.logger.error(f"Client {client_name} could not be marked as failed in the calibration run")

    def _prepare_pedestal_run(self) -> bool:
        """Runs once in the calibration worker thread: every active client turns its HV off
        and sets RC12=1. True if at least one client is ready (or if a stop was requested:
        the engine then ends the run without marking the clients as failed)."""

        clients_to_prepare = list(self.calibration_service.get_active_clients())
        self.logger.info(f"Pedestal preparation: {len(clients_to_prepare)} client(s) to prepare")

        for cid in clients_to_prepare:
            client_name = cid.decode(errors="ignore")

            if self.calibration_engine.stop_requested():
                self.logger.warning("Stop requested during the pedestal preparation: the remaining clients are not prepared")
                return True

            try:
                channels = self.calibration_service.get_effective_channels(client_id=cid)

                if not channels:
                    self._fail_pedestal_client(cid, "no effective channels")
                    continue

                self.logger.info(f"Pedestal preparation of {client_name}: channels={list(channels)}")

                reply, reason = self.command_service.send_calibration_command(
                    client_id=cid,
                    command="pedestal_calibration",
                    payload={"channels": list(channels)},
                )

                if reply is None:
                    self._fail_pedestal_client(cid, f"pedestal_calibration not completed: {reason}")
                    continue

                if reply.payload.get("status") != "ok":
                    error = reply.payload.get("error") or "pedestal_calibration rejected by the client"
                    self._fail_pedestal_client(cid, str(error))
                    continue

                self.logger.info(f"Pedestal preparation of {client_name} completed")

            except Exception as exc:
                self.logger.exception(f"Unexpected error in the pedestal preparation of {client_name}")
                self._fail_pedestal_client(cid, f"unexpected error: {exc}")

        ready_clients = self.calibration_service.get_active_clients()
        self.logger.info(f"Pedestal preparation finished: {len(ready_clients)} client(s) ready")
        return bool(ready_clients)

    def pedestal(self, args) -> bool:
        """calibration pedestal: one acquisition with the HV off and RC12=1, no trigger configuration."""

        self.poutput("Pedestal calibration requested.")

        all_clients = bool(getattr(args, "all_clients", False))
        multipmt_id = getattr(args, "multipmt_id", None)
        batch_id = getattr(args, "batch_id", None)

        # No selector means every eligible client.
        if multipmt_id is None and batch_id is None:
            all_clients = True

        if args.duration <= 0:
            self.poutput("Pedestal duration must be greater than zero.")
            return False

        targets = self._resolve_calibration_targets(
            multipmt_id=multipmt_id,
            batch_id=batch_id,
            all_clients=all_clients,
            channels=args.channels,
        )

        if not targets:
            return False

        # RC12 and RC19 are changed by the pedestal; the HV power state too, except in test mode
        # (HV monitor-only: it is not touched, and neither is it restored).
        snapshot_spec = HardwareSnapshotSpec(
            rc_register=(12, 19),
            hv_power_state=self.get_mode() != "test",
        )

        scan_spec = CalibrationScanSpec(
            calibration_type="pedestal",
            points=(ScanPoint(index=0, suffix=args.suffix),),
            snapshot_spec=snapshot_spec,
            prepare_run=self._prepare_pedestal_run,
        )

        started = self.calibration_engine.start_scan(
            calibration_spec=scan_spec,
            calibration_mode=CalibrationMode.SAFE,
            client_channels_targets=targets,
            trigger_configuration=None,
            duration=args.duration,
            acq_type=args.acq_type,
            run_id=args.run_id,
            file_format=args.file_format,
        )

        if not started:
            self.poutput("Pedestal calibration could not start. See log for details.")
            return False

        self.poutput(f"Pedestal calibration started on {len(targets)} client(s). Use stop_calibration to stop it.")
        return True

    ###########################
    #SPE Calibration Procedure#
    ###########################

    def _prepare_optics(self, *, near_wheel: int, far_wheel: int, polarizer: float) -> bool:
        """Fixed optics of a run (filter wheels and polarizer), shared by all the clients.
        Runs in the calibration worker thread; a stop request interrupts it between the movements."""

        if not self.optical_service.ensure_initialized():
            self.logger.error("Cannot prepare the optics: optical instruments unavailable")
            return False

        movements = (
            ("near wheel", lambda: self.optical_service.move_wheel(position=near_wheel, which_wheel="near")),
            ("far wheel", lambda: self.optical_service.move_wheel(position=far_wheel, which_wheel="far")),
            ("polarizer", lambda: self.optical_service.move_polarizer(position=polarizer)),
        )

        for name, move in movements:
            if self.calibration_engine.stop_requested():
                self.logger.warning("Stop requested during the optics preparation")
                return True  # not a failure: the engine ends the run as stopped, without failing the clients

            if not move():
                self.logger.error(f"Optics preparation failed: {name} did not reach its position")
                return False

        self.logger.info(f"Optics ready: near={near_wheel}, far={far_wheel}, polarizer={polarizer}")
        return True

    def _build_spe_run_id(self, args) -> str:
        if args.run_id is not None:
            return args.run_id

        return (
            f"spe_near{args.near_wheel}_far{args.far_wheel}_pol{args.polarizer:g}"
            f"_v{args.voltage}_thr{args.threshold:g}_dur{args.duration:g}"
            f"_delay{args.delay_ns}_win{args.window_ns}"
        )

    def _apply_spe_point(self, client_id: bytes, point: ScanPoint) -> bool:
        """HV of the SPE point on one client. Channels that cannot be configured or switched on
        are dropped from the effective channels (the RC19 mask is built afterwards); False only
        if no channel is ready."""

        client_name = client_id.decode(errors="ignore")
        effective_channels = self.calibration_service.get_effective_channels(client_id=client_id)

        if not effective_channels:
            self.logger.error(f"Cannot apply the SPE point to {client_name}: no effective channels")
            return False

        reply, reason = self.command_service.send_calibration_command(
            client_id=client_id,
            command="spe_calibration",
            payload={
                "channels": list(effective_channels),
                "spe_voltage": point.parameters["voltage"],
                "spe_threshold": point.parameters["threshold"],
            },
        )

        if reply is None:
            self.logger.error(f"SPE preparation failed for client {client_name}: {reason}")
            return False

        payload = reply.payload or {}

        if payload.get("status") != "ok":
            self.logger.error(f"SPE preparation rejected by client {client_name}: {payload.get('error')}")
            return False

        # Only channels that were requested can be ready.
        ready_channels = sorted(set(payload.get("successful_channels") or []) & set(effective_channels))

        if not ready_channels:
            self.logger.error(f"SPE preparation of client {client_name}: no channel is ready")
            return False

        excluded_channels = sorted(set(effective_channels) - set(ready_channels))

        if excluded_channels:
            self.logger.warning(f"SPE preparation of client {client_name}: channels {excluded_channels} excluded, ready: {ready_channels}")
            self.poutput(f"Client {client_name}: channels {excluded_channels} excluded from the SPE calibration.")

            if not self.calibration_service.set_effective_channels(client_id=client_id, channels=ready_channels):
                self.logger.error(f"Cannot update the effective channels of client {client_name}")
                return False

        return True

    def spe(self, args) -> bool:
        """calibration spe: one acquisition with fixed optics, common HV voltage and threshold
        and the external trigger. Calibration mode only."""

        self.poutput("SPE calibration requested.")

        if self.get_mode() != "calibration":
            self.poutput("SPE calibration is available only in CALIBRATION mode.")
            return False

        if args.duration <= 0:
            self.poutput("SPE duration must be greater than zero.")
            return False

        if args.window_ns <= 0 or args.window_ns % 5 != 0:
            self.poutput("SPE trigger window must be positive and a multiple of 5 ns.")
            return False

        if args.delay_ns < 0 or args.delay_ns % 5 != 0:
            self.poutput("SPE trigger delay must be non-negative and a multiple of 5 ns.")
            return False

        all_clients = bool(getattr(args, "all_clients", False))
        multipmt_id = getattr(args, "multipmt_id", None)
        batch_id = getattr(args, "batch_id", None)

        # No selector means every eligible client.
        if multipmt_id is None and batch_id is None:
            all_clients = True

        targets = self._resolve_calibration_targets(
            multipmt_id=multipmt_id,
            batch_id=batch_id,
            all_clients=all_clients,
            channels=args.channels,
        )

        if not targets:
            return False

        trigger_configuration = TriggerConfiguration(
            mode="external",
            input_type=args.trigger_input,
            polarity=args.polarity,
            window_ns=args.window_ns,
            delay_ns=args.delay_ns,
            save_external=True,
            save_auto=False,
        )

        point = ScanPoint(
            index=0,
            suffix=args.suffix,
            parameters={"voltage": args.voltage, "threshold": args.threshold},
            metadata={
                "near_wheel": args.near_wheel,
                "far_wheel": args.far_wheel,
                "polarizer": args.polarizer,
                "trigger_input": args.trigger_input,
                "trigger_polarity": args.polarity,
                "delay_ns": args.delay_ns,
                "window_ns": args.window_ns,
                "duration_s": args.duration,
            },
        )

        # The trigger registers (15, 16, 18), RC19, and the HV voltage, threshold and power state are changed.
        snapshot_spec = HardwareSnapshotSpec(
            rc_register=(15, 16, 18, 19),
            hv_voltage=True,
            hv_threshold=True,
            hv_power_state=True,
        )

        scan_spec = CalibrationScanSpec(
            calibration_type="spe",
            points=(point,),
            snapshot_spec=snapshot_spec,
            prepare_run=lambda: self._prepare_optics(
                near_wheel=args.near_wheel,
                far_wheel=args.far_wheel,
                polarizer=args.polarizer,
            ),
            apply_point=self._apply_spe_point,
        )

        started = self.calibration_engine.start_scan(
            calibration_spec=scan_spec,
            calibration_mode=CalibrationMode.SAFE,
            client_channels_targets=targets,
            trigger_configuration=trigger_configuration,
            duration=args.duration,
            acq_type=args.acq_type,
            run_id=self._build_spe_run_id(args),
            file_format=args.file_format,
        )

        if not started:
            self.poutput("SPE calibration could not start. See log for details.")
            return False

        self.poutput(f"SPE calibration started on {len(targets)} client(s). Use stop_calibration to stop it.")
        return True

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

    def _resolve_scan_channels(self, args, client_ids: list[bytes], trigger_config) -> dict[bytes, list[int]]:
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

            selection = self.channel_selection_service.get_test_rc_channels(
                client_id=client_id, requested_channels=args.channels, plane=CommandPlane.ACQUISITION
            )

            for note in selection.notes:
                self.poutput(note)

            channels = selection.channels

            if not channels:
                self.poutput(f"Client {client_name}: no requested channels available for this scan.")
                continue

            rc_ok = self.acquisition_service.configure_acquisition_client(
                client_id=client_id, trigger_config=trigger_config, effective_channels=channels,
            )

            if not rc_ok:
                self.poutput(f"Client {client_name}: RC trigger configuration failed.")
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

        args.trigger_mode = "external"
        args.save_auto = False

        trigger_config = self.acquisition_service.build_trigger_configuration(args=args)

        if trigger_config is None:
            self.poutput("Invalid trigger configuration. TTP scan not started.")
            return

        channels_by_client = self._resolve_scan_channels(args, client_ids, trigger_config)
        rc_ready_clients = list(channels_by_client.keys())

        if not rc_ready_clients:
            self.poutput("No clients ready for TTP scan.")
            return

        self.poutput(f"Starting TTP scan in test mode: values={ttp_values}")

        resolved_batch_id = self.acquisition_service.resolve_batch_id(rc_ready_clients)
        if resolved_batch_id is None:
            self.poutput("Cannot start TTP scan: missing batch_id and multipmt_id.")
            self.acquisition_service.disable_rc_channels(client_ids=rc_ready_clients)
            return

        client_run_folders = self.acquisition_service.get_client_run_folder(client_ids=rc_ready_clients, acq_type=args.acq_type, run_id=args.run_id)
        
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
            args=(args, channels_by_client, ttp_values, client_run_folders, resolved_batch_id, trigger_config),
            daemon=True,
        )
        scan_thread.start()

        self.poutput("TTP scan running in background. The server remains responsive; use 'acquisition stop' to abort early.")

        
    def _run_ttp_scan_loop(self, args, channels_by_client, ttp_values, client_run_folders, resolved_batch_id, trigger_config) -> None:
        base_client_ids = list(channels_by_client.keys())
        overall_success = True

        for index, ttp_value in enumerate(ttp_values):
            if self.server_state.get_server_state() != ServerFSM.ACQUIRING:
                self.poutput(f"Scan interrupted before TTP={ttp_value}: external stop.")
                break

            is_last_point = (index == len(ttp_values) - 1)

            self.poutput(f"\nStarting TTP scan point: register 10 = {ttp_value}")
            ttp_ready_clients = self._set_ttp_register(client_ids=base_client_ids, ttp_value=ttp_value)
            if not ttp_ready_clients:
                self.poutput(f"No clients accepted TTP={ttp_value}. Skipping point.")
                continue

            # RC19 viene abilitato dalla sessione, dopo apertura file e START producer.
            point_result = self.acquisition_service.run_acquisition_session(
                client_ids=ttp_ready_clients,
                acq_type=args.acq_type,
                file_format=args.file_format,
                acq_type_param=ttp_value,
                suffix=f"ttp_{ttp_value}",
                run_id=args.run_id,
                run_folder_clients=client_run_folders,
                duration=args.duration,
                reason=f"TTP scan point completed, ttp={ttp_value}",
                reset_trigger_config=is_last_point,
            )

            if not point_result.success:
                overall_success = False

        if self.server_state.get_server_state() == ServerFSM.ACQUIRING:
            self.server_state.process_event(
                event=ServerFSMEvent.STOP_REQUESTED,
                reason="TTP scan completed",
                source="calibration_orchestrator",
            )

        self.acquisition_service.close_session(success=overall_success, reason="TTP scan completed")
        self.poutput("TTP scan completed.")
        
    
    
    ##############################
    #CHECK PMT CALIBRATION STATUS#
    ##############################
    
    def recheck_calibration(self, args) -> None:
        self.poutput("Calibration recheck command received.")

        client_id = self.server_state.get_client_id_by_multipmt_id(args.multipmt_id)

        if client_id is None:
            self.poutput(f"No connected client found for multipmt_id={args.multipmt_id}")
            return

        client_name = client_id.decode(errors="ignore")

        ok = self.acquisition_service.recheck_calibration(client_id=client_id)

        if not ok:
            self.poutput(f"Client {client_name}: calibration recheck failed. See log for details.")
            return

        excluded = self.server_state.get_calibration_excluded_channels(client_id)

        if excluded:
            self.poutput(
                f"Client {client_name}: calibration rechecked. "
                f"Channels still excluded (no matching calibration): {excluded}"
            )
        else:
            self.poutput(f"Client {client_name}: calibration rechecked. All channels matched — nothing excluded.")