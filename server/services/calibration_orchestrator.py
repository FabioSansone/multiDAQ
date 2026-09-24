import threading
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
import time

from server.services.client_command_service import CommandPlane
from server.core.server_state import ServerFSM, ServerFSMEvent
from server.services.acquisition_service import TriggerConfiguration
from server.services.calibration_service import CalibrationService, ScanPoint, HardwareSnapshotSpec, ClientHardwareSnapshot, CalibrationMode, CalibrationRun, ClientCalibrationSession, CalibrationRunOutcome, CalibrationScanSpec
from server.utils.logger import get_logger




# ------------------------------------------------------------
# Trigger policy
# ------------------------------------------------------------
#
# CALIBRATION:
#   None -> standard calibration defaults are applied by
#           _prepare_calibration_client().
#
# TEST/MULTIPMT:
#   None -> preserve RC15/16/18.
#
# Explicit override:
#   build a complete external-trigger configuration.
#
        
        
        

class CalibrationOrchestrator:

    def __init__(self, server_state, acquisition_service, channel_selection_service, command_service, calibration_service: CalibrationService, get_mode, output_func=None) -> None:
        self.acquisition_service = acquisition_service
        self.channel_selection_service = channel_selection_service
        self.command_service = command_service
        self.calibration_service = calibration_service
        self.poutput = output_func or (lambda message: None)
        self.get_mode = get_mode
        self.server_state = server_state
        
        self._calibration_stop_requested = threading.Event()
        self._calibration_thread: threading.Thread | None = None

        self._calibration_lifecycle_lock = threading.Lock()
        
        self.logger = get_logger("calibration_orchestrator")
        self.logger.debug("Calibration Orchestrator initialized")


    @staticmethod
    def _scan_point_to_dict(point: ScanPoint) -> dict:
        return {
            "index": point.index,
            "parameter": point.parameter_name,
            "value": point.value,
            "suffix": point.suffix,
            "metadata": dict(point.metadata),
        }

    @staticmethod
    def _hardware_snapshot_to_dict(snapshot: ClientHardwareSnapshot) -> dict:
        return {
            "rc_registers": dict(snapshot.rc_registers),
            "hv_parameters": {channel: dict(parameters) for channel, parameters in snapshot.hv_parameters.items()},
            "extra": dict(snapshot.extra),
        }


    def _build_calibration_manifest(
        self,
        client_id: bytes,
        run: CalibrationRun,
        session: ClientCalibrationSession,
    ) -> dict:

        return {
            "version": 1,
            "calibration_type": run.calibration_type,
            "execution_mode": run.execution_mode.value,
            "start_time_utc": (
                run.start_time
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z")
            ),
            "end_time_utc": (
                datetime.now(timezone.utc)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z")
            ),
            "run_outcome": (
                run.outcome.value
                if run.outcome is not None
                else None
            ),
            
            "run_status": run.status.value,

            "client_id": client_id.decode(errors="ignore"),

            "requested_channels": list(
                session.requested_channels
            ),
            "effective_channels_final": list(
                session.effective_channels
            ),

            "requested_points": [
                self._scan_point_to_dict(point)
                for point in session.requested_points
            ],

            "completed_points": [
                self._scan_point_to_dict(point)
                for point in session.completed_points
            ],

            "client_outcome": (
                session.outcome.value
                if session.outcome is not None
                else None
            ),

            "error": session.error,
            "restore_succeeded": session.restore_succeeded,

            "hardware_snapshot": (
                self._hardware_snapshot_to_dict(
                    session.hardware_snapshot
                )
                if session.hardware_snapshot is not None
                else None
            ),
        }


    def _write_calibration_manifests(
        self,
        *,
        run_folder_clients: dict[bytes, Path],
    ) -> bool:

        run = self.calibration_service.get_run()

        if run is None:
            self.logger.error(
                "Cannot write calibration manifests: "
                "no calibration run is available"
            )
            return False

        sessions = self.calibration_service.get_sessions()

        if not sessions:
            self.logger.error(
                "Cannot write calibration manifests: "
                "no calibration sessions are available"
            )
            return False

        overall_success = True

        for client_id, session in sessions.items():

            client_name = client_id.decode(
                errors="ignore"
            )

            run_folder = run_folder_clients.get(
                client_id
            )

            if run_folder is None:
                self.logger.error(
                    "Cannot write calibration manifest: "
                    f"missing run folder for client "
                    f"{client_name}"
                )
                overall_success = False
                continue

            try:
                manifest = self._build_calibration_manifest(
                    client_id=client_id,
                    run=run,
                    session=session,
                )

                manifest_path = (
                    Path(run_folder)
                    / "calibration_manifest.json"
                )

                with manifest_path.open(
                    "w",
                    encoding="utf-8",
                ) as manifest_file:

                    json.dump(
                        manifest,
                        manifest_file,
                        indent=4,
                        ensure_ascii=False,
                    )

                self.logger.info(
                    "Calibration manifest written: "
                    f"client={client_name}, "
                    f"path={manifest_path}"
                )

            except Exception as exc:

                overall_success = False

                self.logger.exception(
                    "Failed to write calibration manifest "
                    f"for client {client_name}: {exc}"
                )

        return overall_success

    
    ##############################
    # HV CALIBRATION PROCEDURE   #
    ##############################

    def _run_hv_calibration(self, targets: dict[bytes, list[int]]) -> None:

        run_clients = list(targets.keys())

        started_clients = []
        results = {}

        try:
            for client_id in run_clients:
                reply, reason = (
                    self.command_service.send_hv_command(
                        client_id=client_id,
                        command="hv_calibration_start",
                        payload={
                            "channels": targets[client_id],
                        },
                        plane=CommandPlane.CONTROL,
                        timeout_s=35.0,
                    )
                )

                if reply is None:

                    self.calibration_service.mark_client_failed(
                        client_id=client_id,
                        error=(
                            "HV calibration start failed: "
                            f"{reason}"
                        ),
                    )

                    continue

                payload = reply.payload or {}

                if payload.get("status") != "ok":

                    self.calibration_service.mark_client_failed(
                        client_id=client_id,
                        error=(
                            payload.get("error")
                            or "HV calibration start rejected"
                        ),
                    )

                    continue

                started_clients.append(client_id)


            running_clients = set(started_clients)
            stop_sent = False

            while running_clients:
                if self._calibration_stop_requested.is_set() and not stop_sent:
                    for client_id in running_clients:  
                        self.command_service.send_hv_command(
                            client_id=client_id,
                            command="hv_calibration_stop",
                            payload={},
                            plane=CommandPlane.CONTROL,
                            timeout_s=10.0,
                        )  

                    stop_sent = True

                for client_id in list(running_clients):
                    reply, reason = (
                        self.command_service.send_hv_command(
                            client_id=client_id,
                            command="hv_calibration_status",
                            payload={},
                            plane=CommandPlane.CONTROL,
                            timeout_s=10.0,
                        )
                    )

                    if reply is None:
                        continue

                    result = reply.payload.get("result", {})

                    if result.get("running", False):
                        continue

                    if not result.get("finished", False):
                        continue

                    results[client_id] = result.get("calibration_result",{})

                    running_clients.remove(
                        client_id
                    )

                if running_clients:
                    time.sleep(1)


            for client_id in started_clients:
                result = result.get(client_id, {})

                if self._calibration_stop_requested.is_set():
                    self.calibration_service.mark_client_aborted(client_id=client_id)

                elif result.get("success", False):
                    self.calibration_service.mark_client_completed(client_id=client_id)

                else:
                    self.calibration_service.mark_client_failed(client_id=client_id, error="HV calibration failed")    

        finally:
            with self._calibration_lifecycle_lock:
                state = self.server_state.get_server_state()

                if state == ServerFSM.CALIBRATING:
                    if self._calibration_stop_requested.is_set():
                        event = ServerFSMEvent.CALIBRATION_STARTED

                    elif self.calibration_service.get_completed_clients():
                        event = ServerFSMEvent.CALIBRATION_COMPLETED

                    else:
                        event = ServerFSMEvent.CALIBRATION_FAILED

                    self.server_state.process_event(
                        event=event,
                        reason="HV calibration finished",
                        source="calibration_orchestrator",
                    )

                self.calibration_service.begin_finalization()

                for client_id in run_clients:
                    result = results.get(client_id)
                    if results is None:
                        restore_ok = False
                    else:
                        restore_ok = not bool(result.get("failed_restore_channels", []))

                    self.calibration_service.mark_restore_result(
                        client_id=client_id,
                        succeeded=restore_ok,
                    )

                outcome = self.calibration_service.finish_run()
                restored_clients = self.calibration_service.get_restored_clients()
                failed_restore_clients = self.calibration_service.get_failed_restore_clients()

                restore_success = (
                    outcome is not None
                    and not failed_restore_clients
                )

                final_event = (
                    ServerFSMEvent
                    .CALIBRATION_FINALIZATION_SUCCEEDED
                    if restore_success
                    else
                    ServerFSMEvent
                    .CALIBRATION_FINALIZATION_FAILED
                )

                self.server_state.process_event(
                    event=final_event,
                    reason="HV calibration finalization completed",
                    source="calibration_orchestrator",
                    metadata={
                        "restored_clients": restored_clients,
                        "failed_restore_clients": (
                            failed_restore_clients
                        ),
                    },
                )

                self.calibration_service.clear_run()

                self._calibration_stop_requested.clear()


    def calibrate_hv(self, args) -> bool:
        self.poutput("HV calibration requested.")

        if self.server_state.get_server_state() != ServerFSM.READY:
            self.poutput("Cannot start HV calibration: server is not READY.")
            return False

        if self._calibration_thread is not None and self._calibration_thread.is_alive():
            self.poutput("Another calibration worker is already running.")
            return False

        all_clients = bool(getattr(args, "all_clients", False))
        multipmt_id = getattr(args, "multipmt_id", None)
        batch_id = getattr(args, "batch_id", None)

        if multipmt_id is None and batch_id is None and not all_clients:
            all_clients = True

        targets = self._resolve_calibration_targets(
            multipmt_id=multipmt_id,
            batch_id=batch_id,
            all_clients=all_clients,
            channels=args.channels,
            require_acquisition_plane=False,
        )

        if not targets:
            return False

        requested_points = {client_id: [] for client_id in targets}
        run = self.calibration_service.start_run(
            calibration_type="hv",
            execution_mode=CalibrationMode.SAFE,
            targets=targets,
            requested_points=requested_points
        )

        if run is None:
            return False

        target_clients = self.calibration_service.get_active_clients()

        started = self.server_state.process_event(
            event=ServerFSMEvent.CALIBRATION_STARTED,
            reason="HV calibration started",
            source="calibration_orchestrator",
            metadata={
                "target_clients": target_clients,
            },
        )

        if not started:
            self.calibration_service.discard_unmodified_run()
            return False

        self._calibration_stop_requested.clear()

        self._calibration_thread = threading.Thread(
            target=self._run_hv_calibration,
            kwargs={
                "targets": targets,
            },
            daemon=True,
            name="calibration-hv",
        )

        try:
            self._calibration_thread.start()
        except Exception:
            self.logger.exception(
                "Failed to start HV calibration worker"
            )
            return False

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

            channels = self.channel_selection_service.get_test_rc_channels(
                client_id=client_id, requested_channels=args.channels, plane=CommandPlane.ACQUISITION
            )

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



    def scan_ttp(self, args) -> bool:

        self.poutput("TTP calibration scan requested.")

        try:
            ttp_values = self._parse_ttp_values(args)
        except (TypeError, ValueError) as exc:

            self.poutput(
                f"Invalid TTP scan values: {exc}"
            )

            return False

        if not ttp_values:

            self.poutput(
                "No TTP scan values were provided."
            )

            return False

        
        all_clients = bool(
            getattr(args, "all_clients", False)
        )

        multipmt_id = getattr(
            args,
            "multipmt_id",
            None,
        )

        batch_id = getattr(
            args,
            "batch_id",
            None,
        )

        if (
            multipmt_id is None
            and batch_id is None
            and not all_clients
        ):
            all_clients = True

        targets = self._resolve_calibration_targets(
            multipmt_id=multipmt_id,
            batch_id=batch_id,
            all_clients=all_clients,
            channels=args.channels,
            require_acquisition_plane=True,
        )

        if not targets:
            return False

        points = [
            ScanPoint(
                index=index,
                parameter_name="ttp",
                value=value,
                suffix=f"{args.suffix}_{value}",
            )
            for index, value in enumerate(ttp_values)
        ]

        try:
            execution_mode = CalibrationMode(
                args.execution_mode
            )
        except ValueError:

            self.poutput(
                f"Invalid calibration execution mode: "
                f"{args.execution_mode!r}"
            )

            return False

        
        mode = self.get_mode()

        trigger_values = (
            args.trigger_input,
            args.polarity,
            args.window_ns,
            args.delay_ns,
        )

        has_trigger_override = any(
            value is not None
            for value in trigger_values
        )

        trigger_config = None

        if has_trigger_override:

            if mode == "calibration":

                trigger_config = TriggerConfiguration(
                    mode="external",
                    input_type=(
                        args.trigger_input
                        or "single-ended"
                    ),
                    polarity=(
                        args.polarity
                        or "default"
                    ),
                    window_ns=(
                        args.window_ns
                        if args.window_ns is not None
                        else 400
                    ),
                    delay_ns=(
                        args.delay_ns
                        if args.delay_ns is not None
                        else 800
                    ),
                    save_external=True,
                    save_auto=False,
                )

            else:

                #
                # In TEST/MULTIPMT, partial override would accidentally
                # overwrite settings that we explicitly decided to preserve.
                #
                if any(
                    value is None
                    for value in trigger_values
                ):

                    self.poutput(
                        "In test/multipmt mode, trigger override "
                        "must specify all of: --input, --polarity, "
                        "--window-ns and --delay-ns. "
                        "Omit all of them to preserve the current "
                        "hardware trigger configuration."
                    )

                    return False

                trigger_config = TriggerConfiguration(
                    mode="external",
                    input_type=args.trigger_input,
                    polarity=args.polarity,
                    window_ns=args.window_ns,
                    delay_ns=args.delay_ns,
                    save_external=True,
                    save_auto=False,
                )

        snapshot_spec = HardwareSnapshotSpec(
            rc_register=(
                10,
                15,
                16,
                18,
                19,
            )
        )

        scan_spec = CalibrationScanSpec(
            calibration_type="ttp",
            points=tuple(points),
            snapshot_spec=snapshot_spec,
            apply_point=self._apply_ttp_point,
        )

        return self.start_calibration_scan(
            scan_spec=scan_spec,
            execution_mode=execution_mode,
            targets=targets,
            trigger_config=trigger_config,
            acq_type=args.acq_type,
            file_format=args.file_format,
            duration=args.duration,
            run_id=args.run_id,
        )
        
    # def _run_ttp_scan_loop(self, args, channels_by_client, ttp_values, client_run_folders, resolved_batch_id, trigger_config) -> None:
    #     base_client_ids = list(channels_by_client.keys())
    #     overall_success = True

    #     for index, ttp_value in enumerate(ttp_values):
    #         if self.server_state.get_server_state() != ServerFSM.ACQUIRING:
    #             self.poutput(f"Scan interrupted before TTP={ttp_value}: external stop.")
    #             break

    #         is_last_point = (index == len(ttp_values) - 1)

    #         self.poutput(f"\nStarting TTP scan point: register 10 = {ttp_value}")
    #         ttp_ready_clients = self._set_ttp_register(client_ids=base_client_ids, ttp_value=ttp_value)
    #         if not ttp_ready_clients:
    #             self.poutput(f"No clients accepted TTP={ttp_value}. Skipping point.")
    #             continue

    #         rc_ready_clients = []
    #         for client_id in ttp_ready_clients:
    #             client_name = client_id.decode(errors="ignore")

    #             rc_ok = self.acquisition_service.enable_rc_channels(
    #                 client_id=client_id, channels=channels_by_client[client_id],
    #             )

    #             if not rc_ok:
    #                 self.poutput(f"Client {client_name}: RC channel re-enable failed for TTP={ttp_value}")
    #                 continue
    #             rc_ready_clients.append(client_id)

    #         if not rc_ready_clients:
    #             self.poutput(f"No clients ready (RC re-enable failed) for TTP={ttp_value}. Skipping point.")
    #             continue

    #         point_success = self.acquisition_service.run_acquisition_session(
    #             client_ids=rc_ready_clients,
    #             acq_type=args.acq_type,
    #             file_format=args.file_format,
    #             acq_type_param=ttp_value,
    #             suffix=f"ttp_{ttp_value}",
    #             run_id=args.run_id,
    #             run_folder_clients=client_run_folders,
    #             duration=args.duration,
    #             reason=f"TTP scan point completed, ttp={ttp_value}",
    #             manage_session=False,
    #             reset_trigger_config=is_last_point,   
    #         )

    #         if not point_success:
    #             overall_success = False

    #     if self.server_state.get_server_state() == ServerFSM.ACQUIRING:
    #         self.server_state.process_event(
    #             event=ServerFSMEvent.STOP_REQUESTED,
    #             reason="TTP scan completed",
    #             source="calibration_orchestrator",
    #         )

    #     self.acquisition_service.close_session(success=overall_success, reason="TTP scan completed")
    #     self.poutput("TTP scan completed.")
        
    
    
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


    def _resolve_calibration_targets(
        self,
        *,
        client_id: bytes | None = None,
        multipmt_id: str | None = None,
        batch_id: str | None = None,
        all_clients: bool = False,
        channels: str | int | list[int] = "all",
        require_acquisition_plane: bool = True,
    ) -> dict[bytes, list[int]]:

        resolution = (
            self.channel_selection_service
            .resolve_calibration_targets(
                client_id=client_id,
                multipmt_id=multipmt_id,
                batch_id=batch_id,
                all_clients=all_clients,
                channels=channels,
                require_acquisition_plane=(
                    require_acquisition_plane
                ),
            )
        )

        if resolution.unresolved_target:
            self.logger.error(
                "Calibration target resolution failed: "
                f"{resolution.unresolved_target}"
            )

            self.poutput(
                "Calibration target resolution failed: "
                f"{resolution.unresolved_target}"
            )

            return {}

        for rejected_client, reason in (
            resolution.rejected_clients.items()
        ):

            client_name = rejected_client.decode(
                errors="ignore"
            )

            self.logger.warning(
                f"Calibration target rejected: "
                f"client={client_name}, reason={reason}"
            )

            self.poutput(
                f"Client {client_name} excluded from calibration: "
                f"{reason}"
            )

        if not resolution.targets:

            self.poutput(
                "No eligible clients remain for calibration."
            )

            return {}

        return resolution.targets


    def _prepare_calibration_client(self, client_id: bytes, trigger_config=None, ) -> bool:
        client_name = client_id.decode(errors="ignore")

        session = self.calibration_service.get_session(client_id=client_id)
        if session is None:
            self.logger.error(
                f"Cannot prepare calibration client {client_name}: "
                "no calibration session"
            )
            return False

        requested_channels = list(session.requested_channels)
        if not requested_channels:
            self.logger.error(
                f"Cannot prepare calibration client {client_name}: "
                "no requested channels"
            )
            return False

        mode = self.get_mode()


        if mode in {"test", "calibration", "multipmt"}:
            effective_channels = self.channel_selection_service.get_test_rc_channels(
                client_id=client_id,
                requested_channels=requested_channels,
                plane=CommandPlane.ACQUISITION,
            )

        else:
            self.logger.error(
                f"Cannot prepare calibration client {client_name}: "
                f"unsupported mode {mode!r}"
            )
            return False

        if not effective_channels:
            self.logger.warning(
                f"Cannot prepare calibration client {client_name}: "
                "no effective channels available"
            )

            return False

        effective_trigger_config = trigger_config

        if mode == "calibration" and effective_trigger_config is None:
            effective_trigger_config = TriggerConfiguration(
                mode="external",
                input_type="single-ended",
                polarity="default",
                window_ns=400,
                delay_ns=800,
                save_external=True,
                save_auto=False,
            )

        if effective_trigger_config is not None:

            if effective_trigger_config.mode != "external":
                self.logger.error(
                    f"Calibration preparation for client {client_name}: "
                    "only external trigger is currently supported"
                )

                return False

            if effective_trigger_config.input_type not in {
                "differential",
                "single-ended",
            }:

                self.logger.error(
                    f"Invalid trigger input type for client "
                    f"{client_name}"
                )

                return False

            if effective_trigger_config.polarity not in {
                "default",
                "inverted",
            }:

                self.logger.error(
                    f"Invalid trigger polarity for client "
                    f"{client_name}"
                )

                return False

            if (
                effective_trigger_config.window_ns <= 0
                or effective_trigger_config.window_ns % 5 != 0
            ):

                self.logger.error(
                    f"Invalid trigger window for client "
                    f"{client_name}: "
                    f"{effective_trigger_config.window_ns} ns"
                )

                return False

            if (
                effective_trigger_config.delay_ns < 0
                or effective_trigger_config.delay_ns % 5 != 0
            ):

                self.logger.error(
                    f"Invalid trigger delay for client "
                    f"{client_name}: "
                    f"{effective_trigger_config.delay_ns} ns"
                )

                return False

            current_reg15 = self.command_service.read_rc_register(
                client_id=client_id,
                address=15,
                plane=CommandPlane.ACQUISITION,
            )

            if current_reg15 is None:
                self.logger.error(
                    f"Cannot prepare calibration client {client_name}: "
                    "failed to read RC register 15"
                )

                return False

            trigger_mask = (
                (1 << 1)
                | (1 << 4)
                | (1 << 7)
                | (1 << 8)
            )

            trigger_bits = 0

            #
            # External trigger enabled.
            #
            trigger_bits |= 1 << 1

            if (
                effective_trigger_config.input_type
                == "single-ended"
            ):
                trigger_bits |= 1 << 7

            if (
                effective_trigger_config.polarity
                == "inverted"
            ):
                trigger_bits |= 1 << 8

            new_reg15 = (
                (current_reg15 & ~trigger_mask)
                | trigger_bits
            )

            reg16_value = (
                effective_trigger_config.window_ns // 5
            )

            reg18_value = (
                effective_trigger_config.delay_ns // 5
            )

            if not self.command_service.write_rc_register(
                client_id=client_id,
                address=15,
                value=new_reg15,
                plane=CommandPlane.ACQUISITION,
            ):

                self.logger.error(
                    f"Failed to configure RC15 for "
                    f"calibration client {client_name}"
                )

                return False

            if not self.command_service.write_rc_register(
                client_id=client_id,
                address=16,
                value=reg16_value,
                plane=CommandPlane.ACQUISITION,
            ):

                self.logger.error(
                    f"Failed to configure RC16 for "
                    f"calibration client {client_name}"
                )

                return False

            if not self.command_service.write_rc_register(
                client_id=client_id,
                address=18,
                value=reg18_value,
                plane=CommandPlane.ACQUISITION,
            ):

                self.logger.error(
                    f"Failed to configure RC18 for "
                    f"calibration client {client_name}"
                )

                return False   

        #
        # 4. Build RC19 from effective channels.
        #
        channel_mask = 0

        for channel in effective_channels:
            channel_mask |= 1 << channel

        
        if (
            mode in {"test", "multipmt"}
            and trigger_config is None
        ):

            current_reg19 = (
                self.command_service.read_rc_register(
                    client_id=client_id,
                    address=19,
                    plane=CommandPlane.ACQUISITION,
                )
            )

            if current_reg19 is None:

                self.logger.error(
                    f"Cannot prepare calibration client {client_name}: "
                    "failed to read RC register 19"
                )

                return False

            non_channel_bits = current_reg19 & ~0x7F

            reg19_value = (
                non_channel_bits | channel_mask
            )

        else:

            reg19_value = channel_mask

            if (
                effective_trigger_config is not None
                and effective_trigger_config.save_external
            ):
                reg19_value |= 1 << 7

            if (
                effective_trigger_config is not None
                and effective_trigger_config.save_auto
            ):
                reg19_value |= 1 << 8

        if not self.command_service.write_rc_register(
            client_id=client_id,
            address=19,
            value=reg19_value,
            plane=CommandPlane.ACQUISITION,
        ):

            self.logger.error(
                f"Failed to configure RC19 for "
                f"calibration client {client_name}"
            )

            return False  


        if not self.calibration_service.set_effective_channels(
            client_id=client_id,
            channels=effective_channels,
        ):

            self.logger.error(
                f"Failed to record effective channels "
                f"for calibration client {client_name}"
            )

            return False

        self.logger.info(
            f"Calibration client prepared: "
            f"client={client_name}, "
            f"mode={mode}, "
            f"requested_channels={requested_channels}, "
            f"effective_channels={effective_channels}"
        )

        return True  

    def _prepare_active_calibration_clients(self, trigger_config: TriggerConfiguration | None = None) -> list[bytes]:

        active_clients = self.calibration_service.get_active_clients()

        ready_clients: list[bytes] = []

        for client_id in active_clients:
            client_name = client_id.decode(errors="ignore")
            ok = self._prepare_calibration_client(client_id=client_id, trigger_config=trigger_config)

            if not ok:
                self.calibration_service.mark_client_failed(client_id=client_id, error="calibration hardware preparation failed")
                self.logger.warning(
                    f"Calibration preparation failed for "
                    f"client {client_name}; "
                    "client removed from subsequent scan points"
                )

                continue

            ready_clients.append(client_id)


        return ready_clients


    def _apply_ttp_point(self, client_id: bytes, point: ScanPoint) -> bool:
        client_name = client_id.decode(errors="ignore")

        try:
            value = int(point.value)
        except (TypeError, ValueError):
            self.logger.error(
                f"Invalid TTP value for client {client_name}: "
                f"{point.value!r}"
            )
            return False

        ok = self.command_service.write_rc_register(
            client_id=client_id,
            address=10,
            value=value,
            plane=CommandPlane.ACQUISITION,
        )

        if not ok:
            self.logger.error(
                f"Failed to apply TTP scan point "
                f"for client {client_name}: "
                f"RC10={value}"
            )
            return False

        self.logger.debug(
            f"Calibration scan point applied: "
            f"client={client_name}, "
            f"parameter=ttp, "
            f"value={value}"
        )

        return True

        

    #FUNZIONE DA ESTENDERE NEL CASO DI IMPLEMENTAZIONI DI CALIBRAZIONI FUTURE
    # def _apply_scan_point(
    #     self,
    #     client_id: bytes,
    #     point: ScanPoint,
    # ) -> bool:

    #     client_name = client_id.decode(errors="ignore")

    #     if point.parameter_name == "ttp":

    #         try:
    #             value = int(point.value)
    #         except (TypeError, ValueError):
    #             self.logger.error(
    #                 f"Invalid TTP value for client {client_name}: "
    #                 f"{point.value!r}"
    #             )
    #             return False

    #         ok = self.command_service.write_rc_register(
    #             client_id=client_id,
    #             address=10,
    #             value=value,
    #             plane=CommandPlane.ACQUISITION,
    #         )

    #         if not ok:
    #             self.logger.error(
    #                 f"Failed to apply TTP scan point "
    #                 f"for client {client_name}: "
    #                 f"RC10={value}"
    #             )
    #             return False

    #         self.logger.debug(
    #             f"Calibration scan point applied: "
    #             f"client={client_name}, "
    #             f"parameter=ttp, "
    #             f"value={value}"
    #         )

    #         return True

    #     self.logger.error(
    #         f"Unsupported calibration scan parameter "
    #         f"{point.parameter_name!r} "
    #         f"for client {client_name}"
    #     )

    #     return False


    def _handle_start_error_calibration(self, target_clients: list[bytes], reason: str) -> bool:

        roolback_completed = False
        session_closed = False

        try:
            calib_failed = self.server_state.process_event(
                event=ServerFSMEvent.CALIBRATION_FAILED,
                reason=reason,
                source="calibration_orchestrator",
                metadata={
                    "target_clients": target_clients,
                },
            )

            if not calib_failed:
                self.logger.error(
                    "CALIBRATION_FAILED transition rejected"
                )

                return False

            for client_id in target_clients:
                if not self.calibration_service.mark_client_failed(client_id=client_id, error=reason):
                    self.logger.error(f"Cannot mark client {client_id!r} failed")
                    return False

            if not self.calibration_service.begin_finalization():
                self.logger.error("Cannot begin calibration finalization")
                return False

            for client_id, session in self.calibration_service.get_sessions().items():
                if session.hardware_snapshot is not None:
                    self.logger.error(f"Unexpected hardware snapshot during startup rollback: client={client_id!r}")
                    return False

                if not self.calibration_service.mark_restore_not_required(client_id):
                    self.logger.error(f"Cannot finalize client {client_id!r}")
                    return False


            outcome = self.calibration_service.finish_run()
            if outcome != CalibrationRunOutcome.FAILED:
                self.logger.error(f"Unexpected startup rollback outcome: {outcome}")
                return False

            try:
                self.acquisition_service.close_session(success=False, reason=reason)
                session_closed = True
            except Exception:
                self.logger.exception("It was not possible to close the acquisition session")
                return False

            restored_clients = self.calibration_service.get_restored_clients()
            if not self.server_state.process_event(
                event=(
                    ServerFSMEvent
                    .CALIBRATION_FINALIZATION_SUCCEEDED
                ),
                reason=(
                    "Calibration startup failed; "
                    "cleanup completed successfully"
                ),
                source="calibration_orchestrator",
                metadata={
                    "restored_clients": restored_clients,
                    "failed_restore_clients": [],
                },
            ):


                self.logger.error("CALIBRATION_FINALIZATION_SUCCEEDED rejected")
                return False



            if not self.calibration_service.clear_run():
                self.logger.error("Cannot clear completed startup rollback run")
                return False

            self.logger.info(
                f"Calibration startup rollback completed: {reason}"
            )

            roolback_completed = True

            return True

        except Exception:
            self.logger.exception(
                "Calibration startup rollback failed"
            )

            return False

        finally:
            if not roolback_completed:
                if not session_closed:
                    try:
                        self.acquisition_service.close_session(
                            success=False,
                            reason=reason,
                        )

                    except Exception:
                        self.logger.exception(
                            "Failed to close acquisition session "
                            "during startup rollback"
                        )

                if self.server_state.get_server_state() == ServerFSM.CALIBRATING:
                    self.server_state.process_event(
                        event=ServerFSMEvent.CALIBRATION_FAILED,
                        reason=reason,
                        source="calibration_orchestrator",
                    )

                if self.server_state.get_server_state() == ServerFSM.CALIBRATION_FINALIZING:
                    restored_clients = (
                        self.calibration_service.get_restored_clients()
                    )

                    failed_restore_clients = [
                        client_id
                        for client_id in target_clients
                        if client_id not in restored_clients
                    ]

                    accepted = self.server_state.process_event(
                        event=ServerFSMEvent.CALIBRATION_FINALIZATION_FAILED,
                        reason="Calibration startup rollback incomplete",
                        source="calibration_orchestrator",
                        metadata={
                            "restored_clients": restored_clients,
                            "failed_restore_clients": failed_restore_clients,
                        },
                    )

                    if not accepted:
                        self.logger.error(
                            "Startup rollback finalization rejected"
                        )







    def start_calibration_scan(
        self,
        *,
        scan_spec: CalibrationScanSpec,
        execution_mode: CalibrationMode,
        targets: dict[bytes, list[int]],
        trigger_config: TriggerConfiguration | None,
        acq_type: str,
        file_format: str = "csv",
        duration: float | None = None,
        run_id=None,
    ) -> bool:

        calibration_type = scan_spec.calibration_type
        points = list(scan_spec.points)
        snapshot_spec = scan_spec.snapshot_spec

        current_state = self.server_state.get_server_state()

        if current_state != ServerFSM.READY:
            self.logger.warning(
                f"Cannot start calibration scan: "
                f"server is {current_state.value}, expected READY"
            )
            return False

        if not targets:
            self.logger.error(
                "Cannot start calibration scan: no targets"
            )
            return False

        if not points:
            self.logger.error(
                "Cannot start calibration scan: no scan points"
            )
            return False

        if (
            self._calibration_thread is not None
            and self._calibration_thread.is_alive()
        ):
            self.logger.warning(
                "Cannot start calibration scan: "
                "calibration worker already running"
            )
            return False

        requested_points = {
            client_id: list(points)
            for client_id in targets
        }

    
        try:
            run_folder_clients = (
                self.acquisition_service.get_client_run_folder(
                    client_ids=list(targets.keys()),
                    acq_type=acq_type,
                    run_id=run_id,
                )
            )
        except Exception:
            self.logger.exception(
                "Cannot create calibration run folders"
            )
            return False

        run = self.calibration_service.start_run(
            calibration_type=calibration_type,
            execution_mode=execution_mode,
            targets=targets,
            requested_points=requested_points,
        )

        if run is None:
            self.logger.error(
                "Cannot start calibration scan: "
                "CalibrationService rejected the run"
            )
            return False

        target_clients = (
            self.calibration_service.get_active_clients()
        )
        

        started = self.server_state.process_event(
            event=ServerFSMEvent.CALIBRATION_STARTED,
            reason=f"{calibration_type} calibration started",
            source="calibration_orchestrator",
            metadata={
                "target_clients": target_clients,
            },
        )

        if not started:

            self.logger.error(
                "Cannot start calibration scan: "
                "CALIBRATION_STARTED rejected by ServerState"
            )

            self.calibration_service.discard_unmodified_run()

            return False

        self._calibration_stop_requested.clear()

        #
        # One AcquisitionService session covers the complete calibration
        # scan, not the individual ScanPoints.
        #
        try:
            self.acquisition_service.begin_session()
            if self._calibration_stop_requested.is_set():
                self.acquisition_service.request_stop()

        except Exception:
            self.logger.exception(
                "Failed to begin calibration acquisition session"
            )

            self._handle_start_error_calibration(
                target_clients=target_clients,
                reason="Failed to begin acquisition session"
            )

            return False


        self._calibration_thread = threading.Thread(
            target=self._run_calibration_scan_loop,
            kwargs={
                "apply_point": scan_spec.apply_point,
                "snapshot_spec": snapshot_spec,
                "execution_mode": execution_mode,
                "trigger_config": trigger_config,
                "points": list(points),
                "calibration_type": calibration_type,
                "acq_type": acq_type,
                "file_format": file_format,
                "duration": duration,
                "run_id": run_id,
                "run_folder_clients": run_folder_clients,
            },
            daemon=True,
            name=f"calibration-{calibration_type}",
        )

        try:
            self._calibration_thread.start()

        except Exception:
            self.logger.exception(
                "Failed to start calibration worker"
            )

            self._handle_start_error_calibration(
                target_clients=target_clients,
                reason="Failed to start calibration worker"
            )

            self._calibration_thread = None

            return False

        self.poutput(
            f"Calibration '{calibration_type}' started: "
            f"mode={execution_mode.value}, "
            f"clients={len(target_clients)}, "
            f"points={len(points)}"
        )

        return True
        
    
    def _run_calibration_scan_loop(
        self,
        *,
        apply_point: Callable[[bytes, ScanPoint], bool],
        snapshot_spec: HardwareSnapshotSpec,
        execution_mode: CalibrationMode,
        trigger_config: TriggerConfiguration | None,
        points: list[ScanPoint],
        calibration_type: str,
        acq_type: str,
        run_folder_clients,
        file_format: str = "csv",
        duration: float | None = None,
        run_id=None,
    ) -> None:

        run_clients = (
            self.calibration_service.get_active_clients()
        )


        try:

            #
            # ============================================================
            # 1. HARDWARE SNAPSHOT
            # ============================================================
            #
            # Snapshot must happen before any calibration modification.
            #
            for client_id in run_clients:

                if self._calibration_stop_requested.is_set():
                    break

                snapshot_ok = (
                    self.calibration_service
                    .capture_hardware_snapshot(
                        client_id=client_id,
                        spec=snapshot_spec,
                    )
                )

                if not snapshot_ok:

                    self.calibration_service.mark_client_failed(
                        client_id=client_id,
                        error="hardware snapshot failed",
                    )

            #
            # User may have requested stop while snapshots were being taken.
            #
            if self._calibration_stop_requested.is_set():

                for client_id in (
                    self.calibration_service.get_active_clients()
                ):
                    self.calibration_service.mark_client_aborted(
                        client_id
                    )

            else:

                #
                # ========================================================
                # 2. FAST PREPARATION
                # ========================================================
                #
                rc19_by_client = {}
                if execution_mode == CalibrationMode.FAST:

                    ready_clients = self._prepare_active_calibration_clients(trigger_config=trigger_config)
                    for client_id in ready_clients:
                        value = self.command_service.read_rc_register(client_id=client_id, address=19, plane=CommandPlane.ACQUISITION,)
                        if value is None:
                            self.calibration_service.mark_client_failed(client_id=client_id, error="Failed to read configured RC19",)
                            continue

                        rc19_by_client[client_id] = value
                #
                # ========================================================
                # 3. SCAN LOOP
                # ========================================================
                #
                for point in points:

                    if self._calibration_stop_requested.is_set():
                        break

                    active_clients = (
                        self.calibration_service
                        .get_active_clients()
                    )

                    if not active_clients:
                        break

                    self.poutput(
                        f"Starting calibration point "
                        f"{point.index}: "
                        f"{point.parameter_name}={point.value}"
                    )

                    #
                    # SAFE revalidates/prepares the hardware before
                    # every ScanPoint.
                    #
                    if execution_mode == CalibrationMode.SAFE:

                        self._prepare_active_calibration_clients(
                            trigger_config=trigger_config
                        )

                        if self._calibration_stop_requested.is_set():
                            break

                    if execution_mode == CalibrationMode.FAST:
                        for client_id in self.calibration_service.get_active_clients():
                            value = rc19_by_client.get(client_id)
                            if value is None or not self.command_service.write_rc_register(client_id=client_id, address=19, value=value, plane=CommandPlane.ACQUISITION,):
                                self.calibration_service.mark_client_failed(client_id=client_id, error=f"Failed to restore RC19 at point {point.index}")
                                
                    ready_clients = (
                        self.calibration_service
                        .get_active_clients()
                    )

                    point_clients: list[bytes] = []

                    #
                    # ----------------------------------------------------
                    # 3a. Activate and apply ScanPoint
                    # ----------------------------------------------------
                    #
                    extra_metadata_by_client = {}
                    for client_id in ready_clients:

                        if self._calibration_stop_requested.is_set():
                            break

                        if not self.calibration_service.set_current_point(
                            client_id=client_id,
                            point=point,
                        ):

                            self.calibration_service.mark_client_failed(
                                client_id=client_id,
                                error=(
                                    f"cannot start scan point "
                                    f"{point.index}"
                                ),
                            )

                            continue

                        try:
                            applied = apply_point(client_id, point)
                        except Exception:
                            self.logger.exception(
                                f"Exception while applying scan point "
                                f"{point.index} for client {client_id!r}"
                            )
                            applied = False

                        if not applied:

                            self.calibration_service.mark_client_failed(
                                client_id=client_id,
                                error=(
                                    f"failed to apply scan point "
                                    f"{point.index}"
                                ),
                            )

                            continue

                        session = self.calibration_service.get_session(client_id)
                        if session is None:
                            continue
                        client_metadata  = {
                            "calibration_type": calibration_type,
                            "calibration_execution_mode": execution_mode.value,
                            "scan_point_index": point.index,
                            "scan_parameter": point.parameter_name,
                            "scan_value": point.value,
                            "requested_channels": ",".join(
                                str(ch)
                                for ch in session.requested_channels
                            ),
                            "effective_channels": ",".join(
                                str(ch)
                                for ch in session.effective_channels
                            ),
                        }

                        client_metadata.update(point.metadata)
                        extra_metadata_by_client[client_id] = client_metadata

                        point_clients.append(
                            client_id
                        )

                    #
                    # Stop may have arrived while point registers were
                    # being applied.
                    #
                    if self._calibration_stop_requested.is_set():
                        break

                    if not point_clients:
                        continue

                    #
                    # ----------------------------------------------------
                    # 3b. Acquire this point
                    # ----------------------------------------------------
                    #
                    result = (
                        self.acquisition_service
                        .run_acquisition_session_detailed(
                            client_ids=point_clients,
                            acq_type=acq_type,
                            file_format=file_format,
                            acq_type_param=point.value,
                            suffix=point.suffix,
                            run_id=run_id,
                            run_folder_clients=(
                                run_folder_clients
                            ),
                            duration=duration,
                            reason=(
                                f"{calibration_type} point "
                                f"{point.index} completed"
                            ),
                            reset_trigger_config=False,
                            extra_metadata_by_client=extra_metadata_by_client
                        )
                    )

                    #
                    # If stop was requested during the acquisition,
                    # the point is NOT considered scientifically complete.
                    #
                    if self._calibration_stop_requested.is_set():
                        break
 
                    #
                    # ----------------------------------------------------
                    # 3c. Per-client acquisition result
                    # ----------------------------------------------------
                    #
                    for client_id in (
                        result.successful_clients
                    ):

                        self.calibration_service.mark_point_completed(
                            client_id
                        )

                    for client_id in (
                        result.failed_clients
                    ):

                        client_result = result.clients.get(
                            client_id
                        )

                        error = (
                            client_result.error
                            if client_result is not None
                            and client_result.error
                            else (
                                f"acquisition failed at "
                                f"point {point.index}"
                            )
                        )

                        self.calibration_service.mark_client_failed(
                            client_id=client_id,
                            error=error,
                        )

                #
                # ========================================================
                # 4. SCIENTIFIC OUTCOME
                # ========================================================
                #
                if self._calibration_stop_requested.is_set():

                    for client_id in (
                        self.calibration_service.get_active_clients()
                    ):

                        self.calibration_service.mark_client_aborted(
                            client_id
                        )

                else:

                    #
                    # Clients that survived every point must have completed
                    # their entire requested point list.
                    #
                    for client_id in (
                        self.calibration_service.get_active_clients()
                    ):

                        completed = (
                            self.calibration_service
                            .mark_client_completed(
                                client_id
                            )
                        )

                        if not completed:

                            self.calibration_service.mark_client_failed(
                                client_id=client_id,
                                error=(
                                    "scan ended with incomplete "
                                    "calibration points"
                                ),
                            )

        except Exception as exc:


            self.logger.exception(
                "Unexpected error in calibration scan worker"
            )

            #
            # Anything still scientifically active is failed.
            #
            for client_id in (
                self.calibration_service.get_active_clients()
            ):

                self.calibration_service.mark_client_failed(
                    client_id=client_id,
                    error=(
                        f"unexpected calibration error: {exc}"
                    ),
                )

        finally:

            #
            # ============================================================
            # 5. MOVE SERVER TO CALIBRATION_FINALIZING
            # ============================================================
            #

            with self._calibration_lifecycle_lock:
                present_state = (
                    self.server_state.get_server_state()
                )

                #
                # If an external stop was requested, ServerState has already
                # received CALIBRATION_STOP_REQUESTED and is already in
                # CALIBRATION_FINALIZING.
                #
                if present_state == ServerFSM.CALIBRATING:

                    if self._calibration_stop_requested.is_set():

                        terminal_event = (
                            ServerFSMEvent
                            .CALIBRATION_STOP_REQUESTED
                        )

                        reason = (
                            f"{calibration_type} calibration aborted"
                        )

                    elif (
                        self.calibration_service
                        .get_completed_clients()
                    ):

                        #
                        # This includes PARTIAL scientific success:
                        # at least one client completed the scan.
                        #
                        terminal_event = (
                            ServerFSMEvent
                            .CALIBRATION_COMPLETED
                        )

                        reason = (
                            f"{calibration_type} calibration "
                            "scientific phase completed"
                        )

                    else:

                        terminal_event = (
                            ServerFSMEvent
                            .CALIBRATION_FAILED
                        )

                        reason = (
                            f"{calibration_type} calibration failed"
                        )

                    transitioned = self.server_state.process_event(
                        event=terminal_event,
                        reason=reason,
                        source="calibration_orchestrator",
                    )

                    if not transitioned:

                        self.logger.error(
                            "Failed to enter calibration "
                            "finalization in ServerState"
                        )

            #
            # ============================================================
            # 6. CALIBRATION SERVICE FINALIZATION
            # ============================================================
            #

            finalization_started = (
                self.calibration_service.begin_finalization()
            )

            if not finalization_started:

                self.logger.error(
                    "CalibrationService failed to enter "
                    "finalization"
                )

                try:
                    self.acquisition_service.close_session(
                        success=False,
                        reason=(
                            f"{calibration_type} finalization failed"
                        ),
                    )

                except Exception:
                    self.logger.exception(
                        "Failed to close calibration acquisition session"
                    )

                if self.server_state.get_server_state() == ServerFSM.CALIBRATION_FINALIZING:
                    operational = set(self.server_state.get_operational_clients())
                    failed_clients = [client_id for client_id in run_clients if client_id in operational]

                    self.server_state.process_event(
                        event=ServerFSMEvent.CALIBRATION_FINALIZATION_FAILED,
                        reason=(
                            f"{calibration_type} calibration "
                            "finalization could not start"
                        ),
                        source="calibration_orchestrator",
                        metadata={
                            "restored_clients": [],
                            "failed_restore_clients": failed_clients,
                        },
                    )

                return

            #
            # ============================================================
            # 7. HARDWARE RESTORE
            # ============================================================
            #
            for client_id in run_clients:

                session = (
                    self.calibration_service.get_session(
                        client_id
                    )
                )

                if session is None:

                    self.logger.error(
                        f"Missing calibration session during "
                        f"restore: client={client_id!r}"
                    )

                    continue

                #
                # Snapshot failure happened before any calibration
                # hardware write. Nothing needs to be restored.
                #
                if session.hardware_snapshot is None:

                    ok = (
                        self.calibration_service
                        .mark_restore_not_required(
                            client_id
                        )
                    )

                    if not ok:
                        self.logger.error(
                            f"Failed to finalize client "
                            f"{client_id!r} without restore"
                        )

                    continue

                try:
                    restore_ok = (
                        self.calibration_service
                        .restore_hardware_snapshot(
                            client_id
                        )
                    )
                except Exception:
                    self.logger.exception(
                        f"Exception while restoring hardware configuration "
                        f"for client {client_id!r}"
                    )
                    recorded = self.calibration_service.mark_restore_result(
                        client_id=client_id,
                        succeeded=False,
                    )

                    if not recorded:
                        self.logger.error(
                            "Cannot record hardware restore failure "
                            f"for client {client_id!r}"
                        )

                    continue

                #
                # restore_hardware_snapshot normally records the result
                # itself. This fallback prevents a client from remaining
                # indefinitely in FINALIZING if bookkeeping failed.
                #
                if not restore_ok:

                    session_after = (
                        self.calibration_service.get_session(
                            client_id
                        )
                    )

                    if (
                        session_after is not None
                        and session_after.restore_succeeded
                        is None
                    ):

                        self.calibration_service.mark_restore_result(
                            client_id=client_id,
                            succeeded=False,
                        )

            #
            # ============================================================
            # 8. FINISH CALIBRATION RUN
            # ============================================================
            #

            outcome = (
                self.calibration_service.finish_run()
            )

            if outcome is  None:
                self.logger.error(
                    "CalibrationService could not finish the run"
                )

            manifests_written = (
                self._write_calibration_manifests(
                    run_folder_clients=run_folder_clients,
                )
            )

            if not manifests_written:
                self.logger.warning(
                    "Calibration completed, but one or more "
                    "calibration manifests could not be written"
                )


            restored_clients = (
                self.calibration_service
                .get_restored_clients()
            )

            if outcome is None:
                failed_restore_clients = list(set(run_clients) - set(restored_clients))
            else:
                failed_restore_clients = self.calibration_service.get_failed_restore_clients()


            restore_success = (
                outcome is not None
                and not failed_restore_clients
            )

            #
            # AcquisitionService session bookkeeping only.
            # close_session() will NOT emit ordinary acquisition FSM events
            # while the server is CALIBRATION_FINALIZING.
            #
           
            try:

                self.acquisition_service.close_session(
                    success=restore_success,
                    reason=(
                        f"{calibration_type} calibration completed"
                    ),
                )

            except Exception:

                self.logger.exception(
                    "Failed to close calibration acquisition session"
                )

                restore_success = False

            #
            # ============================================================
            # 9. SERVER FINALIZATION RESULT
            # ============================================================
            #

            accepted = False

            with self._calibration_lifecycle_lock:
                if (
                    self.server_state.get_server_state()
                    == ServerFSM.CALIBRATION_FINALIZING
                ):

                    operational = set(self.server_state.get_operational_clients())
                    restored_clients = [client_id for client_id in restored_clients if client_id in operational]

                    final_event = (
                        ServerFSMEvent
                        .CALIBRATION_FINALIZATION_SUCCEEDED
                        if restore_success
                        else ServerFSMEvent
                        .CALIBRATION_FINALIZATION_FAILED
                    )

                    accepted = self.server_state.process_event(
                        event=final_event,
                        reason=(
                            f"{calibration_type} calibration "
                            "hardware finalization completed"
                            if restore_success
                            else (
                                f"{calibration_type} calibration "
                                "hardware finalization failed"
                            )
                        ),
                        source="calibration_orchestrator",
                        metadata={
                            "restored_clients": restored_clients,
                            "failed_restore_clients": (
                                failed_restore_clients
                            ),
                        },
                    )

                    if not accepted:
                        self.logger.error("Server calibration finalization rejected")

            #
            # ============================================================
            # 10. CLEAR RUNTIME
            # ============================================================
            #
            if outcome is not None and accepted:

                if not self.calibration_service.clear_run():
                    self.logger.error(
                        "Cannot clear completed calibration run"
                    )

            was_aborted = (
                self._calibration_stop_requested.is_set()
            )

            self._calibration_stop_requested.clear()

            self.poutput(
                f"Calibration '{calibration_type}' finished"
                + (
                    f" with outcome={outcome.value}"
                    if outcome is not None
                    else " with finalization error"
                )
                + (
                    " after user abort."
                    if was_aborted
                    else "."
                )
            )
    
    def request_calibration_stop(self) -> bool:

        with self._calibration_lifecycle_lock:
            if (
                self.server_state.get_server_state()
                != ServerFSM.CALIBRATING
            ):
                self.logger.warning(
                    "Calibration stop requested while calibration "
                    "is not running"
                )
                return False

            accepted = self.server_state.process_event(
                event=ServerFSMEvent.CALIBRATION_STOP_REQUESTED,
                reason="Calibration stop requested by user",
                source="calibration_orchestrator",
            )

            if not accepted:
                return False


            self._calibration_stop_requested.set()


        self.acquisition_service.request_stop()
        

        self.poutput(
            "Calibration stop requested. "
            "Current acquisition point is being finalized."
        )

        return True
                
                
            
            
                
            
            
            