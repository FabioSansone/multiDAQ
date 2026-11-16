import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from enum import Enum

from server.utils.logger import get_logger
from server.services.calibration_service import (
    CalibrationScanSpec, CalibrationService, CalibrationMode, CalibrationRun,
    ClientCalibrationSession, ClientHardwareSnapshot, ScanPoint, CalibrationRunOutcome,
)
from server.services.acquisition_service import AcquisitionService, TriggerConfiguration
from server.core.server_state import ServerState, ServerFSM, ServerFSMEvent
from server.services.channel_selection_service import ChannelSelectionService, ChannelSelection
from server.services.client_command_service import CommandPlane


class CalibrationLoopOutcome(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"

class CalibrationEngine:

    def __init__(self, server_state: ServerState, calibration_service: CalibrationService, acquisition_service: AcquisitionService, channel_selection_service: ChannelSelectionService):

        self.calibration_service = calibration_service
        self.acquisition_service = acquisition_service
        self.server_state = server_state
        self.channel_selection_service = channel_selection_service

        self._calibration_run_thread: threading.Thread | None = None
        self._calibration_run_stop = threading.Event()
        self._lifecycle_lock = threading.Lock()

        self._trigger_clients_to_reset: list[bytes] = []

        self._finalization_target: ServerFSM | None = None

        self.logger = get_logger("calibration_engine")
        self.logger.debug("Calibration Engine initialized")


    def start_scan(self, calibration_spec: CalibrationScanSpec, calibration_mode: CalibrationMode, client_channels_targets: dict[bytes, list[int]], trigger_configuration: TriggerConfiguration | None, duration: float | None, acq_type: str, run_id: str | int | None, file_format: str = "csv") -> bool:
        """
        Prepare a calibration and start its worker thread, then return: the scan itself
        runs in the thread. Nothing here touches the hardware.

        Returns False (and logs why) if the calibration cannot start; in that case the
        server is left as it was.
        """


        current_state = self.server_state.get_server_state()
        if current_state not in (ServerFSM.READY, ServerFSM.CALIBRATION_SEQUENCE):
            self.logger.error(
                f"Cannot start calibration scan: server is {current_state.value}, "
                "expected ready or calibration_sequence"
            )
            return False

        if not client_channels_targets or not isinstance(client_channels_targets, dict):
            self.logger.error("Cannot start calibration scan: no calibration targets")
            return False

        if not calibration_spec.points:
            self.logger.error("Cannot start calibration scan: the scan spec has no points")
            return False

        if self._calibration_run_thread is not None and self._calibration_run_thread.is_alive():
            self.logger.error("Cannot start calibration scan: the calibration worker is already running")
            return False

        # After the run the server goes back where it started from: READY, or
        # CALIBRATION_SEQUENCE if this is a phase of a sequence.
        finalization_target = current_state
        self._finalization_target = current_state

        if duration is None or duration <= 0:
            self.logger.error(f"Cannot start calibration scan: invalid duration {duration!r}, expected a positive number of seconds")
            return False

        if file_format not in ("csv", "bin"):
            self.logger.error(f"Cannot start calibration scan: invalid file format {file_format!r}, expected csv or bin")
            return False

        # Without a trigger configuration the RC19 mask is built from the RC19 of the snapshot.
        if trigger_configuration is None and 19 not in calibration_spec.snapshot_spec.rc_register:
            self.logger.error("Cannot start calibration scan: without a trigger configuration the snapshot must include RC19")
            return False

        client_ids = list(client_channels_targets.keys())

        # The server is READY or CALIBRATION_SEQUENCE and no worker is alive: a run still in
        # the service is left by a finalization that could not finish it. Try the normal
        # clear; if the run is not FINISHED it is abandoned (the hardware was already
        # handled by that finalization), otherwise start_run would reject every new run.
        stale_run = self.calibration_service.get_run()

        if stale_run is not None:
            self.logger.warning(
                f"A calibration run is left in the service (type={stale_run.calibration_type}, "
                f"status={stale_run.status.value}): clearing it before starting"
            )

            if not self.calibration_service.clear_run():
                self.calibration_service.abandon_run("left by a previous finalization that could not finish it")

        try:
            client_run_folders = self.acquisition_service.get_client_run_folder(client_ids=client_ids, acq_type=acq_type, run_id=run_id)
        except Exception:
            self.logger.exception("Cannot start calibration scan: the run folders could not be created")
            return False


        requested_points = {client_id: list(calibration_spec.points) for client_id in client_ids}

        run = self.calibration_service.start_run(
            calibration_type=calibration_spec.calibration_type,
            execution_mode=calibration_mode,
            targets=client_channels_targets,
            requested_points=requested_points,
        )

        if run is None:
            self.logger.error("Cannot start calibration scan: CalibrationService rejected the run")
            return False

        target_clients = self.calibration_service.get_active_clients()


        # From CALIBRATION_STARTED to the start of the thread nothing else can change the run:
        # a stop that arrived in the middle would put the server in finalization while the
        # worker, started afterwards, ran the whole calibration. request_stop takes this lock.
        with self._lifecycle_lock:
            self._calibration_run_stop.clear()
            self._trigger_clients_to_reset = []

            started = self.server_state.process_event(
                event=ServerFSMEvent.CALIBRATION_STARTED,
                reason=f"{calibration_spec.calibration_type} calibration started",
                source="calibration_engine",
                metadata={"target_clients": target_clients},
            )

            if not started:
                self.logger.error("Cannot start calibration scan: CALIBRATION_STARTED rejected by ServerState")
                # Nothing has touched the hardware yet: the run can simply be dropped.
                self.calibration_service.discard_unmodified_run()
                return False

            # 5. One acquisition session covers the whole scan, not every point

            try:
                self.acquisition_service.begin_session()
            except Exception:
                self.logger.exception("Cannot start calibration scan: the acquisition session did not begin")
                self._abort_start(target_clients, "Failed to begin the acquisition session", finalization_target)
                return False


            self._calibration_run_thread = threading.Thread(
                target=self._run_scan_loop,
                kwargs={
                    "calibration_spec": calibration_spec,
                    "calibration_mode": calibration_mode,
                    "trigger_configuration": trigger_configuration,
                    "duration": duration,
                    "acq_type": acq_type,
                    "run_id": run_id,
                    "file_format": file_format,
                    "client_run_folders": client_run_folders,
                    "finalization_target": finalization_target,
                },
                daemon=True,
                name=f"calibration-{calibration_spec.calibration_type}",
            )

            try:
                self._calibration_run_thread.start()
            except Exception:
                self.logger.exception("Cannot start calibration scan: the worker thread did not start")
                self._calibration_run_thread = None
                self._abort_start(target_clients, "Failed to start the calibration worker", finalization_target)
                return False

            self.logger.info(
                f"Calibration '{calibration_spec.calibration_type}' started: "
                f"mode={calibration_mode.value}, clients={len(target_clients)}, "
                f"points={len(calibration_spec.points)}"
            )

        return True


    def request_stop(self) -> bool:
        """
        Stop the calibration. Called from the thread of the prompt.

        CALIBRATING: the server goes to CALIBRATION_FINALIZING at once, the worker is told
        to stop and the wait of the acquisition is woken up. The hardware operation in
        progress (a callback, a ramp) is not interrupted: the stop takes effect when it ends.
        CALIBRATION_FINALIZING: the restore of the hardware is never interrupted. If the
        calibration is a phase of a sequence the stop is remembered, so that the sequence
        stops after the cleanup.
        CALIBRATION_SEQUENCE: between two phases, the stop is remembered.

        True if the stop will take effect on the calibration or on the sequence.
        """

        with self._lifecycle_lock:
            state = self.server_state.get_server_state()

            if state == ServerFSM.CALIBRATING:
                # The event first: if the FSM refuses it, no flag is left raised.
                accepted = self.server_state.process_event(
                    event=ServerFSMEvent.CALIBRATION_STOP_REQUESTED,
                    reason="Calibration stop requested by user",
                    source="calibration_engine",
                    requested_terminal_state=self._finalization_target,
                )

                if not accepted:
                    self.logger.error("Calibration stop rejected by ServerState")
                    return False

                self._calibration_run_stop.set()
                self.acquisition_service.request_stop()
                self.logger.info("Calibration stop requested: the worker stops at the end of the current operation")
                return True

            if state == ServerFSM.CALIBRATION_FINALIZING:
                if self._finalization_target == ServerFSM.CALIBRATION_SEQUENCE:
                    self._calibration_run_stop.set()
                    self.logger.info("Calibration stop requested during the finalization: the sequence stops after the cleanup")
                    return True

                self.logger.warning("Calibration is already finalizing: the hardware restore cannot be interrupted")
                return False

            if state == ServerFSM.CALIBRATION_SEQUENCE:
                self._calibration_run_stop.set()
                self.logger.info("Calibration stop requested between two phases of a sequence")
                return True

            self.logger.warning(f"No calibration to stop: server is {state.value}")
            return False


    def stop_for_shutdown(self, timeout: float | None = 900.0) -> bool:
        """
        Stop the calibration because the server is shutting down, and wait for the
        worker to end. Called after DISCONNECT_REQUESTED, which already moved the FSM:
        no event is sent here, only the worker is told to stop. A restore in progress
        is not interrupted: the wait lets it finish (the clients go to the safe state
        afterwards).

        True if no worker is left running (also when there was none). False if the wait
        timed out.
        """

        with self._lifecycle_lock:
            self._calibration_run_stop.set()
            self.acquisition_service.request_stop()
            thread = self._calibration_run_thread

        if thread is None or not thread.is_alive():
            return True

        self.logger.info("Waiting for the calibration worker to stop and restore the hardware before the shutdown")
        thread.join(timeout=timeout)

        if thread.is_alive():
            self.logger.error(f"The calibration worker did not end within {timeout} s: the shutdown goes on")
            return False

        return True

    def _abort_start(self, client_ids: list[bytes], reason: str, finalization_target: ServerFSM) -> None:
        """
        Undo start_scan after the server entered CALIBRATING, when no hardware was
        touched yet: nothing to restore, so every client is marked failed, restore
        is not required, and the server returns to where it was.
        """

        self.logger.error(f"Calibration start aborted: {reason}")

        for client_id in client_ids:
            self.calibration_service.mark_client_failed(client_id=client_id, error=reason)

        self.calibration_service.begin_finalization()

        for client_id in client_ids:
            self.calibration_service.mark_restore_not_required(client_id)

        self.calibration_service.finish_run()
        self.calibration_service.clear_run()

        # The acquisition session may have begun: close it without touching the FSM.
        self.acquisition_service.close_session(success=False, reason=reason)

        self.server_state.process_event(
            event=ServerFSMEvent.CALIBRATION_FAILED,
            reason=reason,
            source="calibration_engine",
            requested_terminal_state=finalization_target,
        )

        self.server_state.process_event(
            event=ServerFSMEvent.CALIBRATION_FINALIZATION_SUCCEEDED,
            reason=f"{reason}: hardware not modified",
            source="calibration_engine",
            metadata={"restored_clients": client_ids},
        )


    ###########
    # MANIFEST #
    ###########

    @staticmethod
    def _scan_point_to_dict(point: ScanPoint) -> dict:
        return {
            "index": point.index,
            "suffix": point.suffix,
            "parameters": dict(point.parameters),
            "metadata": dict(point.metadata),
        }

    @staticmethod
    def _hardware_snapshot_to_dict(snapshot: ClientHardwareSnapshot) -> dict:
        return {
            "rc_registers": dict(snapshot.rc_registers),
            "hv_parameters": {channel: dict(parameters) for channel, parameters in snapshot.hv_parameters.items()},
            "extra": dict(snapshot.extra),
        }

    def _build_calibration_manifest(self, client_id: bytes, run: CalibrationRun, session: ClientCalibrationSession) -> dict:

        configuration_identity = self.server_state.get_configuration_identity(client_id) or {}

        return {
            "version": 1,
            "calibration_type": run.calibration_type,
            "execution_mode": run.execution_mode.value,
            "start_time_utc": run.start_time.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "end_time_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "run_outcome": run.outcome.value if run.outcome is not None else None,
            "run_status": run.status.value,
            "client_id": client_id.decode(errors="ignore"),
            "configuration_identity": {
                "multipmt_id": configuration_identity.get("multipmt_id"),
                "batch_id": configuration_identity.get("batch_id"),
            },
            "requested_channels": list(session.requested_channels),
            "effective_channels_final": list(session.effective_channels),
            "channels_not_confirmed_off": list(session.channels_not_confirmed_off),
            "requested_points": [self._scan_point_to_dict(point) for point in session.requested_points],
            "completed_points": [self._scan_point_to_dict(point) for point in session.completed_points],
            "client_outcome": session.outcome.value if session.outcome is not None else None,
            "error": session.error,
            "restore_succeeded": session.restore_succeeded,
            "restore_failed_channels": list(session.restore_failed_channels),
            "hardware_snapshot": (
                self._hardware_snapshot_to_dict(session.hardware_snapshot)
                if session.hardware_snapshot is not None
                else None
            ),
        }

    def _write_calibration_manifests(self, run_folder_clients: dict[bytes, Path]) -> bool:
        """
        Write calibration_manifest.json in the run folder of every client. To be
        called after finish_run and before clear_run. A failure is logged and
        reported with False, but must never stop the finalization.
        """

        run = self.calibration_service.get_run()

        if run is None:
            self.logger.error("Cannot write calibration manifests: no calibration run is available")
            return False

        sessions = self.calibration_service.get_sessions()

        if not sessions:
            self.logger.error("Cannot write calibration manifests: no calibration sessions are available")
            return False

        overall_success = True

        for client_id, session in sessions.items():
            client_name = client_id.decode(errors="ignore")
            run_folder = run_folder_clients.get(client_id)

            if run_folder is None:
                self.logger.error(f"Cannot write calibration manifest: missing run folder for client {client_name}")
                overall_success = False
                continue

            try:
                manifest = self._build_calibration_manifest(client_id=client_id, run=run, session=session)
                manifest_path = Path(run_folder) / "calibration_manifest.json"

                with manifest_path.open("w", encoding="utf-8") as manifest_file:
                    json.dump(manifest, manifest_file, indent=4, ensure_ascii=False, default=str)

                self.logger.info(f"Calibration manifest written: client={client_name}, path={manifest_path}")

            except Exception:
                overall_success = False
                self.logger.exception(f"Failed to write calibration manifest for client {client_name}")

        return overall_success


    def _run_scan_loop(self, calibration_spec: CalibrationScanSpec, calibration_mode: CalibrationMode, trigger_configuration: TriggerConfiguration | None, duration: float | None, acq_type: str, run_id: str | int | None, file_format: str, client_run_folders: dict[bytes, Path], finalization_target: ServerFSM) -> None:

        scan_loop_outcome = CalibrationLoopOutcome.FAILED

        try:
            # A phase that returns False either failed or was interrupted by a stop:
            # the stop request tells the two cases apart (ABORTED, not FAILED).
            for phase_ok in (
                lambda: self._resolve_channels(),
                lambda: self._take_snapshots(calibration_spec),
                lambda: self._disable_rc19(),
                lambda: self._run_prepare_run(calibration_spec),
            ):
                if not phase_ok():
                    if self._stop_requested():
                        scan_loop_outcome = CalibrationLoopOutcome.STOPPED
                    return

            scan_loop_outcome = self._run_points(
                calibration_spec=calibration_spec,
                trigger_configuration=trigger_configuration,
                duration=duration,
                acq_type=acq_type,
                run_id=run_id,
                file_format=file_format,
                client_run_folders=client_run_folders,
            )

        except Exception:
            self.logger.exception("Unexpected error in the calibration worker: going to finalization")
            scan_loop_outcome = CalibrationLoopOutcome.FAILED

        finally:
            self._finalize(scan_loop_outcome, finalization_target, client_run_folders)


    def _resolve_channels(self) -> bool:
        active_clients = self.calibration_service.get_active_clients()

        for cid in active_clients:
            if self._stop_requested():
                return False

            client_name = cid.decode(errors="ignore")

            cid_session = self.calibration_service.get_session(client_id=cid)
            if cid_session is None:
                error = f"no calibration session for client {client_name}"
                self.logger.error(error)
                self.calibration_service.mark_client_failed(client_id=cid, error=error)
                continue

            channels_requested = cid_session.requested_channels

            try:
                channels_selection = self.channel_selection_service.get_test_rc_channels(client_id=cid, requested_channels=channels_requested, plane=CommandPlane.ACQUISITION)

                for note in channels_selection.notes:
                    self.logger.info(note)

                if not channels_selection.channels:
                    self.calibration_service.mark_client_failed(client_id=cid, error="no usable channels")
                    continue

                if not self.calibration_service.set_effective_channels(client_id=cid, channels=channels_selection.channels):
                    self.calibration_service.mark_client_failed(client_id=cid, error="effective channels could not be set")

            except Exception as exc:
                self.logger.exception(f"Channel resolution failed for client {client_name}")
                self.calibration_service.mark_client_failed(client_id=cid, error=f"channel resolution failed: {exc}")

        return bool(self.calibration_service.get_active_clients())


    def _take_snapshots(self, calibration_spec: CalibrationScanSpec) -> bool:
        active_clients = self.calibration_service.get_active_clients()

        for cid in active_clients:
            if self._stop_requested():
                return False

            client_name = cid.decode(errors="ignore")

            # A failed client has no snapshot, so it will not need a restore: the finalization
            # marks it (mark_restore_not_required works only once the run is finalizing).
            try:
                if not self.calibration_service.capture_hardware_snapshot(client_id=cid, spec=calibration_spec.snapshot_spec):
                    error = "hardware snapshot could not be captured"
                    self.logger.error(f"Client {client_name}: {error}")
                    self.calibration_service.mark_client_failed(client_id=cid, error=error)

            except Exception as exc:
                self.logger.exception(f"Hardware snapshot failed for client {client_name}")
                self.calibration_service.mark_client_failed(client_id=cid, error=f"hardware snapshot failed: {exc}")

        return bool(self.calibration_service.get_active_clients())


    def _disable_rc19(self) -> bool:
        active_clients = self.calibration_service.get_active_clients()

        for cid in active_clients:
            if self._stop_requested():
                return False

            client_name = cid.decode(errors="ignore")

            # One client at a time: disable_rc_channels returns a single bool, and a client
            # whose RC19 could not be set to 0 must not measure (residual data).
            try:
                if not self.acquisition_service.disable_rc_channels(client_ids=[cid]):
                    error = "RC19 could not be set to 0"
                    self.logger.error(f"Client {client_name}: {error}")
                    self.calibration_service.mark_client_failed(client_id=cid, error=error)

            except Exception as exc:
                self.logger.exception(f"Disabling RC19 failed for client {client_name}")
                self.calibration_service.mark_client_failed(client_id=cid, error=f"RC19 disable failed: {exc}")

        return bool(self.calibration_service.get_active_clients())

    def _run_prepare_run(self, calibration_spec: CalibrationScanSpec) -> bool:
        if self._stop_requested():
            self.logger.warning("Stop requested before prepare_run: no preparation, no acquisition")
            return False

        if calibration_spec.prepare_run is None:
            self.logger.info("No prepare_run for this calibration: nothing to prepare")
            return bool(self.calibration_service.get_active_clients())

        self.logger.info(f"Running prepare_run of the '{calibration_spec.calibration_type}' calibration")

        error = None

        try:
            if not calibration_spec.prepare_run():
                error = "prepare_run failed"
        except Exception as exc:
            self.logger.exception("prepare_run raised an exception")
            error = f"prepare_run failed: {exc}"

        if error is not None:
            # A client already excluded by the callback keeps its first, more specific error.
            self.logger.error(f"{error}: no acquisition will be done")
            for cid in self.calibration_service.get_active_clients():
                self.calibration_service.mark_client_failed(client_id=cid, error=error)
            return False

        if self._stop_requested():
            self.logger.warning("Stop requested during prepare_run: no acquisition")
            return False

        # The callback can exclude clients through the calibration service.
        active_clients = self.calibration_service.get_active_clients()

        if not active_clients:
            self.logger.error("prepare_run left no active client: no acquisition will be done")
            return False

        self.logger.info(f"prepare_run completed: {len(active_clients)} active client(s)")
        return True

    def _run_points(self, calibration_spec: CalibrationScanSpec, trigger_configuration: TriggerConfiguration | None, duration: float | None, acq_type: str, run_id: str | int | None, file_format: str, client_run_folders: dict[bytes, Path]) -> CalibrationLoopOutcome:

        calibration_points = calibration_spec.points
        total_points = len(calibration_points)

        self.logger.info(f"Starting the '{calibration_spec.calibration_type}' scan: {total_points} point(s)")

        for index, point in enumerate(calibration_points):
            is_last_point = index == total_points - 1

            if self._stop_requested():
                self.logger.warning(f"Stop requested before point {index + 1}/{total_points}: the scan ends here")
                return CalibrationLoopOutcome.STOPPED

            if not self.calibration_service.get_active_clients():
                self.logger.error(f"No active client left before point {index + 1}/{total_points}: the scan ends here")
                return CalibrationLoopOutcome.FAILED

            self.logger.info(f"Point {index + 1}/{total_points}: suffix={point.suffix}, parameters={point.parameters}")

            ok = self._run_point(point=point, calibration_spec=calibration_spec, trigger_configuration=trigger_configuration, duration=duration, acq_type=acq_type, run_id=run_id, file_format=file_format, client_run_folders=client_run_folders, is_last_point=is_last_point)

            if not ok:
                if self._stop_requested():
                    self.logger.warning(f"Stop requested during point {index + 1}/{total_points}: the scan ends here")
                    return CalibrationLoopOutcome.STOPPED

                self.logger.error(f"No client completed point {index + 1}/{total_points}: the scan ends here")
                return CalibrationLoopOutcome.FAILED

        # mark_client_completed moves the client out of the active ones: the result is read
        # from the completed clients, not from the active ones.
        for cid in self.calibration_service.get_active_clients():
            if not self.calibration_service.mark_client_completed(client_id=cid):
                self.logger.error(f"Client {cid.decode(errors='ignore')} could not be marked as completed: some points are missing")

        completed_clients = self.calibration_service.get_completed_clients()

        if completed_clients:
            self.logger.info(f"Scan completed: {len(completed_clients)} client(s) completed all {total_points} point(s)")
            return CalibrationLoopOutcome.COMPLETED

        self.logger.error("Scan ended with no client completed")
        return CalibrationLoopOutcome.FAILED


    def _fail_client(self, client_id: bytes, error: str) -> None:
        self.logger.error(f"Client {client_id.decode(errors='ignore')}: {error}")
        self.calibration_service.mark_client_failed(client_id=client_id, error=error)


    def _run_point(self, point: ScanPoint, calibration_spec: CalibrationScanSpec, trigger_configuration: TriggerConfiguration | None, duration: float | None, acq_type: str, run_id: str | int | None, file_format: str, client_run_folders: dict[bytes, Path], is_last_point: bool) -> bool:
        """
        One point: set it, prepare it (shared), apply it (per client), configure the trigger
        with the final channels, acquire, and record the outcome per client.

        True if at least one client completed the point. False if none did, also because of
        a stop: then the caller reads _stop_requested() to tell the two cases apart.
        """

        for cid in self.calibration_service.get_active_clients():
            if not self.calibration_service.set_current_point(client_id=cid, point=point):
                self._fail_client(cid, f"scan point {point.index} could not be set")

        if not self.calibration_service.get_active_clients():
            self.logger.error(f"Point {point.index}: no client could start the point")
            return False

        if not self._prepare_point(calibration_spec, point):
            return False

        if self._stop_requested():
            self.logger.warning(f"Stop requested after prepare_point of point {point.index}")
            return False

        self._apply_point_to_clients(calibration_spec, point)

        if self._stop_requested():
            self.logger.warning(f"Stop requested while applying point {point.index}")
            return False

        if not self.calibration_service.get_active_clients():
            self.logger.error(f"Point {point.index}: no client left after apply_point")
            return False

        # The RC19 mask is built here, after apply_point, with the final effective channels.
        self._configure_trigger_for_clients(trigger_configuration)

        if self._stop_requested():
            self.logger.warning(f"Stop requested while configuring the trigger for point {point.index}")
            return False

        acquisition_clients = self.calibration_service.get_active_clients()

        if not acquisition_clients:
            self.logger.error(f"Point {point.index}: no client left after the trigger configuration")
            return False

        self.logger.info(f"Point {point.index}: acquiring on {len(acquisition_clients)} client(s)")

        # An exception here goes up to the worker: the acquisition service has already
        # finalized the hardware, and the finalization settles the clients.
        # acq_type_param stays empty: the header already has the acquisition type, and the
        # parameters of the point are written in the calibration manifest.
        session_result = self.acquisition_service.run_acquisition_session(
            client_ids=acquisition_clients,
            acq_type=acq_type,
            file_format=file_format,
            suffix=point.suffix,
            run_id=run_id,
            run_folder_clients=client_run_folders,
            duration=duration,
            reason=f"calibration point {point.index} ({point.suffix}) ended",
            # Without trigger configuration the trigger registers were never written: nothing to reset
            # (and the snapshot does not hold them, so a reset could not be undone).
            reset_trigger_config=is_last_point and trigger_configuration is not None,
        )

        # At the last point the session also resets the trigger of the clients it finalized.
        if is_last_point and trigger_configuration is not None:
            for cid in acquisition_clients:
                client_result = session_result.clients.get(cid)
                if client_result is not None and client_result.finalized and cid in self._trigger_clients_to_reset:
                    self._trigger_clients_to_reset.remove(cid)

        if self._stop_requested():
            self.logger.warning(f"Stop requested during point {point.index}: the measurement is partial and is not counted")
            return False

        completed = 0

        for cid in acquisition_clients:
            client_result = session_result.clients.get(cid)

            if client_result is None or not client_result.success:
                error = client_result.error if client_result is not None and client_result.error else "acquisition failed"
                self._fail_client(cid, f"point {point.index}: {error}")
                continue

            if not self.calibration_service.mark_point_completed(client_id=cid):
                self._fail_client(cid, f"point {point.index} could not be marked as completed")
                continue

            completed += 1

        self.logger.info(f"Point {point.index} done: {completed}/{len(acquisition_clients)} client(s) completed it")

        return completed > 0


    def _prepare_point(self, calibration_spec: CalibrationScanSpec, point: ScanPoint) -> bool:
        """The shared preparation of the point. False if it failed: every active client fails with it."""

        if calibration_spec.prepare_point is None:
            return True

        error = None

        try:
            if not calibration_spec.prepare_point(point):
                error = f"prepare_point failed at point {point.index}"
        except Exception as exc:
            self.logger.exception(f"prepare_point raised an exception at point {point.index}")
            error = f"prepare_point failed at point {point.index}: {exc}"

        if error is None:
            return True

        for cid in self.calibration_service.get_active_clients():
            self._fail_client(cid, error)

        return False


    def _apply_point_to_clients(self, calibration_spec: CalibrationScanSpec, point: ScanPoint) -> None:
        """Apply the point to each active client. A failure excludes only that client. Stops at a stop request."""

        if calibration_spec.apply_point is None:
            return

        for cid in self.calibration_service.get_active_clients():
            if self._stop_requested():
                return

            try:
                if not calibration_spec.apply_point(cid, point):
                    self._fail_client(cid, f"apply_point failed at point {point.index}")
            except Exception as exc:
                self.logger.exception(f"apply_point raised an exception for client {cid.decode(errors='ignore')}")
                self.calibration_service.mark_client_failed(client_id=cid, error=f"apply_point failed at point {point.index}: {exc}")


    def _configure_trigger_for_clients(self, trigger_configuration: TriggerConfiguration | None) -> None:
        """
        RC trigger registers and intended RC19 mask of each active client, with its final channels.

        With a trigger configuration the registers are written and the clients are remembered, so
        that finalization resets them. Without it (e.g. the pedestal) the trigger registers are not
        touched, the clients are not remembered, and the RC19 mask comes from the snapshot.
        """

        for cid in self.calibration_service.get_active_clients():
            if self._stop_requested():
                return

            name = cid.decode(errors="ignore")
            effective_channels = self.calibration_service.get_effective_channels(client_id=cid)

            try:
                if trigger_configuration is None:
                    session = self.calibration_service.get_session(client_id=cid)
                    snapshot = session.hardware_snapshot if session is not None else None
                    snapshot_rc19 = snapshot.rc_registers.get(19) if snapshot is not None else None

                    if not self.acquisition_service.prepare_rc19_from_snapshot(client_id=cid, snapshot_rc19=snapshot_rc19, effective_channels=effective_channels):
                        self._fail_client(cid, "RC19 preparation failed")

                    continue

                # Remembered before writing: a write that fails halfway leaves registers to reset.
                if cid not in self._trigger_clients_to_reset:
                    self._trigger_clients_to_reset.append(cid)

                if not self.acquisition_service.configure_acquisition_client(client_id=cid, trigger_config=trigger_configuration, effective_channels=effective_channels):
                    self._fail_client(cid, "trigger configuration failed")
            except Exception as exc:
                self.logger.exception(f"Trigger configuration raised an exception for client {name}")
                self.calibration_service.mark_client_failed(client_id=cid, error=f"trigger configuration failed: {exc}")

    def _enter_calibration_finalization(self, scan_loop_outcome: CalibrationLoopOutcome, finalization_target: ServerFSM) -> None:
        """
        Move the FSM from CALIBRATING to CALIBRATION_FINALIZING, with the event that
        matches the outcome of the loop.

        The event is sent only if the server is still CALIBRATING: a stop or a
        disconnection may have already moved it to CALIBRATION_FINALIZING from outside
        the worker. A refusal is logged and does not interrupt the finalization:
        the hardware has to be restored anyway.
        """

        events = {
            CalibrationLoopOutcome.COMPLETED: (ServerFSMEvent.CALIBRATION_COMPLETED, "Calibration loop completed"),
            CalibrationLoopOutcome.FAILED: (ServerFSMEvent.CALIBRATION_FAILED, "Calibration loop failed"),
            CalibrationLoopOutcome.STOPPED: (ServerFSMEvent.CALIBRATION_STOP_REQUESTED, "Calibration loop stopped"),
        }

        try:
            # The same lock of request_stop: the state cannot change between the check and the event.
            with self._lifecycle_lock:
                state = self.server_state.get_server_state()

                if state != ServerFSM.CALIBRATING:
                    self.logger.info(f"Calibration finalization already started from outside the worker (server is {state.value}): no event sent")
                    return

                event, reason = events[scan_loop_outcome]
                accepted = self.server_state.process_event(
                    event=event,
                    reason=reason,
                    source="calibration_engine",
                    requested_terminal_state=finalization_target,
                )

                if not accepted:
                    self.logger.error(f"ServerState rejected {event.value}: the finalization continues anyway")
        except Exception:
            self.logger.exception("Could not move the server to CALIBRATION_FINALIZING: the finalization continues anyway")

    def _resolve_unfinished_clients(self, scan_loop_outcome: CalibrationLoopOutcome) -> None:
        """
        Give an explicit outcome to every client still RUNNING, before begin_finalization.

        After begin_finalization the mark_client_* methods no longer work, and a client
        without outcome would make the run PARTIAL. STOPPED: the clients are aborted.
        COMPLETED or FAILED: a client still RUNNING did not finish its points, so it fails.
        """

        running_clients = list(self.calibration_service.get_active_clients())

        if not running_clients:
            return

        for cid in running_clients:
            name = cid.decode(errors="ignore")

            try:
                if scan_loop_outcome == CalibrationLoopOutcome.STOPPED:
                    if self.calibration_service.mark_client_aborted(client_id=cid):
                        self.logger.info(f"Client {name} aborted: calibration stopped before it completed")
                    else:
                        self.logger.warning(f"Could not mark client {name} as aborted")
                else:
                    if scan_loop_outcome == CalibrationLoopOutcome.COMPLETED:
                        self.logger.warning(f"Client {name} still RUNNING after a completed loop: marking it failed")

                    if self.calibration_service.mark_client_failed(client_id=cid, error="calibration ended before the client completed all points"):
                        self.logger.info(f"Client {name} marked failed: calibration ended before it completed all points")
                    else:
                        self.logger.warning(f"Could not mark client {name} as failed")
            except Exception:
                self.logger.exception(f"Could not resolve the outcome of client {name}")


    def _finalize_without_run(self, run_clients: list[bytes], reason: str) -> None:
        """
        Emergency path: begin_finalization refused, so there is no run in a state that
        allows the restore of the snapshots. The minimum is done: RC19=0, the
        acquisition session closed and the server released from CALIBRATION_FINALIZING
        with every client of the run reported as not restored.
        """

        self.logger.error(f"Calibration finalization without a restorable run: {reason}")

        try:
            if not self.acquisition_service.disable_rc_channels(client_ids=run_clients):
                self.logger.error("RC19 could not be disabled for every client of the run: the hardware state is uncertain")
        except Exception:
            self.logger.exception("Disabling RC19 raised an exception: the hardware state is uncertain")

        try:
            self.acquisition_service.close_session(success=False, reason=reason)
        except Exception:
            self.logger.exception("Closing the acquisition session raised an exception")

        try:
            with self._lifecycle_lock:
                state = self.server_state.get_server_state()

                if state != ServerFSM.CALIBRATION_FINALIZING:
                    self.logger.warning(f"No finalization event sent: server is {state.value}")
                else:
                    # The FSM rejects clients that are no longer operational (disconnected meanwhile).
                    operational = set(self.server_state.get_operational_clients())
                    failed_clients = [cid for cid in run_clients if cid in operational]

                    accepted = self.server_state.process_event(
                        event=ServerFSMEvent.CALIBRATION_FINALIZATION_FAILED,
                        reason=f"{reason}: hardware not restored",
                        source="calibration_engine",
                        metadata={"restored_clients": [], "failed_restore_clients": failed_clients},
                    )

                    if not accepted:
                        self.logger.error("ServerState rejected CALIBRATION_FINALIZATION_FAILED: the server may stay in CALIBRATION_FINALIZING")
        except Exception:
            self.logger.exception("Could not send CALIBRATION_FINALIZATION_FAILED")

        try:
            if not self.calibration_service.clear_run():
                self.logger.error("Could not clear the calibration run")
        except Exception:
            self.logger.exception("Clearing the calibration run raised an exception")


    def _secure_hardware(self, run_clients: list[bytes]) -> None:
        """
        Put the hardware in a safe state before the snapshots are restored.

        RC19=0 for every client of the run, also the ones excluded early, whose RC19 is
        uncertain. Then the trigger registers are cleared for the clients that were
        configured. The clear comes BEFORE the restore: it writes RC15/16/18, so after
        the restore it would erase the original values. A failure is logged and does not
        change the outcome: the restore writes the registers again.
        """

        for cid in run_clients:
            name = cid.decode(errors="ignore")

            try:
                if not self.acquisition_service.disable_rc_channels(client_ids=[cid]):
                    self.logger.error(f"Could not disable RC19 for client {name}: its channels may still be enabled")
            except Exception:
                self.logger.exception(f"Disabling RC19 raised an exception for client {name}")

        trg_clients_to_reset = list(self._trigger_clients_to_reset)

        for cid in trg_clients_to_reset:
            name = cid.decode(errors="ignore")

            try:
                if not self.acquisition_service.configure_acquisition_end_client(client_id=cid):
                    self.logger.error(f"Could not clear the trigger configuration for client {name}")
            except Exception:
                self.logger.exception(f"Clearing the trigger configuration raised an exception for client {name}")


    def _restore_clients(self, run_clients: list[bytes]) -> None:
        """
        Restore the hardware of every client of the run, one at a time.

        restore_hardware_snapshot records the result itself in the session, so nothing is
        recorded here on success. If the restore fails without recording (exception, or
        refused before running) the failure is recorded here: a session left without
        result would stay in FINALIZING. The stop never skips this step.
        """

        restore_power_state = self.server_state.get_mode() != "test"

        for cid in run_clients:
            name = cid.decode(errors="ignore")

            try:
                session = self.calibration_service.get_session(client_id=cid)
            except Exception:
                self.logger.exception(f"Could not read the calibration session of client {name}: restore skipped")
                continue

            if session is None:
                self.logger.error(f"No calibration session for client {name}: restore skipped")
                continue

            if session.hardware_snapshot is None:
                try:
                    if self.calibration_service.mark_restore_not_required(client_id=cid):
                        self.logger.info(f"Client {name}: no hardware snapshot, restore not required")
                    else:
                        self.logger.error(f"Could not mark the restore as not required for client {name}")
                except Exception:
                    self.logger.exception(f"Marking the restore as not required raised an exception for client {name}")
                continue

            restored = False

            try:
                restored = self.calibration_service.restore_hardware_snapshot(client_id=cid, restore_power_state=restore_power_state)

                if restored:
                    self.logger.info(f"Hardware restored for client {name}")
                else:
                    self.logger.error(f"Hardware restore failed for client {name}")
            except Exception:
                self.logger.exception(f"Hardware restore raised an exception for client {name}")

            if restored:
                continue

            # The session passed to us is a copy: read it again to know if the result was recorded.
            self._record_restore_failure_if_missing(cid, session.hardware_snapshot.hv_parameters)

    def _record_restore_failure_if_missing(self, client_id: bytes, hv_parameters: dict) -> None:
        name = client_id.decode(errors="ignore")

        try:
            current = self.calibration_service.get_session(client_id=client_id)

            if current is not None and current.restore_succeeded is not None:
                return

            # No detail on the channels: any of them may be wrong.
            if not self.calibration_service.mark_restore_result(client_id=client_id, succeeded=False, failed_channels=sorted(hv_parameters)):
                self.logger.error(f"Could not record the restore failure for client {name}")
        except Exception:
            self.logger.exception(f"Recording the restore failure raised an exception for client {name}")

    def _close_run(self, run_clients: list[bytes], client_run_folders: dict[bytes, Path], scan_loop_outcome: CalibrationLoopOutcome) -> tuple[CalibrationRunOutcome | None, list[bytes], list[bytes], bool]:
        """
        After the restore: finish the run, write the manifests, close the acquisition
        session. Returns (run outcome, restored clients, clients not restored, restore
        success). The run outcome is None if finish_run refused: in that case every
        client that was not restored is reported as failed, since the result is unknown.
        """

        outcome = None

        try:
            outcome = self.calibration_service.finish_run()
        except Exception:
            self.logger.exception("finish_run raised an exception")

        if outcome is None:
            self.logger.error("Calibration run could not be finished: the run is left in the service")

        # After finish_run (the outcome is in the run) and before clear_run (which erases it).
        try:
            if not self._write_calibration_manifests(client_run_folders):
                self.logger.error("Calibration manifests were not written for every client")
        except Exception:
            self.logger.exception("Writing the calibration manifests raised an exception")

        try:
            restored = list(self.calibration_service.get_restored_clients())
            failed = list(self.calibration_service.get_failed_restore_clients())
        except Exception:
            self.logger.exception("Could not read the restore results: every client is reported as not restored")
            restored = []
            failed = list(run_clients)

        if outcome is None:
            failed = [cid for cid in run_clients if cid not in restored]
        else:
            # A client with no restore result at all cannot be called restored.
            failed = failed + [cid for cid in run_clients if cid not in restored and cid not in failed]

        restore_success = outcome is not None and not failed

        reason = f"Calibration finalization ({scan_loop_outcome.value}, run outcome {outcome.value if outcome is not None else 'unknown'})"

        try:
            self.acquisition_service.close_session(success=restore_success, reason=reason)
        except Exception:
            self.logger.exception("Closing the acquisition session raised an exception")

        return outcome, restored, failed, restore_success

    def _publish_finalization_result(self, restored: list[bytes], failed: list[bytes], restore_success: bool, reason: str) -> bool:
        """
        Release the server from CALIBRATION_FINALIZING with SUCCEEDED or FAILED.
        True if the event was accepted. The event is sent only if the server is still
        CALIBRATION_FINALIZING, and only with clients that are still operational:
        the FSM rejects the whole event otherwise.
        """

        try:
            with self._lifecycle_lock:
                state = self.server_state.get_server_state()

                if state != ServerFSM.CALIBRATION_FINALIZING:
                    self.logger.warning(f"No finalization result sent: server is {state.value}")
                    return False

                operational = set(self.server_state.get_operational_clients())
                restored_clients = [cid for cid in restored if cid in operational]
                failed_clients = [cid for cid in failed if cid in operational]

                event = ServerFSMEvent.CALIBRATION_FINALIZATION_SUCCEEDED if restore_success else ServerFSMEvent.CALIBRATION_FINALIZATION_FAILED

                accepted = self.server_state.process_event(
                    event=event,
                    reason=reason,
                    source="calibration_engine",
                    metadata={"restored_clients": restored_clients, "failed_restore_clients": failed_clients},
                )

                if not accepted:
                    self.logger.error(f"ServerState rejected {event.value}: the server may stay in CALIBRATION_FINALIZING")

                return bool(accepted)
        except Exception:
            self.logger.exception("Could not send the calibration finalization result")
            return False

    def _finalize(self, scan_loop_outcome: CalibrationLoopOutcome, finalization_target: ServerFSM, client_run_folders: dict[bytes, Path]) -> None:
        """
        Close the calibration: FSM to finalizing, outcome of every client, safe hardware,
        restore, run outcome, manifests, final FSM event. Each step handles its own
        errors, so a failure never skips the following ones, above all the last event.
        """

        run_clients: list[bytes] = []

        try:
            run_clients = list(self.calibration_service.get_sessions())
            self.logger.info(f"Calibration finalization started: loop outcome {scan_loop_outcome.value}, {len(run_clients)} clients")

            self._enter_calibration_finalization(scan_loop_outcome, finalization_target)
            self._resolve_unfinished_clients(scan_loop_outcome)

            if not self.calibration_service.begin_finalization():
                self._finalize_without_run(run_clients, "begin_finalization refused")
                return

            self._secure_hardware(run_clients)
            self._restore_clients(run_clients)

            outcome, restored, failed, restore_success = self._close_run(run_clients, client_run_folders, scan_loop_outcome)

            reason = f"Calibration finalization completed, run outcome {outcome.value if outcome is not None else 'unknown'}"

            if not restore_success:
                reason = f"Calibration finalization with errors, run outcome {outcome.value if outcome is not None else 'unknown'}: hardware not restored for {len(failed)} clients"

            accepted = self._publish_finalization_result(restored, failed, restore_success, reason)

            if outcome is not None and accepted:
                if not self.calibration_service.clear_run():
                    self.logger.error("Could not clear the calibration run")

            self.logger.info(f"Calibration finalization finished: run outcome {outcome.value if outcome is not None else 'unknown'}, restore {'ok' if restore_success else 'with errors'}")

        except Exception:
            self.logger.exception("Calibration finalization raised an unexpected exception")

            # Last resort: do not leave the server in CALIBRATION_FINALIZING (no effect if already released).
            self._publish_finalization_result([], run_clients, False, "Calibration finalization raised an unexpected exception")
        finally:
            with self._lifecycle_lock:
                self._trigger_clients_to_reset = []
                self._finalization_target = None

    def _stop_requested(self) -> bool:
        return self._calibration_run_stop.is_set()

    def stop_requested(self) -> bool:
        """True if a stop was requested for the calibration in progress. For the callbacks
        of the calibrations (prepare_run, ...), which run in the thread of the worker."""
        return self._calibration_run_stop.is_set()

    
