from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable
import threading
import copy

from server.services.client_command_service import ClientCommandService, CommandPlane
from server.utils.logger import get_logger


class CalibrationMode(str, Enum):
    FAST = "fast"
    SAFE = "safe"


class CalibrationOutcome(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


class CalibrationRunOutcome(str, Enum):
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    ABORTED = "aborted"


class CalibrationStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    FINALIZING = "finalizing"
    FINISHED = "finished"


@dataclass(frozen=True)
class HardwareSnapshotSpec:
    rc_register: tuple[int, ...] = ()
    hv_voltage: bool = False
    hv_threshold: bool = False
    hv_power_state: bool = False


@dataclass
class ClientHardwareSnapshot:
    rc_registers: dict[int, int] = field(default_factory=dict)
    hv_parameters: dict[int, dict[str, Any]] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScanPoint:
    index: int
    parameter_name: str
    value: int | float | str
    suffix: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClientCalibrationSession:
    client_id: bytes
    requested_channels: list[int]

    effective_channels: list[int] = field(default_factory=list)

    status: CalibrationStatus = CalibrationStatus.CREATED
    outcome: CalibrationOutcome | None = None

    hardware_snapshot: ClientHardwareSnapshot | None = None

    requested_points: list[ScanPoint] = field(default_factory=list)
    completed_points: list[ScanPoint] = field(default_factory=list)
    current_point: ScanPoint | None = None

    restore_succeeded: bool | None = None
    error: str | None = None


@dataclass
class CalibrationRun:
    calibration_type: str
    start_time: datetime
    execution_mode: CalibrationMode

    status: CalibrationStatus = CalibrationStatus.CREATED
    outcome: CalibrationRunOutcome | None = None


@dataclass(frozen=True)
class CalibrationScanSpec:
    calibration_type: str
    points: tuple[ScanPoint, ...]
    snapshot_spec: HardwareSnapshotSpec
    apply_point: Callable[[bytes, ScanPoint], bool]


class CalibrationService:

    def __init__(self, command_service: ClientCommandService):

        self.command_service = command_service

        self._lock = threading.RLock()

        self.current_run: CalibrationRun | None = None
        self._sessions: dict[bytes, ClientCalibrationSession] = {}

        self.logger = get_logger("calibration_service")
        self.logger.debug("Calibration Service initialized")


    def _create_calibration_run(
        self,
        *,
        calibration_type: str,
        execution_mode: CalibrationMode,
    ) -> CalibrationRun | None:

        if not isinstance(calibration_type, str) or not calibration_type.strip():
            self.logger.error(
                "Cannot create calibration run: calibration_type must be non-empty"
            )
            return None

        if not isinstance(execution_mode, CalibrationMode):
            self.logger.error(
                f"Cannot create calibration run: invalid execution mode {execution_mode!r}"
            )
            return None

        if self.current_run is not None:
            self.logger.warning(
                "Cannot create calibration run: another calibration run already exists "
                f"(type={self.current_run.calibration_type}, "
                f"status={self.current_run.status.value})"
            )
            return None

        run = CalibrationRun(
            calibration_type=calibration_type.strip(),
            start_time=datetime.now(timezone.utc),
            execution_mode=execution_mode,
            status=CalibrationStatus.CREATED,
        )

        self.current_run = run

        self.logger.info(
            "Calibration run created: "
            f"type={run.calibration_type}, "
            f"mode={run.execution_mode.value}"
        )

        return run
            
    
    def _require_session_locked(
        self,
        client_id: bytes,
    ) -> ClientCalibrationSession:

        session = self._sessions.get(client_id)

        if session is None:
            self.logger.error(
                f"Calibration session not found for client {client_id!r}"
            )
            raise KeyError(
                f"No calibration session exists for client {client_id!r}"
            )

        return session


    def _reset_run_locked(self,):
        self.current_run = None
        self._sessions.clear()
        self.logger.debug("Calibration run state reset")


    def _normalize_channels(
        self,
        channels: list[int],
    ) -> list[int] | None:

        if not channels:
            self.logger.error(
                "Calibration channel list must not be empty"
            )
            return None

        normalized: list[int] = []

        for channel in channels:

            if not isinstance(channel, int):
                self.logger.error(
                    f"Invalid calibration channel {channel!r}: expected int"
                )
                return None

            if channel < 0 or channel >= 7:
                self.logger.error(
                    f"Invalid calibration channel {channel}: "
                    "expected range 0..6"
                )
                return None

            if channel not in normalized:
                normalized.append(channel)

        return normalized


    def _normalize_scan_points(
        self,
        points: list[ScanPoint],
    ) -> list[ScanPoint] | None:

        if not points:
            self.logger.error(
                "Calibration scan must contain at least one point"
            )
            return None

        normalized = []

        seen_indexes = set()
        seen_suffixes = set()

        for point in points:

            if not isinstance(point, ScanPoint):
                self.logger.error(
                    f"Invalid scan point object: {point!r}"
                )
                return None

            if point.index < 0:
                self.logger.error(
                    f"Invalid scan point index: {point.index}"
                )
                return None

            if (
                not isinstance(point.parameter_name, str)
                or not point.parameter_name.strip()
            ):
                self.logger.error(
                    f"Invalid parameter name for scan point "
                    f"{point.index}"
                )
                return None

            if (
                not isinstance(point.suffix, str)
                or not point.suffix.strip()
            ):
                self.logger.error(
                    f"Invalid suffix for scan point "
                    f"{point.index}"
                )
                return None

            if point.index in seen_indexes:
                self.logger.error(
                    f"Duplicate scan point index: "
                    f"{point.index}"
                )
                return None

            if point.suffix in seen_suffixes:
                self.logger.error(
                    f"Duplicate scan point suffix: "
                    f"{point.suffix}"
                )
                return None

            seen_indexes.add(
                point.index
            )

            seen_suffixes.add(
                point.suffix
            )

            normalized.append(
                point
            )

        return sorted(
            normalized,
            key=lambda point: point.index,
        )

    def _derive_run_outcome_locked(self) -> CalibrationRunOutcome:

        outcomes = [
            session.outcome
            for session in self._sessions.values()
        ]

        if not outcomes:
            return CalibrationRunOutcome.FAILED

        if any(
            outcome == CalibrationOutcome.ABORTED
            for outcome in outcomes
        ):
            return CalibrationRunOutcome.ABORTED

        if all(
            outcome == CalibrationOutcome.COMPLETED
            for outcome in outcomes
        ):
            return CalibrationRunOutcome.COMPLETED

        if all(
            outcome == CalibrationOutcome.FAILED
            for outcome in outcomes
        ):
            return CalibrationRunOutcome.FAILED

        return CalibrationRunOutcome.PARTIAL


    def start_run(
        self,
        *,
        calibration_type: str,
        execution_mode: CalibrationMode,
        targets: dict[bytes, list[int]],
        requested_points: dict[bytes, list[ScanPoint]],
    ) -> CalibrationRun | None:

        if not targets:
            self.logger.error(
                "Cannot start calibration run: no calibration targets provided"
            )
            return None

        with self._lock:

            if self.current_run is not None:
                self.logger.warning(
                    "Cannot start calibration run: another calibration run "
                    f"is already active (type={self.current_run.calibration_type}, "
                    f"status={self.current_run.status.value})"
                )
                return None

            invalid_point_clients = set(requested_points) - set(targets)

            if invalid_point_clients:
                self.logger.error(
                    "Cannot start calibration run: scan points were provided "
                    "for clients that are not calibration targets: "
                    f"{invalid_point_clients!r}"
                )
                return None

            sessions: dict[bytes, ClientCalibrationSession] = {}

            for client_id, channels in targets.items():

                if not isinstance(client_id, bytes) or not client_id:
                    self.logger.error(
                        f"Invalid calibration client ID: {client_id!r}"
                    )
                    return None

                normalized_channels = self._normalize_channels(channels)

                if normalized_channels is None:
                    self.logger.error(
                        f"Cannot create calibration session for {client_id!r}: "
                        "invalid channel selection"
                    )
                    return None

                points = self._normalize_scan_points(requested_points.get(client_id, []))

                if points is None:
                    self.logger.error(
                        f"Cannot create calibration session "
                        f"for {client_id!r}: invalid scan points"
                    )
                    return None

                sessions[client_id] = ClientCalibrationSession(
                    client_id=client_id,
                    requested_channels=normalized_channels,
                    status=CalibrationStatus.CREATED,
                    requested_points=points
                )

            run = self._create_calibration_run(
                calibration_type=calibration_type,
                execution_mode=execution_mode,
            )

            if run is None:
                return None

            self._sessions = sessions

            run.status = CalibrationStatus.RUNNING

            for session in self._sessions.values():
                session.status = CalibrationStatus.RUNNING

            self.logger.info(
                "Calibration run started: "
                f"type={run.calibration_type}, "
                f"mode={run.execution_mode.value}, "
                f"clients={len(self._sessions)}"
            )

            for client_id, session in self._sessions.items():
                self.logger.debug(
                    "Calibration session started: "
                    f"client={client_id!r}, "
                    f"channels={session.requested_channels}, "
                    f"points={len(session.requested_points)}"
                )

            return copy.deepcopy(run)    


    def get_run(self) -> CalibrationRun | None:

        with self._lock:

            if self.current_run is None:
                return None

            return copy.deepcopy(self.current_run)


    def get_session(
        self,
        client_id: bytes,
    ) -> ClientCalibrationSession | None:

        with self._lock:

            session = self._sessions.get(client_id)

            if session is None:
                return None

            return copy.deepcopy(session) 

    def get_sessions(self)-> dict[bytes, ClientCalibrationSession]:
        with self._lock:
            return copy.deepcopy(self._sessions)


    def get_active_clients(self) -> list[bytes]:

        with self._lock:
            return [
                client_id
                for client_id, session in self._sessions.items()
                if session.status == CalibrationStatus.RUNNING
            ]


    def get_failed_clients(self) -> list[bytes]:

        with self._lock:
            return [
                client_id
                for client_id, session in self._sessions.items()
                if session.outcome == CalibrationOutcome.FAILED
            ]


    def get_completed_clients(self) -> list[bytes]:

        with self._lock:
            return [
                client_id
                for client_id, session in self._sessions.items()
                if session.outcome == CalibrationOutcome.COMPLETED
            ]


    def get_aborted_clients(self) -> list[bytes]:

        with self._lock:
            return [
                client_id
                for client_id, session in self._sessions.items()
                if session.outcome == CalibrationOutcome.ABORTED
            ]


    def get_restored_clients(self) -> list[bytes]:

        with self._lock:
            return [
                client_id
                for client_id, session in self._sessions.items()
                if session.restore_succeeded is True
            ]


    def get_failed_restore_clients(self) -> list[bytes]:

        with self._lock:
            return [
                client_id
                for client_id, session in self._sessions.items()
                if session.restore_succeeded is False
            ]




    def set_current_point(self, client_id: bytes, point: ScanPoint) -> bool:

        with self._lock:
            if self.current_run is None or self.current_run.status != CalibrationStatus.RUNNING:
                return False

        
            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot set scan point: no calibration session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.RUNNING:
                self.logger.warning(
                    f"Cannot set scan point for {client_id!r}: "
                    f"session is {session.status.value}"
                )
                return False

            if point not in session.requested_points:
                self.logger.error(
                    f"Cannot set scan point for {client_id!r}: "
                    f"point {point!r} was not requested"
                )
                return False

            if session.current_point is not None:
                self.logger.warning(
                    f"Cannot set scan point for {client_id!r}: "
                    f"point {session.current_point.index} is still active"
                )
                return False
            

            session.current_point = point

            self.logger.debug(
                f"Calibration point started: client={client_id!r}, "
                f"index={point.index}, parameter={point.parameter_name}, "
                f"value={point.value}"
            )

            return True



    def mark_point_completed(self, client_id: bytes):

         with self._lock:
            if self.current_run is None or self.current_run.status != CalibrationStatus.RUNNING:
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot complete scan point: no calibration session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.RUNNING:
                return False

            if session.current_point is None:
                self.logger.warning(
                    f"Cannot complete scan point for {client_id!r}: "
                    "no point is currently active"
                )
                return False
            
            completed_point = session.current_point

            if completed_point not in session.completed_points:
                session.completed_points.append(completed_point)

            session.current_point = None

            self.logger.debug(
                f"Calibration point completed: client={client_id!r}, "
                f"index={completed_point.index}, "
                f"parameter={completed_point.parameter_name}, "
                f"value={completed_point.value}"
            )

            return True

    def mark_client_completed(self, client_id: bytes):

        with self._lock:
            if self.current_run is None or self.current_run.status != CalibrationStatus.RUNNING:
                return False


            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot complete calibration client: "
                    f"no session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.RUNNING:
                return False

            if session.current_point is not None:
                self.logger.warning(
                    f"Cannot complete calibration for {client_id!r}: "
                    "a scan point is still active"
                )
                return False

            missing_points = [point for point in session.requested_points if point not in session.completed_points]

            if missing_points:
                self.logger.warning(
                    f"Cannot complete calibration for {client_id!r}: "
                    f"{len(missing_points)} scan points are still incomplete"
                )
                return False

            session.outcome = CalibrationOutcome.COMPLETED
            session.status = CalibrationStatus.FINALIZING

            self.logger.info(
                f"Calibration completed for client {client_id!r}; "
                "waiting for hardware restore"
            )

            return True

    def mark_client_failed(
        self,
        client_id: bytes,
        *,
        error: str,
    ) -> bool:

        with self._lock:

            if (
                self.current_run is None
                or self.current_run.status != CalibrationStatus.RUNNING
            ):
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot mark calibration client failed: "
                    f"no session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.RUNNING:
                return False

            session.current_point = None
            session.outcome = CalibrationOutcome.FAILED
            session.error = str(error)
            session.status = CalibrationStatus.FINALIZING

            self.logger.warning(
                f"Calibration failed for client {client_id!r}: {error}. "
                "Client removed from subsequent scan points and awaiting restore."
            )

            return True

    def mark_client_aborted(
        self,
        client_id: bytes,
    ) -> bool:

        with self._lock:

            if (
                self.current_run is None
                or self.current_run.status != CalibrationStatus.RUNNING
            ):
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot abort calibration client: "
                    f"no session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.RUNNING:
                return False

            session.current_point = None
            session.outcome = CalibrationOutcome.ABORTED
            session.status = CalibrationStatus.FINALIZING

            self.logger.info(
                f"Calibration aborted for client {client_id!r}; "
                "waiting for hardware restore"
            )

            return True


    def begin_finalization(self) -> bool:
        with self._lock:

            if self.current_run is None or self.current_run.status != CalibrationStatus.RUNNING:
                return False

            self.current_run.status = CalibrationStatus.FINALIZING

            for session in self._sessions.values():
                if session.status == CalibrationStatus.RUNNING:
                    session.status = CalibrationStatus.FINALIZING

            self.logger.info(
                f"Calibration run entering finalization: "
                f"type={self.current_run.calibration_type}"
            )

            return True


    def set_hardware_snapshot(self, client_id: bytes, snapshot: ClientHardwareSnapshot) -> bool:

        with self._lock:
            if self.current_run is None:
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot set hardware snapshot: "
                    f"no calibration session for {client_id!r}"
                )
                return False

            session.hardware_snapshot = copy.deepcopy(snapshot)

            self.logger.debug(
                f"Hardware snapshot stored for calibration client {client_id!r}"
            )

            return True


    def mark_restore_result(self, client_id, succeeded: bool) -> bool:

        with self._lock:
            if self.current_run is None or self.current_run.status != CalibrationStatus.FINALIZING:
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot record restore result: "
                    f"no calibration session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.FINALIZING:
                self.logger.warning(
                    f"Cannot record restore result for {client_id!r}: "
                    f"session is {session.status.value}"
                )
                return False

            session.restore_succeeded = bool(succeeded)
            session.status = CalibrationStatus.FINISHED

            if succeeded:
                self.logger.info(
                    f"Hardware restore succeeded for calibration client {client_id!r}"
                )
            else:
                self.logger.warning(
                    f"Hardware restore failed for calibration client {client_id!r}"
                )

            return True


    def finish_run(self) -> CalibrationRunOutcome | None:

        with self._lock:

            if self.current_run is None:
                self.logger.error(
                    "Cannot finish calibration run: no run is active"
                )
                return None

            if self.current_run.status != CalibrationStatus.FINALIZING:
                self.logger.warning(
                    "Cannot finish calibration run: "
                    f"run is {self.current_run.status.value}, "
                    "expected finalizing"
                )
                return None

            unfinished_clients = [
                client_id
                for client_id, session in self._sessions.items()
                if session.status != CalibrationStatus.FINISHED
            ]

            if unfinished_clients:
                self.logger.warning(
                    "Cannot finish calibration run: "
                    f"{len(unfinished_clients)} client sessions are not finished: "
                    f"{unfinished_clients!r}"
                )
                return None

            clients_without_outcome = [
                client_id
                for client_id, session in self._sessions.items()
                if session.outcome is None
            ]

            if clients_without_outcome:
                self.logger.error(
                    "Cannot finish calibration run: "
                    "client sessions without calibration outcome: "
                    f"{clients_without_outcome!r}"
                )
                return None

            outcome = self._derive_run_outcome_locked()

            self.current_run.outcome = outcome
            self.current_run.status = CalibrationStatus.FINISHED

            restored_clients = [
                client_id
                for client_id, session in self._sessions.items()
                if session.restore_succeeded is True
            ]

            failed_restore_clients = [
                client_id
                for client_id, session in self._sessions.items()
                if session.restore_succeeded is False
            ]

            self.logger.info(
                "Calibration run finished: "
                f"type={self.current_run.calibration_type}, "
                f"outcome={outcome.value}, "
                f"restored_clients={len(restored_clients)}, "
                f"failed_restore_clients={len(failed_restore_clients)}"
            )

            return outcome



    def clear_run(self) -> bool:
        with self._lock:

            if self.current_run is None:
                return True

            if self.current_run.status != CalibrationStatus.FINISHED:
                self.logger.warning(
                    "Cannot clear calibration run: "
                    f"run is still {self.current_run.status.value}"
                )
                return False

            calibration_type = self.current_run.calibration_type

            self._reset_run_locked()

            self.logger.info(
                f"Calibration run cleared: type={calibration_type}"
            )

            return True


    def capture_hardware_snapshot(self, client_id: bytes, spec: HardwareSnapshotSpec) -> bool:

        with self._lock:

            if self.current_run is None:
                return False

            if self.current_run.status != CalibrationStatus.RUNNING:
                self.logger.warning(
                    "Cannot capture and set an hardware snapshot: "
                    f"run is still {self.current_run.status.value}"
                )
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot set hardware snapshot: "
                    f"no calibration session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.RUNNING:
                self.logger.warning(
                    f"Cannot set hardware snapshot for {client_id!r}: "
                    f"session is {session.status.value}"
                )
                return False

            if session.hardware_snapshot is not None:
                self.logger.warning(
                    f"Hardware snapshot already exists for {client_id!r}"
                )
                return False

            requested_channels = session.requested_channels

        rc_registers: dict[int, int] = {}
        hv_parameters: dict[int, dict[str, Any]] = {channel: {} for channel in requested_channels}

        # RC snapshot
        for register in spec.rc_register:

            value = self.command_service.read_rc_register(
                client_id=client_id,
                address=int(register),
                plane=CommandPlane.CONTROL,
                timeout_s=15.0,
            )

            if value is None:
                self.logger.error(
                    f"Hardware snapshot failed for {client_id!r}: "
                    f"cannot read RC register {register}"
                )
                return False

            rc_registers[int(register)] = value

        # HV voltage + threshold
        if spec.hv_voltage and spec.hv_threshold:

            configuration = (
                self.command_service.get_hv_volt_thr_configuration(
                    client_id=client_id,
                    requested_channels=requested_channels,
                    plane=CommandPlane.CONTROL,
                    timeout_s=120.0,
                )
            )

            if configuration is None:
                self.logger.error(
                    f"Hardware snapshot failed for {client_id!r}: "
                    "cannot read HV voltage/threshold configuration"
                )
                return False

            for channel in requested_channels:

                values = configuration.get(channel)

                if values is None:
                    self.logger.error(
                        f"Hardware snapshot failed for {client_id!r}: "
                        f"missing HV configuration for channel {channel}"
                    )
                    return False

                hv_parameters[channel]["voltage"] = values.get("voltage")
                hv_parameters[channel]["threshold"] = values.get("threshold")

        elif spec.hv_voltage:

            configuration = (
                self.command_service.get_hv_voltage_configuration(
                    client_id=client_id,
                    requested_channels=requested_channels,
                    plane=CommandPlane.CONTROL,
                    timeout_s=60.0,
                )
            )

            if configuration is None:
                return False

            for channel in requested_channels:

                if channel not in configuration:
                    self.logger.error(
                        f"Hardware snapshot failed for {client_id!r}: "
                        f"missing voltage for channel {channel}"
                    )
                    return False

                hv_parameters[channel]["voltage"] = configuration[channel]

        elif spec.hv_threshold:

            configuration = (
                self.command_service.get_hv_threshold_configuration(
                    client_id=client_id,
                    requested_channels=requested_channels,
                    plane=CommandPlane.CONTROL,
                    timeout_s=60.0,
                )
            )

            if configuration is None:
                return False

            for channel in requested_channels:

                if channel not in configuration:
                    self.logger.error(
                        f"Hardware snapshot failed for {client_id!r}: "
                        f"missing threshold for channel {channel}"
                    )
                    return False

                hv_parameters[channel]["threshold"] = configuration[channel]

        # HV power state
        if spec.hv_power_state:

            power_state = self.command_service.get_hv_power_state(
                client_id=client_id,
                requested_channels=requested_channels,
                plane=CommandPlane.CONTROL,
                timeout_s=60.0,
            )

            if power_state is None:
                return False

            for channel in requested_channels:

                state = power_state.get(channel)

                if state not in {"on", "off"}:
                    self.logger.error(
                        f"Hardware snapshot failed for {client_id!r}: "
                        f"unknown power state for channel {channel}"
                    )
                    return False

                hv_parameters[channel]["power_state"] = state


        hv_parameters = {
            channel: parameters
            for channel, parameters in hv_parameters.items()
            if parameters
        }


        snapshot = ClientHardwareSnapshot(
            rc_registers=rc_registers,
            hv_parameters=hv_parameters,
        )

        if not self.set_hardware_snapshot(
            client_id=client_id,
            snapshot=snapshot,
        ):
            return False

        self.logger.info(
            f"Hardware snapshot captured for calibration client {client_id!r}: "
            f"rc_registers={len(rc_registers)}, "
            f"hv_channels={len(hv_parameters)}"
        )

        return True


    def restore_hardware_snapshot(
        self,
        client_id: bytes,
    ) -> bool:

        with self._lock:

            if self.current_run is None:
                return False

            if self.current_run.status != CalibrationStatus.FINALIZING:
                self.logger.warning(
                    f"Cannot restore hardware for {client_id!r}: "
                    f"run is {self.current_run.status.value}"
                )
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot restore hardware: "
                    f"no calibration session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.FINALIZING:
                self.logger.warning(
                    f"Cannot restore hardware for {client_id!r}: "
                    f"session is {session.status.value}"
                )
                return False

            if session.hardware_snapshot is None:
                self.logger.error(
                    f"Cannot restore hardware for {client_id!r}: "
                    "no hardware snapshot is available"
                )
                return False

            snapshot = copy.deepcopy(session.hardware_snapshot)


        restore_ok = True


        #
        # Restore HV Configuration
        #
        if snapshot.hv_parameters:

            try:
                hv_ok = self.command_service.restore_hv_configuration(
                    client_id=client_id,
                    configuration=snapshot.hv_parameters,
                    plane=CommandPlane.CONTROL,
                    timeout_s=300.0,
                )

                if not hv_ok:
                    restore_ok = False

            except Exception:
                self.logger.exception(
                    f"Exception while restoring HV configuration "
                    f"for client {client_id!r}"
                )
                restore_ok = False


        #
        # Restore RC registers.
        #

        for register, value in snapshot.rc_registers.items():

            try:
                success = self.command_service.write_rc_register(
                    client_id=client_id,
                    address=register,
                    value=value,
                    plane=CommandPlane.CONTROL,
                    timeout_s=15.0,
                )

                if not success:
                    self.logger.warning(
                        f"Failed to restore RC register {register} "
                        f"for client {client_id!r}"
                    )
                    restore_ok = False

            except Exception:
                self.logger.exception(
                    f"Exception while restoring RC register "
                    f"{register} for client {client_id!r}"
                )
                restore_ok = False

        

        

        #
        # Record restore result in CalibrationService.
        #
        if not self.mark_restore_result(
            client_id=client_id,
            succeeded=restore_ok,
        ):
            self.logger.error(
                f"Failed to record restore result for client {client_id!r}"
            )
            return False

        return restore_ok


    def set_effective_channels(
        self,
        client_id: bytes,
        channels: list[int],
    ) -> bool:

        with self._lock:

            if (
                self.current_run is None
                or self.current_run.status != CalibrationStatus.RUNNING
            ):
                return False

            session = self._sessions.get(client_id)

            if session is None:
                self.logger.error(
                    f"Cannot set effective calibration channels: "
                    f"no session for {client_id!r}"
                )
                return False

            if session.status != CalibrationStatus.RUNNING:
                self.logger.warning(
                    f"Cannot set effective channels for {client_id!r}: "
                    f"session is {session.status.value}"
                )
                return False

            normalized = []

            for channel in channels:

                if not isinstance(channel, int):
                    self.logger.error(
                        f"Invalid effective channel {channel!r} "
                        f"for client {client_id!r}"
                    )
                    return False

                if channel not in session.requested_channels:
                    self.logger.error(
                        f"Cannot use channel {channel} for client {client_id!r}: "
                        "channel was not requested"
                    )
                    return False

                if channel not in normalized:
                    normalized.append(channel)

            session.effective_channels = sorted(
                normalized
            )

            self.logger.debug(
                f"Calibration effective channels updated: "
                f"client={client_id!r}, "
                f"channels={session.effective_channels}"
            )

            return True


    def get_effective_channels(
        self,
        client_id: bytes,
    ) -> list[int] | None:

        with self._lock:

            session = self._sessions.get(client_id)

            if session is None:
                return None

            return list(
                session.effective_channels
            )


    def clear_effective_channels(
        self,
        client_id: bytes,
    ) -> bool:

        return self.set_effective_channels(
            client_id=client_id,
            channels=[],
        )
        
    
    def discard_unmodified_run(self) -> bool:
        """
        Drop a calibration run that was created locally but never entered
        the hardware execution phase.

        Intended for rollback when CALIBRATION_STARTED is rejected by
        ServerState.
        """
        with self._lock:
            if self.current_run is None:
                return True
            
            if self.current_run.status != CalibrationStatus.RUNNING:
                self.logger.warning(
                    "Cannot discard calibration run: "
                    f"run is {self.current_run.status.value}"
                )
                return False
            
            for session in self._sessions.values():
                if session.hardware_snapshot is not None:
                    self.logger.error(
                        "Cannot discard calibration run: "
                        "hardware snapshot already exists"
                    )
                    return False
                
                if session.current_point is not None:
                    self.logger.error(
                        "Cannot discard calibration run: "
                        "a scan point is already active"
                    )
                    return False
                
                if session.completed_points:
                    self.logger.error(
                        "Cannot discard calibration run: "
                        "scan points were already completed"
                    )
                    return False
                
                if session.effective_channels:
                    self.logger.error(
                        "Cannot discard calibration run: "
                        "hardware preparation already occurred"
                    )
                    return False
            
            
            calibration_type = self.current_run.calibration_type
            self._reset_run_locked()
            self.logger.info(
                "Unmodified calibration run discarded: "
                f"type={calibration_type}"
            )

            return True
    
    def mark_restore_not_required(self, client_id: bytes) -> bool:
        with self._lock:
            if self.current_run is None or self.current_run.status != CalibrationStatus.FINALIZING:
                return False
            
            session = self._sessions.get(client_id)
            
            if session is None:
                self.logger.error(
                    f"Cannot mark restore not required: "
                    f"no session for {client_id!r}"
                )
                return False
            
            if session.status != CalibrationStatus.FINALIZING:
                self.logger.warning(
                    f"Cannot mark restore not required for {client_id!r}: "
                    f"session is {session.status.value}"
                )
                return False
            
            if session.hardware_snapshot is not None:
                self.logger.error(
                    f"Cannot skip restore for {client_id!r}: "
                    "hardware snapshot exists"
                )
                return False
            
            session.restore_succeeded = True
            session.status = CalibrationStatus.FINISHED
            
            self.logger.info(
                f"No hardware restore required for calibration "
                f"client {client_id!r}"
            )

            return True
            
            
            





