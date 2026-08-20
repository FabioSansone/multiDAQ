from __future__ import annotations

import threading
from datetime import datetime, timezone
from enum import Enum
from functools import wraps
from typing import Iterable, List, Optional
from dataclasses import dataclass, field

from server.utils.logger import get_logger


class ServerFSM(str, Enum):
    DISCONNECTED = "disconnected"
    CONTROL_CONNECTED = "control_connected"
    CONNECTED = "connected"
    CONFIGURING = "configuring"
    READY = "ready"
    ACQUIRING = "acquiring"
    FINALIZING = "finalizing"
    ERROR = "error"

class AcquisitionMode(str, Enum):
    TEST = "test"
    CALIBRATION = "calibration"
    MULTIPMT = "multipmt"

class ServerFSMEvent(str, Enum):
    CONTROL_CONNECTION_SUCCEEDED = "control_connection_succeeded"
    CONTROL_CONNECTION_FAILED = "control_connection_failed"
    CONTROL_CONNECTION_LOST = "control_connection_lost"

    ACQUISITION_CONNECTION_SUCCEEDED = "acquisition_connection_succeeded"
    ACQUISITION_CONNECTION_FAILED = "acquisition_connection_failed"
    ACQUISITION_CONNECTION_LOST = "acquisition_connection_lost"

    DISCONNECT_REQUESTED = "disconnect_requested"

    CONFIGURATION_STARTED = "configuration_started"
    CONFIGURATION_SUCCEEDED = "configuration_succeeded"
    CONFIGURATION_FAILED = "configuration_failed"

    ACQUISITION_STARTED = "acquisition_started"
    STOP_REQUESTED = "stop_requested"
    RECEIVER_COMPLETED = "receiver_completed"

    FINALIZATION_SUCCEEDED = "finalization_succeeded"
    FINALIZATION_FAILED = "finalization_failed"

    FATAL_ERROR = "fatal_error"

    RECOVERY_STARTED = "recovery_started"
    RECOVERY_SUCCEEDED = "recovery_succeeded"
    RECOVERY_FAILED = "recovery_failed"


class ClientFSM(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTED = "connected"
    CONFIGURING = "configuring"
    READY = "ready"
    ACQUIRING = "acquiring"
    FINALIZING = "finalizing"
    ERROR = "error"


class ClientFSMEvent(str, Enum):
    CONNECT_SUCCEEDED = "connect_succeeded"
    DISCONNECTED = "disconnected"

    CONFIGURATION_STARTED = "configuration_started"
    CONFIGURATION_SUCCEEDED = "configuration_succeeded"
    CONFIGURATION_FAILED = "configuration_failed"

    ACQUISITION_STARTED = "acquisition_started"
    STOP_REQUESTED = "stop_requested"
    RECEIVER_COMPLETED = "receiver_completed"

    FINALIZATION_SUCCEEDED = "finalization_succeeded"
    FINALIZATION_FAILED = "finalization_failed"

    FATAL_ERROR = "fatal_error"


TRANSITION_TABLE: dict[tuple[ServerFSM, ServerFSMEvent], ServerFSM] = {
    # No plane connected.
    (ServerFSM.DISCONNECTED, ServerFSMEvent.CONTROL_CONNECTION_SUCCEEDED): ServerFSM.CONTROL_CONNECTED,
    (ServerFSM.DISCONNECTED, ServerFSMEvent.CONTROL_CONNECTION_FAILED): ServerFSM.DISCONNECTED,
    (ServerFSM.DISCONNECTED, ServerFSMEvent.ACQUISITION_CONNECTION_FAILED): ServerFSM.DISCONNECTED,
    (ServerFSM.DISCONNECTED, ServerFSMEvent.DISCONNECT_REQUESTED): ServerFSM.DISCONNECTED,
    (ServerFSM.DISCONNECTED, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,

    # Only the Control Plane is available.
    (ServerFSM.CONTROL_CONNECTED, ServerFSMEvent.ACQUISITION_CONNECTION_SUCCEEDED): ServerFSM.CONNECTED,
    (ServerFSM.CONTROL_CONNECTED, ServerFSMEvent.ACQUISITION_CONNECTION_FAILED): ServerFSM.CONTROL_CONNECTED,
    (ServerFSM.CONTROL_CONNECTED, ServerFSMEvent.CONTROL_CONNECTION_LOST): ServerFSM.DISCONNECTED,
    (ServerFSM.CONTROL_CONNECTED, ServerFSMEvent.DISCONNECT_REQUESTED): ServerFSM.DISCONNECTED,
    (ServerFSM.CONTROL_CONNECTED, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,

    # At least one common client is connected on both planes.
    (ServerFSM.CONNECTED, ServerFSMEvent.CONFIGURATION_STARTED): ServerFSM.CONFIGURING,
    (ServerFSM.CONNECTED, ServerFSMEvent.CONTROL_CONNECTION_LOST): ServerFSM.DISCONNECTED,
    (ServerFSM.CONNECTED, ServerFSMEvent.ACQUISITION_CONNECTION_LOST): ServerFSM.CONTROL_CONNECTED,
    (ServerFSM.CONNECTED, ServerFSMEvent.DISCONNECT_REQUESTED): ServerFSM.DISCONNECTED,
    (ServerFSM.CONNECTED, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,

    # Hardware/server initialization is in progress.
    (ServerFSM.CONFIGURING, ServerFSMEvent.CONFIGURATION_SUCCEEDED): ServerFSM.READY,
    (ServerFSM.CONFIGURING, ServerFSMEvent.CONFIGURATION_FAILED): ServerFSM.CONNECTED,
    (ServerFSM.CONFIGURING, ServerFSMEvent.RECOVERY_SUCCEEDED): ServerFSM.READY,
    (ServerFSM.CONFIGURING, ServerFSMEvent.RECOVERY_FAILED): ServerFSM.ERROR,
    (ServerFSM.CONFIGURING, ServerFSMEvent.CONTROL_CONNECTION_LOST): ServerFSM.ERROR,
    (ServerFSM.CONFIGURING, ServerFSMEvent.ACQUISITION_CONNECTION_LOST): ServerFSM.ERROR,
    (ServerFSM.CONFIGURING, ServerFSMEvent.DISCONNECT_REQUESTED): ServerFSM.DISCONNECTED,
    (ServerFSM.CONFIGURING, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,

    # The server is fully initialized and may start an acquisition.
    (ServerFSM.READY, ServerFSMEvent.CONFIGURATION_STARTED): ServerFSM.CONFIGURING,
    (ServerFSM.READY, ServerFSMEvent.ACQUISITION_STARTED): ServerFSM.ACQUIRING,
    (ServerFSM.READY, ServerFSMEvent.CONTROL_CONNECTION_LOST): ServerFSM.DISCONNECTED,
    (ServerFSM.READY, ServerFSMEvent.ACQUISITION_CONNECTION_LOST): ServerFSM.CONTROL_CONNECTED,
    (ServerFSM.READY, ServerFSMEvent.DISCONNECT_REQUESTED): ServerFSM.DISCONNECTED,
    (ServerFSM.READY, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,

    # Data taking is active.
    (ServerFSM.ACQUIRING, ServerFSMEvent.STOP_REQUESTED): ServerFSM.FINALIZING,
    (ServerFSM.ACQUIRING, ServerFSMEvent.RECEIVER_COMPLETED): ServerFSM.FINALIZING,
    (ServerFSM.ACQUIRING, ServerFSMEvent.DISCONNECT_REQUESTED): ServerFSM.FINALIZING,
    (ServerFSM.ACQUIRING, ServerFSMEvent.CONTROL_CONNECTION_LOST): ServerFSM.FINALIZING,
    (ServerFSM.ACQUIRING, ServerFSMEvent.ACQUISITION_CONNECTION_LOST): ServerFSM.FINALIZING,
    (ServerFSM.ACQUIRING, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,

    # The static target is READY. process_event() may replace it with the
    # validated pending terminal state (normally DISCONNECTED).
    (ServerFSM.FINALIZING, ServerFSMEvent.FINALIZATION_SUCCEEDED): ServerFSM.READY,
    (ServerFSM.FINALIZING, ServerFSMEvent.FINALIZATION_FAILED): ServerFSM.ERROR,
    (ServerFSM.FINALIZING, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,

    # Error recovery.
    (ServerFSM.ERROR, ServerFSMEvent.RECOVERY_STARTED): ServerFSM.CONFIGURING,
    (ServerFSM.ERROR, ServerFSMEvent.DISCONNECT_REQUESTED): ServerFSM.DISCONNECTED,
    (ServerFSM.ERROR, ServerFSMEvent.FATAL_ERROR): ServerFSM.ERROR,
}


TRANSITION_TABLE_CLIENT: dict[tuple[ClientFSM, ClientFSMEvent], ClientFSM] = {
    (ClientFSM.DISCONNECTED, ClientFSMEvent.CONNECT_SUCCEEDED): ClientFSM.CONNECTED,

    (ClientFSM.CONNECTED, ClientFSMEvent.CONFIGURATION_STARTED): ClientFSM.CONFIGURING,
    (ClientFSM.CONFIGURING, ClientFSMEvent.CONFIGURATION_SUCCEEDED): ClientFSM.READY,
    (ClientFSM.CONFIGURING, ClientFSMEvent.CONFIGURATION_FAILED): ClientFSM.ERROR,

    (ClientFSM.READY, ClientFSMEvent.CONFIGURATION_STARTED): ClientFSM.CONFIGURING,
    (ClientFSM.READY, ClientFSMEvent.ACQUISITION_STARTED): ClientFSM.ACQUIRING,

    (ClientFSM.ACQUIRING, ClientFSMEvent.STOP_REQUESTED): ClientFSM.FINALIZING,
    (ClientFSM.ACQUIRING, ClientFSMEvent.RECEIVER_COMPLETED): ClientFSM.FINALIZING,

    (ClientFSM.FINALIZING, ClientFSMEvent.FINALIZATION_SUCCEEDED): ClientFSM.READY,
    (ClientFSM.FINALIZING, ClientFSMEvent.FINALIZATION_FAILED): ClientFSM.ERROR,

    (ClientFSM.CONNECTED, ClientFSMEvent.DISCONNECTED): ClientFSM.DISCONNECTED,
    (ClientFSM.CONFIGURING, ClientFSMEvent.DISCONNECTED): ClientFSM.DISCONNECTED,
    (ClientFSM.READY, ClientFSMEvent.DISCONNECTED): ClientFSM.DISCONNECTED,
    (ClientFSM.ACQUIRING, ClientFSMEvent.DISCONNECTED): ClientFSM.ERROR,
    (ClientFSM.FINALIZING, ClientFSMEvent.DISCONNECTED): ClientFSM.ERROR,
    (ClientFSM.ERROR, ClientFSMEvent.DISCONNECTED): ClientFSM.DISCONNECTED,
}

for _client_state in ClientFSM:
    TRANSITION_TABLE_CLIENT[(_client_state, ClientFSMEvent.FATAL_ERROR)] = ClientFSM.ERROR


ACQUISITION_ALLOWED_STATES = {ServerFSM.READY}
CONTROL_PLANE_AVAILABLE_STATES = {
    ServerFSM.CONTROL_CONNECTED,
    ServerFSM.CONNECTED,
    ServerFSM.CONFIGURING,
    ServerFSM.READY,
    ServerFSM.ACQUIRING,
    ServerFSM.FINALIZING,
    ServerFSM.ERROR,
}
ACQUISITION_PLANE_AVAILABLE_STATES = {
    ServerFSM.CONNECTED,
    ServerFSM.CONFIGURING,
    ServerFSM.READY,
    ServerFSM.ACQUIRING,
    ServerFSM.FINALIZING,
    ServerFSM.ERROR,
}


@dataclass
class ClientRecord:
    """Tutto ciò che il server sa su un singolo client."""
    state: ClientFSM = ClientFSM.CONNECTED
    previous_state: ClientFSM | None = None
    last_event_context: dict | None = None
    error_context: dict | None = None
    identity: dict | None = None
    hv_parameters: dict[int, dict] = field(default_factory=dict)
    calibration_excluded_channels: list[int] = field(default_factory=list)
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    on_control_plane: bool = False
    on_acquisition_plane: bool = False
    on_monitoring_plane: bool = False
    operational: bool = False


def command_guard(allowed_states: Iterable[ServerFSM]):
    allowed_states = set(allowed_states)

    if not allowed_states:
        raise ValueError("command_guard requires at least one allowed ServerFSM state")

    invalid_states = [state for state in allowed_states if not isinstance(state, ServerFSM)]
    if invalid_states:
        raise TypeError(
            "command_guard accepts only ServerFSM values. "
            f"Invalid values: {invalid_states!r}"
        )

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            server_state = getattr(self, "server_state", None)
            if server_state is None:
                raise RuntimeError(
                    f"{func.__qualname__} uses command_guard, "
                    "but the object has no 'server_state' attribute"
                )

            present_state = server_state.get_server_state()
            if present_state not in allowed_states:
                allowed_names = ", ".join(
                    state.value for state in sorted(allowed_states, key=lambda item: item.value)
                )
                message = (
                    f"Command '{func.__name__}' is not allowed while the server "
                    f"is in state '{present_state.value}'. Allowed states: {allowed_names}."
                )

                output_func = getattr(self, "poutput", None)
                if callable(output_func):
                    output_func(message)
                else:
                    server_state.logger.warning(message)
                return None

            return func(self, *args, **kwargs)

        return wrapper

    return decorator


def acquisition_guard(allowed_modes: Iterable[AcquisitionMode]):
    allowed_modes = set(allowed_modes)

    if not allowed_modes:
        raise ValueError("acquisition_guard requires at least one allowed acquisition mode")

    invalid_modes = [mode for mode in allowed_modes if not isinstance(mode, AcquisitionMode)]
    if invalid_modes:
        raise TypeError(
            "acquisition_guard accepts only AcquisitionMode values."
            f"Invalid values: {invalid_modes!r}"
        )

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            server_state = getattr(self, "server_state", None)
            if server_state is None:
                raise RuntimeError(
                    f"{func.__qualname__} uses acquisition_guard, "
                    "but the object has no 'server_state' attribute"
                )

            present_mode =  AcquisitionMode(server_state.get_mode())
            if present_mode not in allowed_modes:
                allowed_names = ", ".join(
                    mode.value for mode in sorted(allowed_modes, key=lambda item: item.value)
                )
                message = (
                    f"Command '{func.__name__}' is not allowed while the server "
                    f"is in acquisition mode '{present_mode}'. Allowed modes: {allowed_names}."
                )

                output_func = getattr(self, "poutput", None)
                if callable(output_func):
                    output_func(message)
                else:
                    server_state.logger.warning(message)
                return None

            return func(self, *args, **kwargs)

        return wrapper

    return decorator


class ServerState:
    """Thread-safe logical state and client registry for the multiDAQ server."""

    _ALLOWED_PENDING_TERMINAL_STATES = {
        ServerFSM.READY,
        ServerFSM.DISCONNECTED,
        ServerFSM.CONTROL_CONNECTED,
        ServerFSM.CONNECTED,
        ServerFSM.ERROR,
    }

    def __init__(self, initial_mode: str = "test"):
        self._lock = threading.RLock()
        self.logger = get_logger("server_state")

        self.acq_mode = initial_mode


        self.clients: dict[bytes, ClientRecord] = {}

        self.run_state = ServerFSM.DISCONNECTED
        self.previous_state: ServerFSM | None = None

        self.pending_event: ServerFSMEvent | None = None
        self.pending_terminal_state: ServerFSM | None = None
        self.pending_context: dict | None = None
        self.pending_configuration_source: ServerFSM | None = None

        self.last_event_context: dict | None = None
        self.error_context: dict | None = None

        self.logger.debug("Server State initialized")

    def set_mode(self, mode: str) -> None:
        if not isinstance(mode, str) or not mode.strip():
            raise ValueError("mode must be a non-empty string")

        normalized_mode = mode.strip().lower()

        valid_modes = {member.value for member in AcquisitionMode}
        if normalized_mode not in valid_modes:
            raise ValueError(
                f"Invalid acquisition mode: {normalized_mode!r}. "
                f"Valid modes: {sorted(valid_modes)}"
            )

        with self._lock:
            self.acq_mode = normalized_mode

    def get_mode(self) -> str:
        with self._lock:
            return self.acq_mode

    def get_server_state(self) -> ServerFSM:
        with self._lock:
            return self.run_state



    def _control_client_ids_locked(self) -> list[bytes]:
        return [cid for cid, r in self.clients.items() if r.on_control_plane]

    def _acquisition_client_ids_locked(self) -> list[bytes]:
        return [cid for cid, r in self.clients.items() if r.on_acquisition_plane]

    def _common_plane_client_ids_locked(self) -> list[bytes]:
        return [
            cid for cid, r in self.clients.items()
            if r.on_control_plane and r.on_acquisition_plane
        ]

    def _operational_client_ids_locked(self) -> list[bytes]:
        return [cid for cid, r in self.clients.items() if r.operational]

    def _set_operational_locked(self, client_ids: Iterable[bytes]) -> None:
        """Sostituisce per intero l'insieme operativo: True per i client
        indicati, False per tutti gli altri."""
        target = set(client_ids)
        for cid, record in self.clients.items():
            record.operational = cid in target

    def _clear_operational_locked(self) -> None:
        for record in self.clients.values():
            record.operational = False


    def has_control_clients(self) -> bool:
        with self._lock:
            return any(r.on_control_plane for r in self.clients.values())

    def has_acquisition_clients(self) -> bool:
        with self._lock:
            return any(r.on_acquisition_plane for r in self.clients.values())

    def list_common_plane_clients(self) -> list[bytes]:
        with self._lock:
            return self._common_plane_client_ids_locked()

    def has_common_plane_client(self) -> bool:
        with self._lock:
            return bool(self._common_plane_client_ids_locked())

    def can_start_acquisition(self) -> bool:
        with self._lock:
            if self.run_state != ServerFSM.READY:
                return False
            operational = self._operational_client_ids_locked()
            if not operational:
                return False
            common = set(self._common_plane_client_ids_locked())
            return all(cid in common for cid in operational)

    def is_control_plane_available(self) -> bool:
        with self._lock:
            return (
                any(r.on_control_plane for r in self.clients.values())
                and self.run_state in CONTROL_PLANE_AVAILABLE_STATES
            )

    def is_acquisition_plane_available(self) -> bool:
        with self._lock:
            return (
                bool(self._common_plane_client_ids_locked())
                and self.run_state in ACQUISITION_PLANE_AVAILABLE_STATES
            )

    def _build_event_context(
        self,
        event: ServerFSMEvent,
        *,
        reason: str,
        source: str,
        client_id: bytes | None,
        error: str | Exception | None,
        metadata: dict | None,
    ) -> dict:
        return {
            "event": event,
            "reason": reason.strip(),
            "source": source.strip(),
            "client_id": client_id,
            "error": str(error) if error is not None else None,
            "metadata": dict(metadata or {}),
            "timestamp": datetime.now(timezone.utc),
        }

    @staticmethod
    def _normalize_client_ids(client_ids) -> list[bytes]:
        if client_ids is None:
            return []

        normalized = list(dict.fromkeys(client_ids))
        invalid = [
            client_id
            for client_id in normalized
            if not isinstance(client_id, bytes) or not client_id
        ]
        if invalid:
            raise ValueError(f"Invalid client IDs: {invalid!r}")
        return normalized

    def _set_client_state_locked(self, client_id: bytes, state: ClientFSM, event_context: dict) -> None:
        record = self.clients.get(client_id)
        if record is None:
            self.logger.warning(f"Attempted to set state for unregistered client {client_id!r}: ignored")
            return

        record.previous_state = record.state
        record.state = state
        record.last_event_context = dict(event_context)

        if state == ClientFSM.ERROR:
            record.error_context = {**event_context, "previous_state": record.previous_state}
        else:
            record.error_context = None

    def process_event(
        self,
        event: ServerFSMEvent,
        *,
        reason: str,
        source: str,
        client_id: bytes | None = None,
        error: str | Exception | None = None,
        metadata: dict | None = None,
        requested_terminal_state: ServerFSM | None = None,
    ) -> bool:
        if not isinstance(event, ServerFSMEvent):
            self.logger.error(f"Invalid FSM event type: {event!r}")
            return False
        if not isinstance(reason, str) or not reason.strip():
            self.logger.error(f"Invalid or missing reason for FSM event {event.value}")
            return False
        if not isinstance(source, str) or not source.strip():
            self.logger.error(f"Invalid or missing source for FSM event {event.value}")
            return False
        if requested_terminal_state is not None:
            if not isinstance(requested_terminal_state, ServerFSM):
                self.logger.error(
                    f"Invalid requested terminal state: {requested_terminal_state!r}"
                )
                return False
            if requested_terminal_state not in self._ALLOWED_PENDING_TERMINAL_STATES:
                self.logger.error(
                    "Requested terminal state is not valid after finalization: "
                    f"{requested_terminal_state.value}"
                )
                return False

        event_context = self._build_event_context(
            event,
            reason=reason,
            source=source,
            client_id=client_id,
            error=error,
            metadata=metadata,
        )

        with self._lock:
            present_state = self.run_state
            next_state = TRANSITION_TABLE.get((present_state, event))
            if next_state is None:
                self.logger.warning(
                    "Invalid FSM transition: "
                    f"{present_state.value} --{event.value}--> ? | "
                    f"source={source}, reason={reason}"
                )
                return False

            if next_state == ServerFSM.FINALIZING:
                if event == ServerFSMEvent.DISCONNECT_REQUESTED:
                    terminal_state = ServerFSM.DISCONNECTED
                elif event == ServerFSMEvent.CONTROL_CONNECTION_LOST:
                    terminal_state = ServerFSM.DISCONNECTED
                elif event == ServerFSMEvent.ACQUISITION_CONNECTION_LOST:
                    terminal_state = ServerFSM.CONTROL_CONNECTED
                else:
                    terminal_state = requested_terminal_state or ServerFSM.READY

                self.pending_event = event
                self.pending_terminal_state = terminal_state
                self.pending_context = dict(event_context)

            elif requested_terminal_state is not None:
                # A terminal target is meaningful only for an operation that
                # enters FINALIZING.
                self.logger.error(
                    "requested_terminal_state can only be used when the event "
                    "transitions the server to FINALIZING"
                )
                return False

            if event == ServerFSMEvent.CONFIGURATION_STARTED and next_state == ServerFSM.CONFIGURING:
                self.pending_configuration_source = present_state

            if (
                present_state == ServerFSM.CONFIGURING
                and event == ServerFSMEvent.CONFIGURATION_FAILED
            ):
                next_state = self.pending_configuration_source or ServerFSM.CONNECTED

            if (
                present_state == ServerFSM.FINALIZING
                and event == ServerFSMEvent.FINALIZATION_SUCCEEDED
                and self.pending_terminal_state is not None
            ):
                next_state = self.pending_terminal_state

            metadata_payload = event_context["metadata"]

            if event == ServerFSMEvent.CONFIGURATION_STARTED:
                try:
                    target_clients = self._normalize_client_ids(
                        metadata_payload.get(
                            "target_clients",
                            self._common_plane_client_ids_locked(),
                        )
                    )
                except ValueError as exc:
                    self.logger.error(str(exc))
                    return False

                common_clients = set(self._common_plane_client_ids_locked())
                invalid_clients = [
                    client_id for client_id in target_clients
                    if client_id not in common_clients
                ]
                if invalid_clients or not target_clients:
                    self.logger.error(
                        "Configuration requires at least one target client connected "
                        f"on both planes. Invalid clients: {invalid_clients!r}"
                    )
                    return False

                # A new configuration invalidates the previous operational set
                # until its result is known.
                self._clear_operational_locked()
                for target_client in target_clients:
                    self._set_client_state_locked(
                        target_client, ClientFSM.CONFIGURING, event_context
                    )

            elif event == ServerFSMEvent.CONFIGURATION_SUCCEEDED:
                try:
                    successful_clients = self._normalize_client_ids(
                        metadata_payload.get("successful_clients")
                    )
                    failed_clients = self._normalize_client_ids(
                        metadata_payload.get("failed_clients", [])
                    )
                except ValueError as exc:
                    self.logger.error(str(exc))
                    return False

                if not successful_clients:
                    self.logger.error(
                        "CONFIGURATION_SUCCEEDED requires at least one successful client"
                    )
                    return False

                if set(successful_clients) & set(failed_clients):
                    self.logger.error(
                        "A client cannot be both successful and failed in the same "
                        "configuration result"
                    )
                    return False

                expected = set(self.list_clients_in_state(ClientFSM.CONFIGURING))
                provided = set(successful_clients) | set(failed_clients)

                if provided != expected:
                    missing = expected - provided
                    unexpected = provided - expected

                    self.logger.error(
                        "CONFIGURATION_SUCCEEDED does not match clients under configuration. "
                        f"Missing: {missing!r}, unexpected: {unexpected!r}"
                    )
                    return False

                common_clients = set(self._common_plane_client_ids_locked())
                invalid_clients = [
                    client_id for client_id in successful_clients
                    if client_id not in common_clients
                ]
                if invalid_clients:
                    self.logger.error(
                        "Configured clients are not connected on both planes: "
                        f"{invalid_clients!r}"
                    )
                    return False

                self._set_operational_locked(successful_clients)

                for configured_client in successful_clients:
                    self._set_client_state_locked(
                        configured_client, ClientFSM.READY, event_context
                    )

                # Failed clients remain registered and may still use the Control
                # Plane, but are excluded from subsequent acquisition commands.
                for failed_client in failed_clients:
                    self._set_client_state_locked(
                        failed_client, ClientFSM.ERROR, event_context
                    )

            elif event == ServerFSMEvent.CONFIGURATION_FAILED:
                failed_clients = metadata_payload.get(
                    "failed_clients",
                    self.list_clients_in_state(ClientFSM.CONFIGURING),
                )

                try:
                    failed_clients = self._normalize_client_ids(failed_clients)
                except ValueError as exc:
                    self.logger.error(str(exc))
                    return False

                expected = set(
                    self.list_clients_in_state(ClientFSM.CONFIGURING)
                )
                provided = set(failed_clients)

                if provided != expected:
                    missing = expected - provided
                    unexpected = provided - expected

                    self.logger.error(
                        "CONFIGURATION_FAILED does not match clients under configuration. "
                        f"Missing: {missing!r}, unexpected: {unexpected!r}"
                    )
                    return False

                self._clear_operational_locked()

                for failed_client in failed_clients:
                    self._set_client_state_locked(
                        failed_client,
                        ClientFSM.ERROR,
                        event_context,
                    )

            elif event == ServerFSMEvent.ACQUISITION_STARTED:
                try:
                    active_clients = self._normalize_client_ids(
                        metadata_payload.get(
                            "active_clients",
                            self._operational_client_ids_locked(),
                        )
                    )
                except ValueError as exc:
                    self.logger.error(str(exc))
                    return False

                if not active_clients:
                    self.logger.error(
                        "ACQUISITION_STARTED requires at least one active client"
                    )
                    return False

                operational_set = set(self._operational_client_ids_locked())
                invalid_clients = [
                    client_id for client_id in active_clients
                    if client_id not in operational_set
                    or (self.clients.get(client_id) is None or self.clients[client_id].state != ClientFSM.READY)
                ]
                if invalid_clients:
                    self.logger.error(
                        "Acquisition clients are not in the configured READY set: "
                        f"{invalid_clients!r}"
                    )
                    return False

                self._set_operational_locked(active_clients)
                for active_client in active_clients:
                    self._set_client_state_locked(
                        active_client, ClientFSM.ACQUIRING, event_context
                    )
                    
            elif event == ServerFSMEvent.FATAL_ERROR:
                operational_clients = list(
                    self._operational_client_ids_locked()
                )

                for failed_client in operational_clients:
                    self._set_client_state_locked(
                        failed_client,
                        ClientFSM.ERROR,
                        event_context,
                    )

                self._clear_operational_locked()

            elif next_state == ServerFSM.FINALIZING:
                for active_client in self._operational_client_ids_locked():
                    self._set_client_state_locked(
                        active_client, ClientFSM.FINALIZING, event_context
                    )

            elif (
                present_state == ServerFSM.FINALIZING
                and event == ServerFSMEvent.FINALIZATION_SUCCEEDED
            ):
                operational_clients = list(
                    self._operational_client_ids_locked()
                )

                terminal_state = (
                    self.pending_terminal_state
                    if self.pending_terminal_state is not None
                    else ServerFSM.READY
                )

                if terminal_state == ServerFSM.READY:

                    for finalized_client in operational_clients:
                        self._set_client_state_locked(
                            finalized_client,
                            ClientFSM.READY,
                            event_context,
                        )

                elif terminal_state == ServerFSM.CONTROL_CONNECTED:

                    for finalized_client in operational_clients:
                        record = self.clients.get(finalized_client)

                        if record is None:
                            continue

                        record.operational = False

                        if record.on_control_plane:
                            self._set_client_state_locked(
                                finalized_client,
                                ClientFSM.CONNECTED,
                                event_context,
                            )
                        else:
                            self._set_client_state_locked(
                                finalized_client,
                                ClientFSM.DISCONNECTED,
                                event_context,
                            )

                elif terminal_state == ServerFSM.DISCONNECTED:

                    for finalized_client in operational_clients:
                        record = self.clients.get(finalized_client)

                        if record is None:
                            continue

                        record.operational = False

                        self._set_client_state_locked(
                            finalized_client,
                            ClientFSM.DISCONNECTED,
                            event_context,
                        )

                elif terminal_state == ServerFSM.CONNECTED:

                    for finalized_client in operational_clients:
                        record = self.clients.get(finalized_client)

                        if record is None:
                            continue

                        record.operational = False

                        self._set_client_state_locked(
                            finalized_client,
                            ClientFSM.CONNECTED,
                            event_context,
                        )

                elif terminal_state == ServerFSM.ERROR:

                    for finalized_client in operational_clients:
                        record = self.clients.get(finalized_client)

                        if record is None:
                            continue

                        record.operational = False

                        self._set_client_state_locked(
                            finalized_client,
                            ClientFSM.ERROR,
                            event_context,
                        )

            elif (
                present_state == ServerFSM.FINALIZING
                and event == ServerFSMEvent.FINALIZATION_FAILED
            ):
                for failed_client in self._operational_client_ids_locked():
                    self._set_client_state_locked(
                        failed_client,
                        ClientFSM.ERROR,
                        event_context,
                    )

                self._clear_operational_locked()

            self.previous_state = present_state
            self.run_state = next_state
            self.last_event_context = event_context

            if next_state == ServerFSM.ERROR:
                self.error_context = {
                    **event_context,
                    "previous_state": present_state,
                }

            if present_state == ServerFSM.FINALIZING and event in {
                ServerFSMEvent.FINALIZATION_SUCCEEDED,
                ServerFSMEvent.FINALIZATION_FAILED,
                ServerFSMEvent.FATAL_ERROR,
            }:
                self.pending_event = None
                self.pending_terminal_state = None
                self.pending_context = None

            if present_state == ServerFSM.CONFIGURING and event in {
                ServerFSMEvent.CONFIGURATION_SUCCEEDED,
                ServerFSMEvent.CONFIGURATION_FAILED,
            }:
                self.pending_configuration_source = None

        if present_state == next_state:
            self.logger.info(
                "FSM event processed without state change: "
                f"{present_state.value} --{event.value}--> {next_state.value}"
            )
        else:
            self.logger.info(
                f"FSM transition: {present_state.value} --{event.value}--> {next_state.value}"
            )
        return True

    def process_client_event(
        self,
        client_id: bytes,
        event: ClientFSMEvent,
        *,
        reason: str,
        source: str,
        error: str | Exception | None = None,
        metadata: dict | None = None,
    ) -> bool:
        if not isinstance(client_id, bytes) or not client_id:
            self.logger.error(f"Invalid client ID for client FSM event: {client_id!r}")
            return False
        if not isinstance(event, ClientFSMEvent):
            self.logger.error(f"Invalid client FSM event type: {event!r}")
            return False
        if not isinstance(reason, str) or not reason.strip():
            self.logger.error(
                f"Invalid or missing reason for client FSM event {event.value}"
            )
            return False
        if not isinstance(source, str) or not source.strip():
            self.logger.error(
                f"Invalid or missing source for client FSM event {event.value}"
            )
            return False

        event_context = {
            "event": event,
            "reason": reason.strip(),
            "source": source.strip(),
            "client_id": client_id,
            "error": str(error) if error is not None else None,
            "metadata": dict(metadata or {}),
            "timestamp": datetime.now(timezone.utc),
        }

        with self._lock:
            record = self.clients.get(client_id)
            if record is None:
                self.logger.error(f"Cannot process client FSM event for unknown client: {client_id!r}")
                return False

            present_state = record.state
            next_state = TRANSITION_TABLE_CLIENT.get((present_state, event))
            if next_state is None:
                self.logger.warning(
                    f"Invalid client FSM transition for {client_id!r}: "
                    f"{present_state.value} --{event.value}--> ? | source={source}, reason={reason}"
                )
                return False

            record.previous_state = present_state
            record.state = next_state
            record.last_event_context = event_context

            if next_state == ClientFSM.ERROR:
                record.error_context = {**event_context, "previous_state": present_state}

        client_name = client_id.decode(errors="ignore")
        self.logger.info(
            f"Client FSM transition for {client_name}: "
            f"{present_state.value} --{event.value}--> {next_state.value}"
        )
        return True

    def reset_to_disconnected(self, *, reason: str, source: str) -> None:
        with self._lock:
            self.previous_state = self.run_state
            self.run_state = ServerFSM.DISCONNECTED
            self._clear_operational_locked()
            self.pending_event = None
            self.pending_terminal_state = None
            self.pending_context = None
            self.last_event_context = {
                "event": None,
                "reason": reason,
                "source": source,
                "timestamp": datetime.now(timezone.utc),
            }
        self.logger.info(f"Server state forcibly reset to DISCONNECTED: {reason}")

    def get_pending_transition(
        self,
    ) -> tuple[ServerFSM | None, ServerFSMEvent | None, dict | None]:
        with self._lock:
            context = dict(self.pending_context) if self.pending_context else None
            return self.pending_terminal_state, self.pending_event, context

    def clear_pending_transition(self) -> None:
        with self._lock:
            self.pending_terminal_state = None
            self.pending_event = None
            self.pending_context = None

    def get_client_state(self, client_id: bytes) -> ClientFSM | None:
        with self._lock:
            record = self.clients.get(client_id)
            return record.state if record else None

    def list_clients_in_state(self, state: ClientFSM) -> list[bytes]:
        if not isinstance(state, ClientFSM):
            raise TypeError(f"Expected ClientFSM, received {state!r}")
        with self._lock:
            return [cid for cid, record in self.clients.items() if record.state == state]

    def get_client_states(self) -> dict[bytes, ClientFSM]:
        with self._lock:
            return {cid: record.state for cid, record in self.clients.items()}

    def add_control_client(self, client_id: bytes, identity: Optional[dict] = None) -> None:
        if not isinstance(client_id, bytes) or not client_id:
            raise ValueError("client_id must be non-empty bytes")

        with self._lock:
            if client_id not in self.clients:
                self.clients[client_id] = ClientRecord()

            record = self.clients[client_id]

            if not record.on_control_plane:
                record.on_control_plane = True
                self.logger.info(
                    f"Control client {client_id.decode(errors='ignore')} connected. "
                    f"Total control clients: {sum(1 for r in self.clients.values() if r.on_control_plane)}"
                )

            if identity is not None:
                record.identity = dict(identity)

    def add_client(self, client_id: bytes, identity: Optional[dict] = None) -> None:
        self.add_control_client(client_id, identity)

    def remove_acquisition_client(self, client_id: bytes) -> None:
        """Remove a client from the Acquisition Plane only, keeping its
        Control Plane registration and identity intact."""
        with self._lock:
            record = self.clients.get(client_id)
            removed = bool(record and record.on_acquisition_plane)
            if record is not None:
                record.on_acquisition_plane = False
                record.operational = False

        if removed:
            self.logger.info(
                f"Acquisition client {client_id.decode(errors='ignore')} removed "
                "(Control Plane registration retained)"
            )

    def clear_acquisition_clients(self) -> None:
        with self._lock:
            for record in self.clients.values():
                if record.on_acquisition_plane:
                    record.on_acquisition_plane = False
                    record.operational = False
        self.logger.debug("Acquisition Plane client registry cleared")

    def add_acquisition_client(self, client_id: bytes) -> None:
        if not isinstance(client_id, bytes) or not client_id:
            raise ValueError("client_id must be non-empty bytes")

        with self._lock:
            record = self.clients.get(client_id)
            if record is None or not record.on_control_plane:
                raise ValueError(
                    "An Acquisition Plane client must already be registered "
                    "on the Control Plane"
                )

            if not record.on_acquisition_plane:
                record.on_acquisition_plane = True
                self.logger.info(
                    f"Acquisition client {client_id.decode(errors='ignore')} connected. "
                    f"Total acquisition clients: {sum(1 for r in self.clients.values() if r.on_acquisition_plane)}"
                )

    def _forget_client_locked(self, client_id: bytes) -> None:
        self.clients.pop(client_id, None)

    def remove_client(self, client_id: bytes) -> None:
        """Forget a client completely from every server registry."""
        with self._lock:
            existed = client_id in self.clients
            self._forget_client_locked(client_id)

        if existed:
            self.logger.info(
                f"Client {client_id.decode(errors='ignore')} forgotten from server state"
            )

    def remove_control_client(self, client_id: bytes) -> None:
        self.remove_client(client_id)

    def set_operational_clients(self, client_ids: Iterable[bytes]) -> None:
        """Set clients admitted by the latest successful configuration.

        Only clients connected on both planes may become operational.
        """
        requested = list(dict.fromkeys(client_ids))

        with self._lock:
            common = set(self._common_plane_client_ids_locked())
            invalid = [client_id for client_id in requested if client_id not in common]
            if invalid:
                raise ValueError(
                    f"Operational clients must be connected on both planes: {invalid!r}"
                )
            self._set_operational_locked(requested)

    def get_operational_clients(self) -> list[bytes]:
        with self._lock:
            return self._operational_client_ids_locked()

    def get_acquisition_command_clients(self) -> list[bytes]:
        """Return the only clients acquisition commands may target."""
        with self._lock:
            if self.run_state not in {
                ServerFSM.READY,
                ServerFSM.ACQUIRING,
                ServerFSM.FINALIZING,
            }:
                return []
            return self._operational_client_ids_locked()

    def clear_operational_clients(self) -> None:
        with self._lock:
            self._clear_operational_locked()

    def list_connected_clients(self) -> List[bytes]:
        """Backward-compatible Control Plane client list."""
        with self._lock:
            return self._control_client_ids_locked()

    def list_acquisition_clients(self) -> List[bytes]:
        with self._lock:
            return self._acquisition_client_ids_locked()

    def list_operational_clients(self) -> List[bytes]:
        with self._lock:
            return self._operational_client_ids_locked()

    def get_identity(self, client_id: bytes) -> Optional[dict]:
        with self._lock:
            record = self.clients.get(client_id)
            if record is None or record.identity is None:
                return None
            return dict(record.identity)

    def get_client_id_by_multipmt_id(self, multipmt_id: str) -> Optional[bytes]:
        """Ricerca lineare deliberata: nessun indice separato da tenere
        sincronizzato. A qualche migliaio di client il costo è trascurabile
        (microsecondi), e non è mai chiamata nel percorso dati."""
        with self._lock:
            target = str(multipmt_id)
            for cid, record in self.clients.items():
                if record.identity and str(record.identity.get("multipmt_id")) == target:
                    return cid
            return None

    def get_client_id_by_batch_id(self, batch_id: str) -> Optional[bytes]:
        with self._lock:
            target = str(batch_id)
            for cid, record in self.clients.items():
                if record.identity and str(record.identity.get("batch_id")) == target:
                    return cid
            return None

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "mode": self.acq_mode,
                "state": self.run_state,
                "previous_state": self.previous_state,
                "control_clients": self._control_client_ids_locked(),
                "acquisition_clients": self._acquisition_client_ids_locked(),
                "monitoring_clients": self._monitoring_client_ids_locked(),
                "common_clients": self._common_plane_client_ids_locked(),
                "operational_clients": self._operational_client_ids_locked(),
                "pending_terminal_state": self.pending_terminal_state,
                "pending_event": self.pending_event,
                "last_event_context": dict(self.last_event_context)
                if self.last_event_context
                else None,
                "error_context": dict(self.error_context)
                if self.error_context
                else None,
                "client_states": {cid: record.state for cid, record in self.clients.items()},
            }

    def set_client_hv_parameters(self, client_id: bytes, acq_info: dict[int, dict]) -> None:
        with self._lock:
            record = self.clients.get(client_id)
            if record is None:
                self.logger.warning(f"Cannot set HV parameters: unknown client {client_id!r}")
                return
            for ch, params in acq_info.items():
                entry = record.hv_parameters.setdefault(ch, {})
                if "voltage" in params:
                    entry["voltage"] = params["voltage"]
                if "threshold" in params:
                    entry["threshold"] = params["threshold"]

    def get_client_hv_parameters(self, client_id: bytes) -> dict[int, dict]:
        with self._lock:
            record = self.clients.get(client_id)
            if record is None:
                return {}
            return {ch: dict(v) for ch, v in record.hv_parameters.items()}

    def set_calibration_excluded_channels(self, client_id: bytes, channels: list[int]) -> None:
        with self._lock:
            record = self.clients.get(client_id)
            if record is None:
                return
            record.calibration_excluded_channels = list(channels)

    def get_calibration_excluded_channels(self, client_id: bytes) -> list[int]:
        with self._lock:
            record = self.clients.get(client_id)
            return list(record.calibration_excluded_channels) if record else []

    def get_client_connected_since(self, client_id: bytes) -> datetime | None:
        with self._lock:
            record = self.clients.get(client_id)
            return record.connected_at if record else None
        

    def _monitoring_client_ids_locked(self) -> list[bytes]:
        return [cid for cid, r in self.clients.items() if r.on_monitoring_plane]

    def list_monitoring_clients(self) -> list[bytes]:
        with self._lock:
            return self._monitoring_client_ids_locked()

    def add_monitoring_client(self, client_id: bytes) -> None:
        if not isinstance(client_id, bytes) or not client_id:
            raise ValueError("client_id must be non-empty bytes")

        with self._lock:
            record = self.clients.get(client_id)
            if record is None or not record.on_control_plane:
                raise ValueError(
                    "A Monitoring Plane client must already be registered "
                    "on the Control Plane"
                )

            if not record.on_monitoring_plane:
                record.on_monitoring_plane = True
                self.logger.info(
                    f"Monitoring client {client_id.decode(errors='ignore')} connected. "
                    f"Total monitoring clients: {sum(1 for r in self.clients.values() if r.on_monitoring_plane)}"
                )

    def remove_monitoring_client(self, client_id: bytes) -> None:
        with self._lock:
            record = self.clients.get(client_id)
            removed = bool(record and record.on_monitoring_plane)
            if record is not None:
                record.on_monitoring_plane = False

        if removed:
            self.logger.info(
                f"Monitoring client {client_id.decode(errors='ignore')} removed "
                "(Control Plane registration retained)"
            )

    def clear_monitoring_clients(self) -> None:
        with self._lock:
            for record in self.clients.values():
                record.on_monitoring_plane = False
        self.logger.debug("Monitoring Plane client registry cleared")
        
        
    def is_client_on_plane(
        self,
        client_id: bytes,
        plane: str,
    ) -> bool:

        normalized_plane = str(plane).strip().lower()

        with self._lock:
            record = self.clients.get(client_id)

            if record is None:
                return False

            if normalized_plane == "control":
                return record.on_control_plane

            if normalized_plane == "acquisition":
                return record.on_acquisition_plane

            if normalized_plane == "monitoring":
                return record.on_monitoring_plane

            self.logger.error(
                f"Unknown plane requested for client membership check: "
                f"{plane!r}"
            )
            return False
    
    
    def resolve_client_id(
        self,
        *,
        client_id: bytes | None = None,
        multipmt_id: str | None = None,
        batch_id: str | None = None,
    ) -> bytes | None:

        provided = [
            value is not None
            for value in (
                client_id,
                multipmt_id,
                batch_id,
            )
        ]

        if sum(provided) != 1:
            return None

        if client_id is not None:
            with self._lock:
                return client_id if client_id in self.clients else None

        if multipmt_id is not None:
            return self.get_client_id_by_multipmt_id(multipmt_id)

        return self.get_client_id_by_batch_id(batch_id)