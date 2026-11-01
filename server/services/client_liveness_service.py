import threading

from server.utils.logger import get_logger
from server.services.client_command_service import CommandPlane
from server.core.server_state import ClientFSMEvent


class ClientLivenessService:

    def __init__(self, server_state, control_manager, acquisition_manager, monitor_manager, output_func=None):

        self.server_state = server_state
        self.control_manager = control_manager
        self.acquisition_manager = acquisition_manager
        self.monitor_manager = monitor_manager

        self._listeners: list = []
        self._listeners_lock = threading.Lock()

        self.poutput = output_func or (lambda message: None)

        self.logger = get_logger('client_liveness_service')
        self.logger.debug("Client Liveness Service initialized")

    def handle_client_lost(self, client_id: bytes, plane: CommandPlane | str) -> bool:
        """Returns True if the client was registered on `plane` and has been removed."""

        client_name = client_id.decode(errors="ignore")

        try:
            plane = CommandPlane(plane)
        except ValueError:
            self.logger.error(f"Client {client_name} reported lost on an unknown plane: {plane!r}")
            return False

        self.logger.warning(f"Client {client_name} reported lost on the {plane.value} plane")

        if not self.server_state.is_client_on_plane(client_id=client_id, plane=plane.value):
            self.logger.info(f"Client {client_name} is no longer registered on the {plane.value} plane, nothing to do")
            return False

        try:
            if plane == CommandPlane.CONTROL:
                self._remove_from_every_plane(client_id)
                self._cancel_pending_replies(client_id, plane="control")
                self._cancel_pending_replies(client_id, plane="acquisition")
                self._cancel_pending_replies(client_id, plane="monitoring")

                self.logger.info(f"Client {client_name} removed from every plane and from the operational clients")
                self.poutput(f"Client {client_name} lost on the control plane: removed from every plane, it has to reconnect.")

            elif plane == CommandPlane.ACQUISITION:
                # Before the removal, so that the log shows the state the client was in
                if not self.server_state.process_client_event(
                    client_id,
                    ClientFSMEvent.ACQUISITION_PLANE_LOST,
                    reason=f"client {client_name} lost on the acquisition plane",
                    source="client_liveness_service",
                ):
                    self.logger.error(f"Client FSM rejected ACQUISITION_PLANE_LOST for {client_name}")

                # remove_acquisition_client also clears the operational flag of the client
                self.acquisition_manager.remove_client(client_id)
                self._cancel_pending_replies(client_id, plane="acquisition")

                self.logger.info(f"Client {client_name} removed from the acquisition plane and from the operational clients")
                self.poutput(f"Client {client_name} lost on the acquisition plane: it is no longer operational until the plane is recovered.")

            elif plane == CommandPlane.MONITORING:
                # The manager also stops the time-sync of the client and invalidates its streams
                self.monitor_manager.remove_client(client_id)
                self._cancel_pending_replies(client_id, plane="monitoring")

                self.logger.info(f"Client {client_name} removed from the monitoring plane")
                self.poutput(f"Client {client_name} lost on the monitoring plane: monitoring stopped for this client.")

        except Exception:
            self.logger.exception(f"Failed to handle the loss of client {client_name} on the {plane.value} plane")
            return False

        self._notify_listeners(client_id=client_id, plane=plane)

        return True

    def _remove_from_every_plane(self, client_id: bytes) -> None:
        # Through the managers where they do more than the registry (monitoring: time-sync and streams)
        if self.server_state.is_client_on_plane(client_id=client_id, plane="monitoring"):
            self.monitor_manager.remove_client(client_id)

        if self.server_state.is_client_on_plane(client_id=client_id, plane="acquisition"):
            self.acquisition_manager.remove_client(client_id)

        self.server_state.remove_operational_client(client_id=client_id)
        self.server_state.remove_client(client_id=client_id)

    def _cancel_pending_replies(self, client_id: bytes, plane: str) -> None:
        """Wake up whoever is waiting for a reply of the client on that plane only."""
        manager = {
            "control": self.control_manager,
            "acquisition": self.acquisition_manager,
            "monitoring": self.monitor_manager,
        }[plane]

        manager.reply_handler.cancel_client(client_id, reason=f"client lost on the {plane} plane")


    def register_listener(self, listener) -> None:
        """`listener(client_id, plane)` is called after a client has been removed.
        It runs in the I/O thread of the manager that detected the loss: it must be
        quick and must not wait for the network."""

        name = getattr(listener, "__qualname__", repr(listener))

        with self._listeners_lock:
            if listener in self._listeners:
                self.logger.warning(f"Client-lost listener {name} already registered")
                return

            self._listeners.append(listener)

        self.logger.debug(f"Client-lost listener {name} registered")

    def _notify_listeners(self, client_id: bytes, plane: CommandPlane) -> None:
        with self._listeners_lock:
            listeners = list(self._listeners)

        for listener in listeners:
            try:
                listener(client_id, plane)
            except Exception:
                name = getattr(listener, "__qualname__", repr(listener))
                self.logger.exception(f"Client-lost listener {name} failed")
