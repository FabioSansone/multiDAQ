import queue
import threading
import time
from typing import List, Optional

import zmq

from common.message_handler import (
    Channel,
    MessageHandler,
    MessageStatus,
    MessageType,
    ProtocolMessage,
)
from server.core.server_state import ServerState
from server.utils.logger import get_logger


MAX_RETRIES = 5


class MonitoringPlaneManager:
    def __init__(
        self,
        context: zmq.Context,
        state: ServerState,
    ) -> None:
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.endpoint: Optional[str] = None
        self.recv_poller = zmq.Poller()

        self.server_state = state

        self.message_handler = MessageHandler(
            logger=get_logger("message_handler")
        )

        self.mon_incoming_queue = queue.Queue()
        self.mon_outgoing_queue = queue.Queue()
        self.mon_event_queue = queue.Queue()

        self.mon_stop_listening = threading.Event()

        self.mon_listener_thread: Optional[threading.Thread] = None
        self.mon_event_thread: Optional[threading.Thread] = None
        self.mon_event_callback = None

        self.logger = get_logger("monitoring_manager")
        self.logger.debug("ZMQ Monitoring Server Manager initialized")

    def list_connected_clients(self) -> List[bytes]:
        return self.server_state.list_monitoring_clients()

    def is_client_connected(self, client_id: bytes) -> bool:
        return self.server_state.is_client_on_plane(
            client_id=client_id,
            plane="monitoring",
        )

    def add_client(self, client_id: bytes) -> None:
        try:
            self.server_state.add_monitoring_client(client_id)
        except ValueError as e:
            self.logger.error(f"Cannot register monitoring client {client_id!r}: {e}")
            return

    def remove_client(self, client_id: bytes) -> None:
        self.server_state.remove_monitoring_client(client_id)
        self.logger.info(f"MonitoringPlane client removed: {client_id!r}")

    def clear_clients(self) -> None:
        self.server_state.clear_monitoring_clients()
        self.logger.debug("MonitoringPlane client registry cleared")

    def get_identity(self, client_id: bytes) -> Optional[dict]:
        return self.server_state.get_identity(client_id)

    def start_connection(self, port: int) -> bool:
        """Start the MonitoringPlane ROUTER socket."""

        if self.socket is not None:
            try:
                self.recv_poller.unregister(self.socket)
            except KeyError:
                pass

            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()

            self.socket = None
            self.endpoint = None

        try:
            self.socket = self.context.socket(zmq.ROUTER)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.ROUTER_MANDATORY, 1)

            self.socket.bind(f"tcp://*:{port}")
            self.recv_poller.register(self.socket, zmq.POLLIN)

            self.endpoint = f"tcp://*:{port}"

            self.logger.info(f"MonitoringPlaneManager started on port {port}")
            return True

        except zmq.ZMQError as exc:
            self.socket = None
            self.endpoint = None

            self.logger.error(
                "ZMQ Exception: failed to bind monitoring socket "
                f"on port {port}: {exc}"
            )
            return False

        except Exception as exc:
            self.socket = None
            self.endpoint = None

            self.logger.error(
                "Generic Exception: failed to bind monitoring socket "
                f"on port {port}: {exc}"
            )
            return False

    def receive_message(
        self,
        timeout_ms: int,
    ) -> tuple[Optional[bytes], Optional[ProtocolMessage], str]:

        if self.socket is None:
            self.logger.error(
                "Cannot receive message: monitoring socket not initialized"
            )
            return None, None, "monitoring socket not initialized"

        try:
            socks = dict(self.recv_poller.poll(timeout=timeout_ms))

            if self.socket not in socks:
                return None, None, "timeout elapsed"

            frames = self.socket.recv_multipart()

            if not frames:
                self.logger.error("Received empty multipart message")
                return None, None, "empty multipart message"

            if len(frames) < 2:
                self.logger.error(f"Invalid multipart message format: {frames}")
                return None, None, "invalid multipart format"

            client_id = frames[0]
            raw_message = frames[-1]

            message, reason = self.message_handler.deserialize(raw_message)

            if message is None:
                self.logger.error(
                    "Failed to deserialize monitoring message "
                    f"from client {client_id!r}: {reason}"
                )
                return client_id, None, reason

            self.logger.debug(
                f"Received monitoring message from client {client_id!r}: "
                f"type={message.msg_type.value}, "
                f"request_id={message.request_id}"
            )

            return client_id, message, "ok"

        except zmq.ZMQError as exc:
            self.logger.error(f"ZMQ error while receiving monitoring message: {exc}")
            return None, None, f"zmq error: {exc}"

        except Exception as exc:
            self.logger.error(
                f"Unexpected error while receiving monitoring message: {exc}"
            )
            return None, None, f"unexpected error: {exc}"

    def send_message(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        if self.socket is None:
            self.logger.error(
                "Cannot send message: monitoring socket not initialized"
            )
            return False

        try:
            message_raw = self.message_handler.serialize(message)

            if not message_raw:
                self.logger.error(
                    "Serialization failed for monitoring message "
                    f"with request_id={message.request_id}"
                )
                return False

            self.socket.send_multipart([client_id, message_raw])

            self.logger.debug(
                f"Sent monitoring message to client {client_id!r}: "
                f"type={message.msg_type.value}, "
                f"request_id={message.request_id}"
            )

            return True

        except zmq.ZMQError as exc:
            self.logger.error(
                f"ZMQ error while sending monitoring message to client {client_id!r}: {exc}"
            )

            if exc.errno == zmq.EHOSTUNREACH:
                self.remove_client(client_id)

            return False

        except Exception as exc:
            self.logger.error(
                f"Unexpected error while sending monitoring message to client {client_id!r}: {exc}"
            )
            return False

    def handshake_core(self, timeout_ms: int = 20000) -> bool:
        if self.socket is None:
            self.logger.error(
                "Communication not yet established. Cannot perform monitoring handshake."
            )
            return False

        client_id, message, reason = self.receive_message(timeout_ms)

        if message is None or client_id is None:
            self.logger.error(
                f"Monitoring handshake failed while waiting for client hello: {reason}"
            )
            return False

        # Il monitoring richiede solo il piano di controllo gia' attivo indipendentemente dal piano di acquisizione
        if client_id not in self.server_state.list_connected_clients():
            self.logger.error(
                "Monitoring handshake received from unknown "
                f"ControlPlane client {client_id!r}"
            )
            return False

        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                "Unexpected message type during monitoring handshake "
                f"from {client_id!r}: {message.msg_type}"
            )
            return False

        if message.phase != "monitoring_hello":
            self.logger.error(
                f"Unexpected monitoring handshake phase from {client_id!r}: {message.phase}"
            )
            return False

        if message.payload.get("message") != "Mon_hello":
            self.logger.error(
                f"Unexpected monitoring hello payload from {client_id!r}: {message.payload}"
            )
            return False

        mon_response = self.message_handler.create_handshake(
            phase="monitoring_ready",
            payload={"message": "Mon_alive"},
            in_reply_to=message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=mon_response):
            self.logger.error(f"Failed to send monitoring ready message to client {client_id!r}")
            return False

        self.add_client(client_id)

        identity = self.server_state.get_identity(client_id) or {}
        multipmt_id = identity.get("multipmt_id", "unknown")
        batch_id = identity.get("batch_id", "unknown")

        self.logger.info(
            "Monitoring handshake completed successfully with "
            f"client {client_id!r}, multipmt_id={multipmt_id}, batch_id={batch_id}"
        )

        return True

    def handshake(self) -> bool:
        target_clients = len(self.server_state.list_connected_clients())

        if target_clients == 0:
            self.logger.warning(
                "Cannot start monitoring handshake: no ControlPlane clients connected"
            )
            return False

        self.logger.info(f"Waiting for {target_clients} MonitoringPlane client(s) to connect...")

        while len(self.list_connected_clients()) < target_clients:
            retries = 0
            success = False

            while retries < MAX_RETRIES and not success:
                self.logger.info(
                    f"Trying monitoring handshake (attempt {retries + 1}/{MAX_RETRIES})"
                )

                success = self.handshake_core(timeout_ms=20000)

                if not success:
                    retries += 1
                    self.logger.warning("Monitoring handshake attempt failed, retrying...")

            if not success:
                self.logger.warning(
                    "No more MonitoringPlane clients connected after maximum attempts. "
                    "Server will remain operative with currently connected monitoring clients."
                )
                break

        connected_clients = self.list_connected_clients()

        if connected_clients:
            self.logger.info(
                "Monitoring plane ready. Connected monitoring clients: "
                f"{len(connected_clients)}/{target_clients}"
            )
            return True

        self.logger.error("No MonitoringPlane clients connected.")
        return False

    def queue_message(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> None:
        self.mon_outgoing_queue.put((client_id, message))

    def wait_for_reply(
        self,
        *,
        client_id: bytes,
        in_reply_to: str,
        timeout_s: float = 10.0,
    ) -> tuple[Optional[ProtocolMessage], str]:
        """
        Wait for a reply matching both client_id and in_reply_to.

        Non-matching messages are temporarily removed from the incoming queue
        and restored before returning. A monotonic deadline guarantees that
        unrelated messages cannot restart or extend the requested timeout.
        """

        deadline = time.monotonic() + timeout_s
        deferred_messages = []

        try:
            while True:
                remaining_s = deadline - time.monotonic()

                if remaining_s <= 0:
                    return None, "timeout waiting for reply"

                try:
                    reply_client_id, message, reason = self.mon_incoming_queue.get(
                        timeout=remaining_s
                    )
                except queue.Empty:
                    return None, "timeout waiting for reply"

                if message is None:
                    return None, reason

                if reply_client_id == client_id and message.in_reply_to == in_reply_to:
                    return message, "ok"

                deferred_messages.append((reply_client_id, message, reason))

        finally:
            for deferred_message in deferred_messages:
                self.mon_incoming_queue.put(deferred_message)

    def _monitoring_io_loop(self) -> None:
        """
        Own the ROUTER socket after completion of the handshake.

        All post-handshake socket receive/send operations are performed by
        this thread. Other threads communicate through the queues.
        """

        while not self.mon_stop_listening.is_set():
            client_id, message, reason = self.receive_message(timeout_ms=100)

            if message is not None:
                if message.msg_type == MessageType.EVENT:
                    self.mon_event_queue.put((client_id, message))
                else:
                    self.mon_incoming_queue.put((client_id, message, reason))

            elif reason != "timeout elapsed":
                self.logger.warning(f"Monitoring receive problem: {reason}")

            while True:
                try:
                    client_id_out, outgoing_message = self.mon_outgoing_queue.get_nowait()
                except queue.Empty:
                    break

                if not self.is_client_connected(client_id_out):
                    self.logger.error(
                        f"Cannot send queued monitoring message: client {client_id_out!r} is not connected"
                    )
                    continue

                if not self.send_message(client_id=client_id_out, message=outgoing_message):
                    self.logger.error(
                        f"Failed to send queued monitoring message to {client_id_out!r}: "
                        f"request_id={outgoing_message.request_id}"
                    )

    def _handle_main_event(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> None:

        client_name = client_id.decode(errors="ignore")
        payload = message.payload or {}

        event = payload.get("event")
        details = payload.get("details", {})

        if event == "sensor_threshold_exceeded":

            sensor = details.get("sensor", "unknown")
            value = details.get("value")
            min_value = details.get("min")
            max_value = details.get("max")
            direction = details.get("direction")

            self.logger.warning(
                "MAIN sensor threshold exceeded: "
                f"client={client_name}, "
                f"sensor={sensor}, "
                f"value={value}, "
                f"direction={direction}, "
                f"min={min_value}, "
                f"max={max_value}"
            )

            return

        if event == "sensor_threshold_recovered":

            sensor = details.get("sensor", "unknown")
            value = details.get("value")

            self.logger.info(
                "MAIN sensor threshold recovered: "
                f"client={client_name}, "
                f"sensor={sensor}, "
                f"value={value}"
            )

            return

        if event == "motion_detected":

            sensor = details.get(
                "sensor",
                "bmi270",
            )

            self.logger.warning(
                "MAIN motion detected: "
                f"client={client_name}, "
                f"sensor={sensor}"
            )

            return

        self.logger.info(
            f"Unknown MAIN monitoring event from "
            f"{client_name}: {payload}"
        )
    
    
    
    def _event_loop(self) -> None:

        while not self.mon_stop_listening.is_set():

            try:
                client_id, message = (
                    self.mon_event_queue.get(
                        timeout=0.5
                    )
                )

            except queue.Empty:
                continue

            try:

                if self.mon_event_callback is not None:
                    try:
                        self.mon_event_callback(
                            client_id,
                            message,
                        )

                    except Exception as exc:
                        self.logger.error(
                            f"Monitoring event callback failed: {exc}"
                        )

                if message.channel == Channel.MAIN:
                    self._handle_main_event(
                        client_id,
                        message,
                    )
                    continue


                client_name = client_id.decode(
                    errors="ignore"
                )

                payload = message.payload or {}
                event = payload.get(
                    "event",
                    "unknown_event",
                )

                severity = payload.get(
                    "severity",
                    "info",
                )

                try:
                    channel_name = (
                        message.channel.value
                    )
                except AttributeError:
                    channel_name = str(
                        message.channel
                    )

                log_message = (
                    f"{channel_name} event on MonitoringPlane "
                    f"from {client_name}: "
                    f"{event} - {payload}"
                )

                if severity == "warning":
                    self.logger.warning(
                        log_message
                    )

                elif severity == "error":
                    self.logger.error(
                        log_message
                    )

                else:
                    self.logger.info(
                        log_message
                    )

            finally:
                self.mon_event_queue.task_done()

    def start_listener(self) -> bool:
        if self.socket is None:
            self.logger.error("Cannot start monitoring listener: socket not initialized")
            return False

        if (
            self.mon_listener_thread is not None
            and self.mon_listener_thread.is_alive()
        ):
            self.logger.warning("Monitoring listener already running")
            return True

        self.mon_stop_listening.clear()

        self.mon_listener_thread = threading.Thread(
            target=self._monitoring_io_loop,
            daemon=True,
            name="monitoring-plane-io",
        )
        self.mon_listener_thread.start()

        if (
            self.mon_event_thread is None
            or not self.mon_event_thread.is_alive()
        ):
            self.mon_event_thread = threading.Thread(
                target=self._event_loop,
                daemon=True,
                name="monitoring-plane-events",
            )
            self.mon_event_thread.start()

        self.logger.info("Monitoring listener started")
        return True

    def stop_listener(self) -> None:
        self.mon_stop_listening.set()

        if (
            self.mon_listener_thread is not None
            and self.mon_listener_thread.is_alive()
        ):
            self.mon_listener_thread.join(timeout=2.0)

        if (
            self.mon_event_thread is not None
            and self.mon_event_thread.is_alive()
        ):
            self.mon_event_thread.join(timeout=2.0)

        self.logger.info("Monitoring listener stopped")

    def clear_queues(self) -> None:
        while True:
            try:
                self.mon_incoming_queue.get_nowait()
            except queue.Empty:
                break

        while True:
            try:
                self.mon_outgoing_queue.get_nowait()
            except queue.Empty:
                break

        while True:
            try:
                self.mon_event_queue.get_nowait()
            except queue.Empty:
                break

    def close_connection(self) -> None:
        self.stop_listener()
        self.clear_clients()
        self.clear_queues()

        if self.socket is not None:
            try:
                self.recv_poller.unregister(self.socket)
            except Exception:
                pass

            try:
                self.socket.setsockopt(zmq.LINGER, 0)
                self.socket.close()

            except Exception as exc:
                self.logger.warning(
                    "Error while closing monitoring socket: "
                    f"{exc}"
                )

            finally:
                self.socket = None
                self.endpoint = None

        self.logger.info("Monitoring connection closed")