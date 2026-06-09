import zmq
import queue
import threading
from typing import Optional, List

from server.core.server_state import ServerState
from common.message_handler import (
    MessageHandler,
    ProtocolMessage,
    MessageStatus,
    MessageType,
    Channel,
)
from server.utils.logger import get_logger


MAX_RETRIES = 5


class AcquisitionPlaneManager:

    def __init__(self, context: zmq.Context, state: ServerState):
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.endpoint: Optional[str] = None
        self.recv_poller = zmq.Poller()

        self.server_state = state

        self.acquisition_clients: List[bytes] = []

        self.message_handler = MessageHandler(
            logger=get_logger("message_handler")
        )

        self.acq_incoming_queue = queue.Queue()
        self.acq_outgoing_queue = queue.Queue()
        self.acq_event_queue = queue.Queue()

        self.acq_stop_listening = threading.Event()
        self.acq_listener_thread: Optional[threading.Thread] = None

        self.acq_event_thread: Optional[threading.Thread] = None
        self.acq_event_callback = None

        self.logger = get_logger("acquisition_manager")
        self.logger.debug("ZMQ Acquisition Server Manager initialized")

    def list_connected_clients(self) -> List[bytes]:
        return self.server_state.list_connected_clients()

    def get_identity(self, client_id: bytes) -> Optional[dict]:
        return self.server_state.get_identity(client_id)

    def start_connection(self, port: int) -> bool:
        """Start acquisition ROUTER socket."""

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

            self.logger.info(f"AcquisitionPlaneManager started on port {port}")
            return True

        except zmq.ZMQError as e:
            self.socket = None
            self.endpoint = None
            self.logger.error(
                f"ZMQ Exception: failed to bind acquisition socket on port {port}: {e}"
            )
            return False

        except Exception as e:
            self.socket = None
            self.endpoint = None
            self.logger.error(
                f"Generic Exception: failed to bind acquisition socket on port {port}: {e}"
            )
            return False

    def receive_message(
        self,
        timeout_ms: int,
    ) -> tuple[Optional[bytes], Optional[ProtocolMessage], str]:

        if self.socket is None:
            self.logger.error(
                "Cannot receive message: acquisition socket not initialized"
            )
            return None, None, "acquisition socket not initialized"

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
                    f"Failed to deserialize acquisition message "
                    f"from client {client_id!r}: {reason}"
                )
                return client_id, None, reason

            self.logger.debug(
                f"Received acquisition message from client {client_id!r}: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return client_id, message, "ok"

        except zmq.ZMQError as e:
            self.logger.error(
                f"ZMQ error while receiving acquisition message: {e}"
            )
            return None, None, f"zmq error: {e}"

        except Exception as e:
            self.logger.error(
                f"Unexpected error while receiving acquisition message: {e}"
            )
            return None, None, f"unexpected error: {e}"

    def send_message(self, client_id: bytes, message: ProtocolMessage) -> bool:

        if self.socket is None:
            self.logger.error(
                "Cannot send message: acquisition socket not initialized"
            )
            return False

        try:
            message_raw = self.message_handler.serialize(message)

            if not message_raw:
                self.logger.error(
                    f"Serialization failed for acquisition message "
                    f"with request_id={message.request_id}"
                )
                return False

            self.socket.send_multipart([client_id, message_raw])

            self.logger.debug(
                f"Sent acquisition message to client {client_id!r}: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return True

        except zmq.ZMQError as e:
            self.logger.error(
                f"ZMQ error while sending acquisition message "
                f"to client {client_id!r}: {e}"
            )
            return False

        except Exception as e:
            self.logger.error(
                f"Unexpected error while sending acquisition message "
                f"to client {client_id!r}: {e}"
            )
            return False

    def handshake_core(self, timeout_ms: int = 20000) -> bool:

        if self.socket is None:
            self.logger.error(
                "Communication not yet established. "
                "Cannot perform acquisition handshake."
            )
            return False

        client_id, message, reason = self.receive_message(timeout_ms)

        if message is None:
            self.logger.error(
                f"Acquisition handshake failed while waiting for client hello: {reason}"
            )
            return False

        if client_id not in self.server_state.list_connected_clients():
            self.logger.error(
                f"Acquisition handshake received from unknown client {client_id!r}"
            )
            return False

        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                f"Unexpected message type during acquisition handshake "
                f"from {client_id!r}: {message.msg_type}"
            )
            return False

        if message.phase != "acquisition_hello":
            self.logger.error(
                f"Unexpected acquisition handshake phase "
                f"from {client_id!r}: {message.phase}"
            )
            return False

        if message.payload.get("message") != "Acq_hello":
            self.logger.error(
                f"Unexpected acquisition hello payload "
                f"from {client_id!r}: {message.payload}"
            )
            return False

        acq_response = self.message_handler.create_handshake(
            phase="acquisition_ready",
            payload={"message": "Acq_alive"},
            in_reply_to=message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=acq_response):
            self.logger.error(
                f"Failed to send acquisition ready message to client {client_id!r}"
            )
            return False

        if client_id not in self.acquisition_clients:
            self.acquisition_clients.append(client_id)

        identity = self.server_state.get_identity(client_id) or {}
        multipmt_id = identity.get("multipmt_id", "unknown")
        batch_id = identity.get("batch_id", "unknown")

        self.logger.info(
            f"Acquisition handshake completed successfully with client {client_id!r}, "
            f"multipmt_id={multipmt_id}, batch_id={batch_id}, "
            f"acq_mode={self.server_state.get_mode()}"
        )

        return True

    def handshake(self) -> bool:

        target_clients = len(self.server_state.list_connected_clients())

        if target_clients == 0:
            self.logger.warning(
                "Cannot start acquisition handshake: "
                "no control-plane clients connected"
            )
            return False

        self.logger.info(
            f"Waiting for {target_clients} acquisition-plane client(s) to connect..."
        )

        while len(self.acquisition_clients) < target_clients:
            retries = 0
            success = False

            while retries < MAX_RETRIES and not success:
                self.logger.info(
                    f"Trying acquisition handshake "
                    f"(attempt {retries + 1}/{MAX_RETRIES})"
                )

                success = self.handshake_core(timeout_ms=20000)

                if not success:
                    retries += 1
                    self.logger.warning(
                        "Acquisition handshake attempt failed, retrying..."
                    )

            if not success:
                self.logger.warning(
                    "No more acquisition clients connected after maximum attempts. "
                    "Server will remain operative with currently connected "
                    "acquisition clients."
                )
                break

        if self.acquisition_clients:
            self.logger.info(
                f"Acquisition plane ready. Connected acquisition clients: "
                f"{len(self.acquisition_clients)}/{target_clients}"
            )
            return True

        self.logger.error("No acquisition clients connected.")
        return False

    def queue_message(self, client_id: bytes, message: ProtocolMessage) -> None:
        self.acq_outgoing_queue.put((client_id, message))

    def wait_for_reply(
        self,
        *,
        client_id: bytes,
        in_reply_to: str,
        timeout_s: float = 10.0,
    ) -> tuple[Optional[ProtocolMessage], str]:

        try:
            while True:
                reply_client_id, message, reason = self.acq_incoming_queue.get(
                    timeout=timeout_s
                )

                if message is None:
                    return None, reason

                if reply_client_id != client_id:
                    self.acq_incoming_queue.put((reply_client_id, message, reason))
                    continue

                if message.in_reply_to != in_reply_to:
                    self.acq_incoming_queue.put((reply_client_id, message, reason))
                    continue

                return message, "ok"

        except queue.Empty:
            return None, "timeout waiting for reply"

    def _acquisition_io_loop(self) -> None:

        while not self.acq_stop_listening.is_set():

            client_id, message, reason = self.receive_message(timeout_ms=100)

            if message is not None:
                if message.msg_type == MessageType.EVENT:
                    self.acq_event_queue.put((client_id, message,))
                else:
                    self.acq_incoming_queue.put((client_id, message, reason))

            elif reason != "timeout elapsed":
                self.logger.warning(f"Receive problem: {reason}")

            while True:
                try:
                    client_id_out, outgoing_message = (
                        self.acq_outgoing_queue.get_nowait()
                    )
                except queue.Empty:
                    break

                if not self.send_message(
                    client_id=client_id_out,
                    message=outgoing_message,
                ):
                    self.logger.error(
                        f"Failed to send queued message to {client_id_out!r}: "
                        f"request_id={outgoing_message.request_id}"
                    )

    def _event_loop(self) -> None:
        while not self.acq_stop_listening.is_set():
            try:
                client_id, message = self.acq_event_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            client_name = client_id.decode(errors="ignore")
            payload = message.payload

            if message.channel == Channel.ACQUISITION:
                event = payload.get("event", "unknown_event")
                severity = payload.get("severity", "info")

                if self.acq_event_callback is not None:
                    try:
                        self.acq_event_callback(message)
                    except Exception as e:
                        self.logger.error(f"Event callback failed: {e}")

                if severity == "warning":
                    self.logger.warning(
                        f"Acquisition warning from {client_name}: {event} - {payload}"
                    )
                else:
                    self.logger.info(
                        f"Acquisition event from {client_name}: {event} - {payload}"
                    )

            self.acq_event_queue.task_done()

    def start_listener(self) -> bool:

        if self.socket is None:
            self.logger.error("Cannot start acquisition listener: socket not initialized")
            return False

        if (
            self.acq_listener_thread
            and self.acq_listener_thread.is_alive()
        ):
            self.logger.warning("Acquisition listener already running")
            return True

        self.acq_stop_listening.clear()

        self.acq_listener_thread = threading.Thread(
            target=self._acquisition_io_loop,
            daemon=True,
        )
        self.acq_listener_thread.start()

        if (
            self.acq_event_thread is None
            or not self.acq_event_thread.is_alive()
        ):
            self.acq_event_thread = threading.Thread(
                target=self._event_loop,
                daemon=True,
            )
            self.acq_event_thread.start()

        self.logger.info("Acquisition listener started")
        return True

    def stop_listener(self) -> None:
        self.acq_stop_listening.set()

        if (
            self.acq_listener_thread
            and self.acq_listener_thread.is_alive()
        ):
            self.acq_listener_thread.join(timeout=2.0)

        if (
            self.acq_event_thread
            and self.acq_event_thread.is_alive()
        ):
            self.acq_event_thread.join(timeout=2.0)

        self.logger.info("Acquisition listener stopped")

    def clear_clients(self) -> None:
        self.acquisition_clients.clear()

    def clear_queues(self) -> None:
        while not self.acq_incoming_queue.empty():
            try:
                self.acq_incoming_queue.get_nowait()
            except queue.Empty:
                break

        while not self.acq_outgoing_queue.empty():
            try:
                self.acq_outgoing_queue.get_nowait()
            except queue.Empty:
                break

        while not self.acq_event_queue.empty():
            try:
                self.acq_event_queue.get_nowait()
            except queue.Empty:
                break

    def close_connection(self) -> None:
        self.stop_listener()
        self.clear_clients()

        if self.socket is not None:
            try:
                self.recv_poller.unregister(self.socket)
            except Exception:
                pass

            try:
                self.socket.setsockopt(zmq.LINGER, 0)
                self.socket.close()
            except Exception as e:
                self.logger.warning(f"Error while closing acquisition socket: {e}")
            finally:
                self.socket = None
                self.endpoint = None

        self.logger.info("Acquisition connection closed")