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


class AcquisitionPlaneManager:
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
        return self.server_state.list_acquisition_clients()

    def is_client_connected(self, client_id: bytes) -> bool:
        return client_id in self.server_state.list_acquisition_clients()


    def add_client(self, client_id: bytes) -> None:
        try:
            self.server_state.add_acquisition_client(client_id)
        except ValueError as e:
            self.logger.error(f"Cannot register acquisition client {client_id!r}: {e}")
            return

    def remove_client(self, client_id: bytes) -> None:
        self.server_state.remove_acquisition_client(client_id)
        self.logger.info(f"AcquisitionPlane client removed: {client_id!r}")

    def clear_clients(self) -> None:
        self.server_state.clear_acquisition_clients()
        self.logger.debug("AcquisitionPlane client registry cleared")


    def get_identity(self, client_id: bytes) -> Optional[dict]:
        return self.server_state.get_identity(client_id)


    def start_connection(self, port: int) -> bool:
        """Start the AcquisitionPlane ROUTER socket."""

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

            self.logger.info(
                f"AcquisitionPlaneManager started on port {port}"
            )
            return True

        except zmq.ZMQError as exc:
            self.socket = None
            self.endpoint = None

            self.logger.error(
                "ZMQ Exception: failed to bind acquisition socket "
                f"on port {port}: {exc}"
            )
            return False

        except Exception as exc:
            self.socket = None
            self.endpoint = None

            self.logger.error(
                "Generic Exception: failed to bind acquisition socket "
                f"on port {port}: {exc}"
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
                self.logger.error(
                    f"Invalid multipart message format: {frames}"
                )
                return None, None, "invalid multipart format"

            client_id = frames[0]
            raw_message = frames[-1]

            message, reason = self.message_handler.deserialize(raw_message)

            if message is None:
                self.logger.error(
                    "Failed to deserialize acquisition message "
                    f"from client {client_id!r}: {reason}"
                )
                return client_id, None, reason

            self.logger.debug(
                f"Received acquisition message from client {client_id!r}: "
                f"type={message.msg_type.value}, "
                f"request_id={message.request_id}"
            )

            return client_id, message, "ok"

        except zmq.ZMQError as exc:
            self.logger.error(
                f"ZMQ error while receiving acquisition message: {exc}"
            )
            return None, None, f"zmq error: {exc}"

        except Exception as exc:
            self.logger.error(
                "Unexpected error while receiving acquisition message: "
                f"{exc}"
            )
            return None, None, f"unexpected error: {exc}"

    def send_message(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        if self.socket is None:
            self.logger.error(
                "Cannot send message: acquisition socket not initialized"
            )
            return False

        try:
            message_raw = self.message_handler.serialize(message)

            if not message_raw:
                self.logger.error(
                    "Serialization failed for acquisition message "
                    f"with request_id={message.request_id}"
                )
                return False

            self.socket.send_multipart([client_id, message_raw])

            self.logger.debug(
                f"Sent acquisition message to client {client_id!r}: "
                f"type={message.msg_type.value}, "
                f"request_id={message.request_id}"
            )

            return True

        except zmq.ZMQError as exc:
            self.logger.error(
                "ZMQ error while sending acquisition message "
                f"to client {client_id!r}: {exc}"
            )

            if exc.errno == zmq.EHOSTUNREACH:
                self.remove_client(client_id)

            return False

        except Exception as exc:
            self.logger.error(
                "Unexpected error while sending acquisition message "
                f"to client {client_id!r}: {exc}"
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

        if message is None or client_id is None:
            self.logger.error(
                "Acquisition handshake failed while waiting for "
                f"client hello: {reason}"
            )
            return False

        if client_id not in self.server_state.list_connected_clients():
            self.logger.error(
                "Acquisition handshake received from unknown "
                f"ControlPlane client {client_id!r}"
            )
            return False

        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                "Unexpected message type during acquisition handshake "
                f"from {client_id!r}: {message.msg_type}"
            )
            return False

        if message.phase != "acquisition_hello":
            self.logger.error(
                "Unexpected acquisition handshake phase "
                f"from {client_id!r}: {message.phase}"
            )
            return False

        if message.payload.get("message") != "Acq_hello":
            self.logger.error(
                "Unexpected acquisition hello payload "
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

        if not self.send_message(
            client_id=client_id,
            message=acq_response,
        ):
            self.logger.error(
                "Failed to send acquisition ready message "
                f"to client {client_id!r}"
            )
            return False

        self.add_client(client_id)

        identity = self.server_state.get_identity(client_id) or {}
        multipmt_id = identity.get("multipmt_id", "unknown")
        batch_id = identity.get("batch_id", "unknown")

        self.logger.info(
            "Acquisition handshake completed successfully with "
            f"client {client_id!r}, "
            f"multipmt_id={multipmt_id}, "
            f"batch_id={batch_id}, "
            f"acq_mode={self.server_state.get_mode()}"
        )

        return True

    def handshake(self) -> bool:
        target_clients = len(
            self.server_state.list_connected_clients()
        )

        if target_clients == 0:
            self.logger.warning(
                "Cannot start acquisition handshake: "
                "no ControlPlane clients connected"
            )
            return False

        self.logger.info(
            f"Waiting for {target_clients} "
            "AcquisitionPlane client(s) to connect..."
        )

        while len(self.list_connected_clients()) < target_clients:
            retries = 0
            success = False

            while retries < MAX_RETRIES and not success:
                self.logger.info(
                    "Trying acquisition handshake "
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
                    "No more AcquisitionPlane clients connected after "
                    "maximum attempts. Server will remain operative with "
                    "currently connected acquisition clients."
                )
                break

        connected_clients = self.list_connected_clients()

        if connected_clients:
            self.logger.info(
                "Acquisition plane ready. Connected acquisition clients: "
                f"{len(connected_clients)}/{target_clients}"
            )
            return True

        self.logger.error("No AcquisitionPlane clients connected.")
        return False


    def queue_message(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> None:
        self.acq_outgoing_queue.put((client_id, message))

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
                    reply_client_id, message, reason = (
                        self.acq_incoming_queue.get(
                            timeout=remaining_s
                        )
                    )
                except queue.Empty:
                    return None, "timeout waiting for reply"

                if message is None:
                    return None, reason

                if (
                    reply_client_id == client_id
                    and message.in_reply_to == in_reply_to
                ):
                    return message, "ok"

                deferred_messages.append(
                    (reply_client_id, message, reason)
                )

        finally:
            for deferred_message in deferred_messages:
                self.acq_incoming_queue.put(deferred_message)



    def _acquisition_io_loop(self) -> None:
        """
        Own the ROUTER socket after completion of the handshake.

        All post-handshake socket receive/send operations are performed by
        this thread. Other threads communicate through the queues.
        """

        while not self.acq_stop_listening.is_set():
            client_id, message, reason = self.receive_message(
                timeout_ms=100
            )

            if message is not None:
                if message.msg_type == MessageType.EVENT:
                    self.acq_event_queue.put((client_id, message))
                else:
                    self.acq_incoming_queue.put(
                        (client_id, message, reason)
                    )

            elif reason != "timeout elapsed":
                self.logger.warning(
                    f"Acquisition receive problem: {reason}"
                )

            while True:
                try:
                    client_id_out, outgoing_message = (
                        self.acq_outgoing_queue.get_nowait()
                    )
                except queue.Empty:
                    break

                if not self.is_client_connected(client_id_out):
                    self.logger.error(
                        "Cannot send queued acquisition message: "
                        f"client {client_id_out!r} is not connected"
                    )
                    continue

                if not self.send_message(
                    client_id=client_id_out,
                    message=outgoing_message,
                ):
                    self.logger.error(
                        "Failed to send queued acquisition message "
                        f"to {client_id_out!r}: "
                        f"request_id={outgoing_message.request_id}"
                    )

    def _event_loop(self) -> None:
        """
        Process every EVENT received on the AcquisitionPlane.

        The logical channel is preserved: events may concern Acquisition,
        HV, RC, System, or another supported protocol channel.
        """

        while not self.acq_stop_listening.is_set():
            try:
                client_id, message = self.acq_event_queue.get(
                    timeout=0.5
                )
            except queue.Empty:
                continue

            try:
                client_name = client_id.decode(errors="ignore")
                payload = message.payload or {}

                event = payload.get("event", "unknown_event")
                severity = payload.get("severity", "info")

                try:
                    channel_name = message.channel.value
                except AttributeError:
                    channel_name = str(message.channel)

                if self.acq_event_callback is not None:
                    try:
                        self.acq_event_callback(message)
                    except Exception as exc:
                        self.logger.error(
                            f"Acquisition event callback failed: {exc}"
                        )

                log_message = (
                    f"{channel_name} event on AcquisitionPlane "
                    f"from {client_name}: {event} - {payload}"
                )

                if severity == "warning":
                    self.logger.warning(log_message)

                elif severity == "error":
                    self.logger.error(log_message)

                else:
                    self.logger.info(log_message)

            finally:
                self.acq_event_queue.task_done()

    def start_listener(self) -> bool:
        if self.socket is None:
            self.logger.error(
                "Cannot start acquisition listener: "
                "socket not initialized"
            )
            return False

        if (
            self.acq_listener_thread is not None
            and self.acq_listener_thread.is_alive()
        ):
            self.logger.warning(
                "Acquisition listener already running"
            )
            return True

        self.acq_stop_listening.clear()

        self.acq_listener_thread = threading.Thread(
            target=self._acquisition_io_loop,
            daemon=True,
            name="acquisition-plane-io",
        )
        self.acq_listener_thread.start()

        if (
            self.acq_event_thread is None
            or not self.acq_event_thread.is_alive()
        ):
            self.acq_event_thread = threading.Thread(
                target=self._event_loop,
                daemon=True,
                name="acquisition-plane-events",
            )
            self.acq_event_thread.start()

        self.logger.info("Acquisition listener started")
        return True

    def stop_listener(self) -> None:
        self.acq_stop_listening.set()

        if (
            self.acq_listener_thread is not None
            and self.acq_listener_thread.is_alive()
        ):
            self.acq_listener_thread.join(timeout=2.0)

        if (
            self.acq_event_thread is not None
            and self.acq_event_thread.is_alive()
        ):
            self.acq_event_thread.join(timeout=2.0)

        self.logger.info("Acquisition listener stopped")


    def clear_queues(self) -> None:
        while True:
            try:
                self.acq_incoming_queue.get_nowait()
            except queue.Empty:
                break

        while True:
            try:
                self.acq_outgoing_queue.get_nowait()
            except queue.Empty:
                break

        while True:
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

            except Exception as exc:
                self.logger.warning(
                    "Error while closing acquisition socket: "
                    f"{exc}"
                )

            finally:
                self.socket = None
                self.endpoint = None

        self.logger.info("Acquisition connection closed")