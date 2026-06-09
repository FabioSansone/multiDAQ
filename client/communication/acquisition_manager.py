import zmq
from typing import Optional
import threading
import queue
import time

from client.core.client_runtime import ClientRunTime
from client.communication.client_command_map import COMMAND_MAP
from common.message_handler import (
    MessageHandler,
    ProtocolMessage,
    MessageStatus,
    MessageType,
)
from client.utils.logger import get_logger


MAX_RETRIES = 10


class AcquisitionPlaneManager:

    def __init__(self, context: zmq.Context, runtime: ClientRunTime) -> None:
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.recv_poller = zmq.Poller()
        self.acq_endpoint: Optional[str] = None

        self.runtime = runtime
        self.server_ip = runtime.server_ip
        self.identity = runtime.identity

        self.stop_listening = threading.Event()
        self.incoming_queue = queue.Queue()
        self.outgoing_queue = queue.Queue()
        self.listener_thread: Optional[threading.Thread] = None
        self.reconnect_requested = threading.Event()

        self.command_map = COMMAND_MAP

        self.message_handler = MessageHandler(
            logger=get_logger("message_handler")
        )

        self.logger = get_logger("acq_manager")
        self.logger.info("ZMQ Acquisition Client Manager initialized")

    def start_connection(self, port: int) -> bool:
        if self.socket is not None:
            try:
                self.recv_poller.unregister(self.socket)
            except KeyError:
                pass

            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
            self.acq_endpoint = None

        try:
            server_address = f"tcp://{self.server_ip}:{port}"

            self.socket = self.context.socket(zmq.DEALER)

            routing_identity = self.runtime.zmq_identity()
            identity_bytes = routing_identity.encode("utf-8")

            self.logger.info(f"Using acquisition ZMQ identity: {routing_identity}")

            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.IDENTITY, identity_bytes)

            self.socket.connect(server_address)
            self.recv_poller.register(self.socket, zmq.POLLIN)
            self.acq_endpoint = server_address

            self.logger.info(
                f"Acquisition client socket connected to {self.acq_endpoint}"
            )
            return True

        except zmq.ZMQError as e:
            self.socket = None
            self.acq_endpoint = None
            self.logger.error(
                f"ZMQ exception: failed to connect acquisition socket on port {port}: "
                f"{e} with server: {server_address}"
            )
            return False

        except Exception as e:
            self.socket = None
            self.acq_endpoint = None
            self.logger.error(
                f"Generic exception: failed to connect acquisition socket on port {port}: "
                f"{e} with server: {server_address}"
            )
            return False

    def receive_message(self, timeout_ms: int) -> tuple[Optional[ProtocolMessage], str]:

        if self.socket is None:
            self.logger.error("Cannot receive message: acquisition socket not initialized")
            return None, "acquisition socket not initialized"

        try:
            socks = dict(self.recv_poller.poll(timeout=timeout_ms))

            if self.socket not in socks:
                return None, "timeout elapsed"

            raw_message = self.socket.recv()

            if not raw_message:
                self.logger.error("Received empty acquisition message")
                return None, "empty message"

            message, reason = self.message_handler.deserialize(raw_message)

            if message is None:
                self.logger.error(
                    f"Failed to deserialize acquisition message from server: {reason}"
                )
                return None, reason

            self.logger.debug(
                f"Received acquisition message from server: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return message, "ok"

        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error while receiving acquisition message: {e}")
            return None, f"zmq error: {e}"

        except Exception as e:
            self.logger.error(f"Unexpected error while receiving acquisition message: {e}")
            return None, f"unexpected error: {e}"

    def send_message(self, message: ProtocolMessage) -> bool:

        if self.socket is None:
            self.logger.error("Cannot send message: acquisition socket not initialized")
            return False

        if self.listener_thread is not None and self.listener_thread.is_alive():
            if threading.current_thread() != self.listener_thread:
                self.logger.error("send_message called outside acquisition IO thread")
                return False

        try:
            message_raw = self.message_handler.serialize(message)

            if not message_raw:
                self.logger.error(
                    f"Serialization failed for acquisition message "
                    f"with request_id={message.request_id}"
                )
                return False

            self.socket.send(message_raw)

            self.logger.debug(
                f"Sent acquisition message to server: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return True

        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error while sending acquisition message to server: {e}")
            return False

        except Exception as e:
            self.logger.error(
                f"Unexpected error while sending acquisition message to server: {e}"
            )
            return False

    def handshake_core(self, timeout_ms: int = 20000) -> bool:
        if self.socket is None:
            self.logger.error(
                "Communication not yet established. Cannot perform acquisition handshake."
            )
            return False

        acq_hello_message = self.message_handler.create_handshake(
            phase="acquisition_hello",
            payload={"message": "Acq_hello"},
            sender="client",
            status=MessageStatus.OK,
        )

        if not self.send_message(message=acq_hello_message):
            self.logger.error("Failed to send acquisition hello message")
            return False

        message, reason = self.receive_message(timeout_ms)

        if message is None:
            self.logger.error(
                f"Acquisition handshake failed while waiting for server acquisition ready: {reason}"
            )
            return False

        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                f"Unexpected message type during acquisition handshake from server "
                f"{self.acq_endpoint}: {message.msg_type}"
            )
            return False

        if message.phase != "acquisition_ready":
            self.logger.error(
                f"Unexpected acquisition handshake phase from server "
                f"{self.acq_endpoint}: {message.phase}"
            )
            return False

        if message.payload.get("message") != "Acq_alive":
            self.logger.error(
                f"Unexpected acquisition handshake payload from server "
                f"{self.acq_endpoint}: {message.payload}"
            )
            return False

        if message.in_reply_to != acq_hello_message.request_id:
            self.logger.error(
                f"Acquisition ready message does not match expected hello request: "
                f"{message.in_reply_to} != {acq_hello_message.request_id}"
            )
            return False

        self.logger.info(
            f"Acquisition handshake completed successfully with server {self.acq_endpoint}. "
            f"Acquisition mode: {self.runtime.acq_mode}, "
            f"identity: {self.identity.hostname}"
        )

        return True

    def handshake(
        self,
        timeout_ms: int = 20000,
        retry_delay_s: float = 1.0,
        max_retries: Optional[int] = MAX_RETRIES,
    ) -> bool:
        """
        Attempt the acquisition-plane handshake repeatedly.

        If max_retries is None, retry indefinitely.
        """

        if self.socket is None:
            self.logger.error("Cannot start acquisition handshake: socket not initialized")
            return False

        attempt = 0

        while max_retries is None or attempt < max_retries:
            attempt += 1

            if max_retries is None:
                self.logger.info(
                    f"Acquisition handshake attempt {attempt} "
                    f"(retrying until success)"
                )
            else:
                self.logger.info(
                    f"Acquisition handshake attempt {attempt}/{max_retries}"
                )

            if self.handshake_core(timeout_ms=timeout_ms):
                return True

            self.logger.warning("Acquisition handshake attempt failed, retrying...")
            time.sleep(retry_delay_s)

        self.logger.error("Acquisition handshake failed after maximum number of attempts")
        return False

    def queue_message(self, message: ProtocolMessage) -> None:
        self.outgoing_queue.put(message)

    def _acquisition_io_loop(self) -> None:

        while not self.stop_listening.is_set():

            message, reason = self.receive_message(timeout_ms=100)

            if message is not None:
                self.incoming_queue.put((message, reason))

            elif reason != "timeout elapsed":
                self.logger.warning(f"Acquisition receive problem: {reason}")

            while True:
                try:
                    outgoing_message = self.outgoing_queue.get_nowait()
                except queue.Empty:
                    break

                if not self.send_message(outgoing_message):
                    self.logger.error(
                        f"Failed to send queued acquisition message: "
                        f"request_id={outgoing_message.request_id}"
                    )

    def start_listener(self) -> bool:

        if self.socket is None:
            self.logger.error("Cannot start acquisition listener: socket not initialized")
            return False

        if self.listener_thread and self.listener_thread.is_alive():
            self.logger.warning("Acquisition listener already running")
            return True

        self.stop_listening.clear()

        self.listener_thread = threading.Thread(
            target=self._acquisition_io_loop,
            daemon=True,
        )
        self.listener_thread.start()

        self.logger.info("Acquisition listener started")
        return True

    def stop_listener(self) -> None:
        self.stop_listening.set()

        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=2.0)

        self.logger.info("Acquisition listener stopped")

    def handle_commands(self) -> None:
        """
        Main acquisition command dispatcher.
        Reads messages from incoming_queue and handles server commands.
        """

        while not self.stop_listening.is_set():
            try:
                message, reason = self.incoming_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if message is None:
                self.logger.warning(f"Invalid queued acquisition message: {reason}")
                continue

            if message.msg_type != MessageType.COMMAND:
                self.logger.warning(
                    f"Unexpected message type in acquisition command handler: "
                    f"{message.msg_type}"
                )
                continue

            handler = self.command_map.get(message.command)

            if handler is None:
                self.logger.warning(f"Unknown acquisition command: {message.command}")
                continue

            try:
                handler(self, message)
            except Exception as e:
                self.logger.error(
                    f"Error handling acquisition command {message.command}: {e}"
                )

    def clear_queues(self) -> None:
        while not self.incoming_queue.empty():
            try:
                self.incoming_queue.get_nowait()
            except queue.Empty:
                break

        while not self.outgoing_queue.empty():
            try:
                self.outgoing_queue.get_nowait()
            except queue.Empty:
                break

    def close_connection(self) -> None:
        """Close only the current acquisition socket connection."""

        self.stop_listener()
        self.clear_queues()

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
                self.acq_endpoint = None

        self.logger.info("Acquisition connection closed")

    def close(self) -> None:
        self.close_connection()
        self.logger.info("AcquisitionPlaneManager closed")