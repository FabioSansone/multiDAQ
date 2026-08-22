import zmq
from typing import Optional
import threading
import queue
import time

from client.core.client_runtime import ClientRunTime
from client.communication.client_command_map import COMMAND_MAP
from common.message_handler import (
    Channel,
    MessageHandler,
    ProtocolMessage,
    MessageStatus,
    MessageType,
)
from client.utils.logger import get_logger


MAX_RETRIES = 10


class MonitoringPlaneManager:

    def __init__(self, context: zmq.Context, runtime: ClientRunTime) -> None:

        self.plane_name = "monitoring"
        
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.recv_poller = zmq.Poller()
        self.mon_endpoint: Optional[str] = None

        self.runtime = runtime
        self.server_ip = runtime.server_ip
        self.identity = runtime.identity

        self.stop_listening = threading.Event()
        self.incoming_queue = queue.Queue()
        self.outgoing_queue = queue.Queue()
        self.listener_thread: Optional[threading.Thread] = None
        self.reconnect_requested = threading.Event()
        
        self.sensors_warning_thread: Optional[threading.Thread] = None

        self.command_map = COMMAND_MAP

        self.message_handler = MessageHandler(
            logger=get_logger("message_handler")
        )

        self.logger = get_logger("mon_manager")
        self.logger.info("ZMQ Monitoring Client Manager initialized")

    def start_connection(self, port: int) -> bool:
        if self.socket is not None:
            try:
                self.recv_poller.unregister(self.socket)
            except KeyError:
                pass

            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
            self.mon_endpoint = None

        try:
            server_address = f"tcp://{self.server_ip}:{port}"

            self.socket = self.context.socket(zmq.DEALER)

            routing_identity = self.runtime.zmq_identity()
            identity_bytes = routing_identity.encode("utf-8")

            self.logger.info(f"Using monitoring ZMQ identity: {routing_identity}")

            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.IDENTITY, identity_bytes)

            self.socket.connect(server_address)
            self.recv_poller.register(self.socket, zmq.POLLIN)
            self.mon_endpoint = server_address

            self.logger.info(
                f"Monitoring client socket connected to {self.mon_endpoint}"
            )
            return True

        except zmq.ZMQError as e:
            self.socket = None
            self.mon_endpoint = None
            self.logger.error(
                f"ZMQ exception: failed to connect monitoring socket on port {port}: "
                f"{e} with server: {server_address}"
            )
            return False

        except Exception as e:
            self.socket = None
            self.mon_endpoint = None
            self.logger.error(
                f"Generic exception: failed to connect monitoring socket on port {port}: "
                f"{e} with server: {server_address}"
            )
            return False

    def receive_message(self, timeout_ms: int) -> tuple[Optional[ProtocolMessage], str]:

        if self.socket is None:
            self.logger.error("Cannot receive message: monitoring socket not initialized")
            return None, "monitoring socket not initialized"

        try:
            socks = dict(self.recv_poller.poll(timeout=timeout_ms))

            if self.socket not in socks:
                return None, "timeout elapsed"

            raw_message = self.socket.recv()

            if not raw_message:
                self.logger.error("Received empty monitoring message")
                return None, "empty message"

            message, reason = self.message_handler.deserialize(raw_message)

            if message is None:
                self.logger.error(
                    f"Failed to deserialize monitoring message from server: {reason}"
                )
                return None, reason

            self.logger.debug(
                f"Received monitoring message from server: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return message, "ok"

        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error while receiving monitoring message: {e}")
            return None, f"zmq error: {e}"

        except Exception as e:
            self.logger.error(f"Unexpected error while receiving monitoring message: {e}")
            return None, f"unexpected error: {e}"

    def send_message(self, message: ProtocolMessage) -> bool:

        if self.socket is None:
            self.logger.error("Cannot send message: monitoring socket not initialized")
            return False

        if self.listener_thread is not None and self.listener_thread.is_alive():
            if threading.current_thread() != self.listener_thread:
                self.logger.error("send_message called outside monitoring IO thread")
                return False

        try:
            message_raw = self.message_handler.serialize(message)

            if not message_raw:
                self.logger.error(
                    f"Serialization failed for monitoring message "
                    f"with request_id={message.request_id}"
                )
                return False

            self.socket.send(message_raw)

            self.logger.debug(
                f"Sent monitoring message to server: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return True

        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error while sending monitoring message to server: {e}")
            return False

        except Exception as e:
            self.logger.error(
                f"Unexpected error while sending monitoring message to server: {e}"
            )
            return False

    def handshake_core(self, timeout_ms: int = 20000) -> bool:
        if self.socket is None:
            self.logger.error(
                "Communication not yet established. Cannot perform monitoring handshake."
            )
            return False

        mon_hello_message = self.message_handler.create_handshake(
            phase="monitoring_hello",
            payload={"message": "Mon_hello"},
            sender="client",
            status=MessageStatus.OK,
        )

        if not self.send_message(message=mon_hello_message):
            self.logger.error("Failed to send monitoring hello message")
            return False

        message, reason = self.receive_message(timeout_ms)

        if message is None:
            self.logger.error(
                f"Monitoring handshake failed while waiting for server monitoring ready: {reason}"
            )
            return False

        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                f"Unexpected message type during monitoring handshake from server "
                f"{self.mon_endpoint}: {message.msg_type}"
            )
            return False

        if message.phase != "monitoring_ready":
            self.logger.error(
                f"Unexpected monitoring handshake phase from server "
                f"{self.mon_endpoint}: {message.phase}"
            )
            return False

        if message.payload.get("message") != "Mon_alive":
            self.logger.error(
                f"Unexpected monitoring handshake payload from server "
                f"{self.mon_endpoint}: {message.payload}"
            )
            return False

        if message.in_reply_to != mon_hello_message.request_id:
            self.logger.error(
                f"Monitoring ready message does not match expected hello request: "
                f"{message.in_reply_to} != {mon_hello_message.request_id}"
            )
            return False

        self.logger.info(
            f"Monitoring handshake completed successfully with server {self.mon_endpoint}. "
            f"Identity: {self.identity.hostname}"
        )

        return True

    def handshake(
        self,
        timeout_ms: int = 20000,
        retry_delay_s: float = 1.0,
        max_retries: Optional[int] = MAX_RETRIES,
    ) -> bool:
        """
        Attempt the monitoring-plane handshake repeatedly.

        If max_retries is None, retry indefinitely.
        """

        if self.socket is None:
            self.logger.error("Cannot start monitoring handshake: socket not initialized")
            return False

        attempt = 0

        while max_retries is None or attempt < max_retries:
            attempt += 1

            if max_retries is None:
                self.logger.info(
                    f"Monitoring handshake attempt {attempt} "
                    f"(retrying until success)"
                )
            else:
                self.logger.info(
                    f"Monitoring handshake attempt {attempt}/{max_retries}"
                )

            if self.handshake_core(timeout_ms=timeout_ms):
                return True

            self.logger.warning("Monitoring handshake attempt failed, retrying...")
            time.sleep(retry_delay_s)

        self.logger.error("Monitoring handshake failed after maximum number of attempts")
        return False

    def queue_message(self, message: ProtocolMessage) -> None:
        self.outgoing_queue.put(message)

    def _monitoring_io_loop(self) -> None:

        while not self.stop_listening.is_set():

            message, reason = self.receive_message(timeout_ms=100)

            if message is not None:
                self.incoming_queue.put((message, reason))

            elif reason != "timeout elapsed":
                self.logger.warning(f"Monitoring receive problem: {reason}")

            while True:
                try:
                    outgoing_message = self.outgoing_queue.get_nowait()
                except queue.Empty:
                    break

                if not self.send_message(outgoing_message):
                    self.logger.error(
                        f"Failed to send queued monitoring message: "
                        f"request_id={outgoing_message.request_id}"
                    )

    def start_listener(self) -> bool:

        if self.socket is None:
            self.logger.error("Cannot start monitoring listener: socket not initialized")
            return False

        if self.listener_thread and self.listener_thread.is_alive():
            self.logger.warning("Monitoring listener already running")
            return True

        self.stop_listening.clear()

        self.listener_thread = threading.Thread(
            target=self._monitoring_io_loop,
            daemon=True,
        )
        self.listener_thread.start()

        if self.sensors_warning_thread is None or not self.sensors_warning_thread.is_alive():
            self.sensors_warning_thread = threading.Thread(
                target=self._main_warning_loop,
                daemon=True,
            )
            self.sensors_warning_thread.start()

        self.logger.info("Monitoring listener started")
        return True

    def stop_listener(self) -> None:
        self.stop_listening.set()

        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=2.0)

        if self.sensors_warning_thread and self.sensors_warning_thread.is_alive():
            self.sensors_warning_thread.join(timeout=2.0)

        self.logger.info("Monitoring listener stopped")

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
                self.logger.warning(f"Invalid queued monitoring message: {reason}")
                continue

            if message.msg_type != MessageType.COMMAND:
                self.logger.warning(
                    f"Unexpected message type in monitoring command handler: "
                    f"{message.msg_type}"
                )
                continue

            handler = self.command_map.get(message.command)

            if handler is None:
                self.logger.warning(f"Unknown monitoring command: {message.command}")
                continue

            try:
                handler(self, message)
            except Exception as e:
                self.logger.error(
                    f"Error handling monitoring command {message.command}: {e}"
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
        """Close only the current monitoring socket connection."""

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
                self.logger.warning(f"Error while closing monitoring socket: {e}")
            finally:
                self.socket = None
                self.mon_endpoint = None

        self.logger.info("Monitoring connection closed")

    def close(self) -> None:
        self.close_connection()
        self.logger.info("MonitoringPlaneManager closed")
        
    def _main_warning_loop(self) -> None:
        while not self.stop_listening.is_set():
            
            main_service = self.runtime.main_service

            if main_service is None:
                self.stop_listening.wait(0.5)
                continue

            try:
                warning = main_service.warning_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            
            try:
                event_message = self.message_handler.create_event(
                    channel=Channel.MAIN,
                    payload=warning,
                    sender="client",
                    status=MessageStatus.OK,
                )

                self.queue_message(event_message)
            
            except Exception as e:
                self.logger.error(f"Failed to create MAIN monitoring event: {e}")