import zmq
from typing import Optional
import time
import threading
import queue
from client.utils.logger import get_logger
from client.communication.identity import ClientIdentity
from common.message_handler import MessageHandler, ProtocolMessage, MessageStatus, MessageType, Channel
from common.constants import ACQUISITION_MODES
from client.communication.client_command_map import COMMAND_MAP
from client.hardware.hv.hv_service import HVService
from client.hardware.rc.rc_service import RCService
from client.hardware.evproducer.ev_service import EVService
from client.acquisition.acquisition_service import AcquisitionService




MAX_RETRIES = 10

class ControlPlaneManager:
    def __init__(self, context: zmq.Context, server_ip: str, identity: ClientIdentity, hv_port: str) -> None:
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.recv_poller = zmq.Poller()
        self.server_endpoint: Optional[str] = None
        
        self.server_ip = server_ip
        self.identity = identity
        self.hv_port = hv_port

        self.stop_listening = threading.Event()
        self.incoming_queue = queue.Queue()
        self.outgoing_queue = queue.Queue()
        self.listener_thread: Optional[threading.Thread] = None
        self.reconnect_requested = threading.Event()

        self.command_map = COMMAND_MAP
        
        self.message_handler = MessageHandler(logger=get_logger("message_handler"))
        
        self.hv_warning_thread: Optional[threading.Thread] = None
        self.hv_service = None #HVService(hv_port=self.hv_port)
        self.rc_service = RCService()
        self.evproducer = EVService()

        self.acq_info = None
        self.start_thr = None

        self.acquisition_service = AcquisitionService(self)
        
        
        self.logger = get_logger("control_manager")
        self.logger.info("ZMQ Control Client Manager initialized")

    def start_connection(self, port: int) -> bool:
        """Start a ZMQ connection with ROUTER server (ROUTER - DEALER)"""
        if self.socket is not None:
            try:
                self.recv_poller.unregister(self.socket)
            except KeyError:
                pass
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None  
            self.server_endpoint = None
        try:
            server_address = f"tcp://{self.server_ip}:{port}"
            
            self.socket = self.context.socket(zmq.DEALER)
            identity_bytes = self.identity.hostname.encode('utf-8')
            
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.IDENTITY, identity_bytes)
            
            self.socket.connect(server_address)
            self.recv_poller.register(self.socket, zmq.POLLIN)
            self.server_endpoint = server_address
            
            return True
        
        except zmq.ZMQError as e:
            self.socket = None
            self.server_endpoint = None
            self.logger.error(f"ZMQ Exception: failed to connect socket on port {port}: {e} with server: {server_address}") 
            return False
        except Exception as e:
            self.socket = None
            self.server_endpoint = None
            self.logger.error(f"Generic Exception: failed to connect socket on port {port}: {e}  with server: {server_address}")
            return False
    
    
    def receive_message(self, timeout_ms: int) -> tuple[Optional[ProtocolMessage], str]:
        """
        Receive one message from the DEALER socket.

        Returns:
            (message, reason)

            - message: deserialized ProtocolMessage, or None
            - reason: 'ok' on success, otherwise a description of the failure
        """

        if self.socket is None:
            self.logger.error("Cannot receive message: client socket not initialized")
            return None, "client socket not initialized"

        try:
            socks = dict(self.recv_poller.poll(timeout=timeout_ms))
            if self.socket not in socks:
                return None, "timeout elapsed"

            raw_message = self.socket.recv()

            if not raw_message:
                self.logger.error("Received empty message")
                return None, "empty message"

            message, reason = self.message_handler.deserialize(raw_message)

            if message is None:
                self.logger.error(f"Failed to deserialize message from server: {reason}")
                return None, reason

            self.logger.debug(
                f"Received message from server: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return message, "ok"

        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error while receiving message: {e}")
            return None, f"zmq error: {e}"

        except Exception as e:
            self.logger.error(f"Unexpected error while receiving message: {e}")
            return None, f"unexpected error: {e}"
    
    
    def send_message(self, message: ProtocolMessage) -> bool:
        """
        Send a ProtocolMessage to the server via DEALER socket.

        Returns:
            True if the message was sent successfully, False otherwise.
        """

        if self.socket is None:
            self.logger.error("Cannot send message: client socket not initialized")
            return False
        
        if self.listener_thread is not None and self.listener_thread.is_alive():
            if threading.current_thread() != self.listener_thread:
                self.logger.error("send_message called outside IO thread")
                return False

        try:
            message_raw = self.message_handler.serialize(message)

            if not message_raw:
                self.logger.error(
                    f"Serialization failed for message with request_id={message.request_id}"
                )
                return False

            self.socket.send(message_raw)

            self.logger.debug(
                f"Sent message to server: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return True

        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error while sending message to server: {e}")
            return False

        except Exception as e:
            self.logger.error(f"Unexpected error while sending message to server: {e}")
            return False
        

    def handshake_core(self, timeout_ms: int = 20000) -> bool:
        if self.socket is None:
            self.logger.error("Communication not yet established. Cannot perform handshake.")
            return False
        
        ping_message = self.message_handler.create_handshake(
            phase="hello",
            payload={"message": "Ping"},
            sender="client",
            status=MessageStatus.OK
        )

        if not self.send_message(message=ping_message):
            self.logger.error(f"Failed to send Ping response")
            return False
        
        message, reason = self.receive_message(timeout_ms)

        if message is None:
            self.logger.error(f"Handshake failed while waiting for server alive: {reason}")
            return False
        
        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                f"Unexpected message type during handshake from server {self.server_endpoint}: {message.msg_type}"
            )
            return False
        
        if message.phase != "hello_ack":
            self.logger.error(
                f"Unexpected handshake phase from server {self.server_endpoint}: {message.phase}"
            )
            return False
        
        if message.payload.get("message") != "Alive":
            self.logger.error(
                f"Unexpected handshake payload from server {self.server_endpoint}: {message.payload}"
            )
            return False
        
        if message.in_reply_to != ping_message.request_id:
            self.logger.error(
                f"Alive message does not match expected Ping request: "
                f"{message.in_reply_to} != {ping_message.request_id}"
            )
            return False
        
        ready_message = self.message_handler.create_handshake(
            phase="ready",
            payload={"message": "Ready"},
            in_reply_to=message.request_id,
            sender="client",
            status=MessageStatus.OK
        )

        if not self.send_message(message=ready_message):
            self.logger.error(f"Failed to send Ready response")
            return False
        
        message, reason = self.receive_message(timeout_ms)

        if message is None:
            self.logger.error(f"Handshake failed while waiting for server startup: {reason}")
            return False
        
        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                f"Unexpected message type during handshake from server {self.server_endpoint}: {message.msg_type}"
            )
            return False
        
        if message.phase != "startup":
            self.logger.error(
                f"Unexpected handshake phase from server {self.server_endpoint}: {message.phase}"
            )
            return False

        if message.payload.get("message") != "Startup":
            self.logger.error(
                f"Unexpected handshake payload from server {self.server_endpoint}: {message.payload}"
            )
            return False

        if message.in_reply_to != ready_message.request_id:
            self.logger.error(
                f"Startup message does not match expected Ready request: "
                f"{message.in_reply_to} != {ready_message.request_id}"
            )
            return False

        acq_mode = message.payload.get("acq_mode")

        if acq_mode not in ACQUISITION_MODES:
            self.logger.error(f"Invalid acquisition mode received: {acq_mode}")
            return False

        self.acq_mode = acq_mode
        
        try:
            identity_payload = self.identity.to_dict_identity()
        except Exception as e:
            self.logger.error(f"Cannot build client identity payload: {e}")
            return False

        identity_message = self.message_handler.create_handshake(
            phase="startup_ack",
            payload={
                "message": "Identity",
                "identity": identity_payload,
            },
            in_reply_to=message.request_id,
            sender="client",
            status=MessageStatus.OK,
        )

        if not self.send_message(identity_message):
            self.logger.error("Failed to send client identity to server")
            return False

        if self.acq_mode == "multipmt":
            message, reason = self.receive_message(timeout_ms)

            if message is None:
                self.logger.error(f"Handshake failed while waiting for server multipmt_acq_config: {reason}")
                return False
            
            if message.msg_type != MessageType.HANDSHAKE:
                self.logger.error(
                    f"Unexpected message type during handshake from server {self.server_endpoint}: {message.msg_type}"
                )
                return False
            
            if message.phase != "multipmt_acq_config":
                self.logger.error(
                    f"Unexpected handshake phase from server {self.server_endpoint}: {message.phase}"
                )
                return False

            if message.payload.get("message") != "ChannelsConfig":
                self.logger.error(
                    f"Unexpected handshake payload from server {self.server_endpoint}: {message.payload}"
                )
                return False

            if message.in_reply_to != identity_message.request_id:
                self.logger.error(
                    f"MultiPMT Acquisition Config message does not match expected Ready request: "
                    f"{message.in_reply_to} != {identity_message.request_id}"
                )
                return False

            start_thr = message.payload.get("pe_thr")
            acq_info = message.payload.get("channels_info")

            if start_thr is None:
                self.logger.error("Missing pe_thr in multipmt acquisition config")
                return False

            if not isinstance(acq_info, dict) or not acq_info:
                self.logger.error(f"Invalid channels_info in multipmt acquisition config: {acq_info}")
                return False

            self.acq_info = acq_info
            self.start_thr = start_thr



        self.logger.info(
            f"Handshake completed successfully with server {self.server_endpoint}. "
            f"Acquisition mode: {self.acq_mode}, identity: {self.identity.hostname}"
        )

        return True
    

    def handshake(
        self,
        timeout_ms: int = 20000,
        retry_delay_s: float = 1.0,
        max_retries: Optional[int] = MAX_RETRIES
        ) -> bool:
        """
        Attempt the control-plane handshake repeatedly.

        If max_retries is None, retry indefinitely.
        """
        if self.socket is None:
            self.logger.error("Cannot start handshake: control socket not initialized")
            return False

        attempt = 0

        while max_retries is None or attempt < max_retries:
            attempt += 1
            if max_retries is None:
                self.logger.info(f"Handshake attempt {attempt} (retrying until success)")
            else:
                self.logger.info(f"Handshake attempt {attempt}/{max_retries}")

            if self.handshake_core(timeout_ms=timeout_ms):
                success = self.acquisition_service.apply_acquisition_mode(
                    new_mode=self.acq_mode,
                    acq_info=self.acq_info,
                    pe_thr=self.start_thr,
                )

                return success

            self.logger.warning("Handshake attempt failed, retrying...")
            time.sleep(retry_delay_s)

        self.logger.error("Handshake failed after maximum number of attempts")
        return False

    def queue_message(self, message: ProtocolMessage) -> None:
        self.outgoing_queue.put(message)
    
    def _ensure_hv_service(self) -> bool:
        if self.hv_service is not None:
            return True
        
        try:
            self.hv_service = HVService(hv_port=self.hv_port)
            return True
        except Exception as e:  
            self.logger.error(f"Cannot initialize HVService: {e}")
            self.hv_service = None
            return False
        
    def _control_io_loop(self) -> None:
        """
        Owns the control socket after the handshake.
        Receives messages and sends queued outgoing messages.
        """

        while not self.stop_listening.is_set():

            message, reason = self.receive_message(timeout_ms=100)

            if message is not None:
                self.incoming_queue.put((message, reason))
            elif reason != "timeout elapsed":
                self.logger.warning(f"Receive problem: {reason}")

            while True:
                try:
                    outgoing_message = self.outgoing_queue.get_nowait()
                except queue.Empty:
                    break

                if not self.send_message(outgoing_message):
                    self.logger.error(
                        f"Failed to send queued message: request_id={outgoing_message.request_id}"
                    )
    
    def _hv_warning_loop(self) -> None:
        while not self.stop_listening.is_set():

            if self.hv_service is None:
                time.sleep(0.5)
                continue

            try:
                warning = self.hv_service.warning_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            
            event_message = self.message_handler.create_event(
                channel=Channel.HV,
                payload=warning,
                sender="client",
                status=MessageStatus.ERROR,
            )
            
            self.queue_message(event_message)

    def start_listener(self) -> bool:
        
        if self.socket is None:
            self.logger.error("Cannot start listener: socket not initialized")
            return False
        

        if self.listener_thread and self.listener_thread.is_alive():
            self.logger.warning("Control listener already running")
            return True

        self.stop_listening.clear()

        self.listener_thread = threading.Thread(
            target=self._control_io_loop,
            daemon=True
        )
        self.listener_thread.start()
        
        if self.hv_warning_thread is None or not self.hv_warning_thread.is_alive():
            self.hv_warning_thread = threading.Thread(
                target=self._hv_warning_loop,
                daemon=True
            )
            self.hv_warning_thread.start()

        self.logger.info("Control listener started")
        return True
    

    def stop_listener(self) -> None:
        self.stop_listening.set()

        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=2.0)
        
        if self.hv_warning_thread and self.hv_warning_thread.is_alive():
            self.hv_warning_thread.join(timeout=2.0)

        self.logger.info("Control listener stopped")

    
    def handle_commands(self) -> None:
        """
        Main command dispatcher.
        Reads messages from incoming_queue and handles server commands.
        """

        while not self.stop_listening.is_set():
            try:
                message, reason = self.incoming_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if message is None:
                self.logger.warning(f"Invalid queued message: {reason}")
                continue

            if message.msg_type != MessageType.COMMAND:
                self.logger.warning(
                    f"Unexpected message type in command handler: {message.msg_type}"
                )
                continue

            handler = self.command_map.get(message.command)

            if handler is None:
                self.logger.warning(f"Unknown command: {message.command}")
                continue

            try:
                handler(self, message)
            except Exception as e:
                self.logger.error(f"Error handling command {message.command}: {e}")

    
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
        """Close only the current control socket connection."""

        self.stop_listener()
        if self.hv_service is not None:
            self.hv_service.stop()
        self.evproducer.stop()

        if self.socket is not None:
            try:
                self.recv_poller.unregister(self.socket)
            except Exception:
                pass

            try:
                self.socket.setsockopt(zmq.LINGER, 0)
                self.socket.close()
            except Exception as e:
                self.logger.warning(f"Error while closing socket: {e}")
            finally:
                self.socket = None
                self.server_endpoint = None

        self.logger.info("Control connection closed")


    def close(self) -> None:
        self.close_connection()
        self.logger.info("ControlPlaneManager closed")
                    
                    
                    
                    
                    
                    
                    
                    
                    
                     

        