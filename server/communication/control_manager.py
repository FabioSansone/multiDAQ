import zmq
from typing import Optional, List
from common.message_handler import MessageHandler, ProtocolMessage, MessageStatus, MessageType, Channel
from common.constants import ACQUISITION_MODES
from server.utils.logger import get_logger
from server.utils.json_parser import JsonParser
import threading
import queue

MAX_RETRIES = 5


class ControlPlaneManager:
    def __init__(self, context: zmq.Context, num_multi_clients:int, acq_mode: str):
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.endpoint: Optional[str] = None
        self.recv_poller = zmq.Poller()
        
        self.num_multi_clients = num_multi_clients
        self.acq_mode = acq_mode
        
        self.clients_by_multipmt_id: dict[str, bytes] = {}
        self.identity_by_client_id: dict[bytes, dict] = {}
        self.client_id_by_multipmt_id: dict[str, bytes] = {}
        
        self.connected_clients: List[bytes] = []
        self.message_handler = MessageHandler(logger=get_logger("message_handler"))
        
        self.stop_listening = threading.Event()
        self.incoming_queue = queue.Queue()
        self.outgoing_queue = queue.Queue()
        self.listener_thread: Optional[threading.Thread] = None
        
        self.event_queue = queue.Queue()
        self.event_thread: Optional[threading.Thread] = None
        self.event_callback = None
        
        self.logger = get_logger("control_manager")
        self.logger.debug("ZMQ Control Server Manager initialized")
        
        
    def start_connection(self, port: int) -> bool:
        """Start a ZMQ connection with DEALER client (ROUTER - DEALER)"""    
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
            self.logger.debug(f"Server started on port {port}")
        
            return True
        
        except zmq.ZMQError as e:
            self.socket = None
            self.endpoint = None
            self.logger.error(f"ZMQ Exception: failed to bind socket on port {port}: {e}")
            return False
        
        except Exception as e:
            self.socket = None
            self.endpoint = None
            self.logger.error(f"Generic Exception: failed to bind socket on port {port}: {e}")
            return False
    
    def receive_message(self, timeout_ms: int) -> tuple[Optional[bytes], Optional[ProtocolMessage], str]:
        """
        Receive one message from the ROUTER socket.

        Returns:
            (client_id, message, reason)

            - client_id: ZMQ identity of the sender, or None
            - message: deserialized ProtocolMessage, or None
            - reason: 'ok' on success, otherwise a description of the failure
        """
        
        if self.socket is None:
            self.logger.error("Cannot receive message: Server socket not initialized")
            return None, None, "server socket not initialized"
        
                
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
                self.logger.error(f"Failed to deserialize message from client {client_id!r}: {reason}")
                return client_id, None, reason
            
            self.logger.debug(
                f"Received message from client {client_id!r}: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )
            
            return client_id, message, "ok"
        
        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error while receiving message: {e}")
            return None, None, f"zmq error: {e}"

        except Exception as e:
            self.logger.error(f"Unexpected error while receiving message: {e}")
            return None, None, f"unexpected error: {e}"
        
    
    def send_message(self, client_id: bytes, message: ProtocolMessage) -> bool:
        """
        Send a ProtocolMessage to a specific client via ROUTER socket.

        Returns:
            True if the message was sent successfully, False otherwise.
        """

        if self.socket is None:
            self.logger.error("Cannot send message: server socket not initialized")
            return False

        try:
            message_raw = self.message_handler.serialize(message)

            if not message_raw:
                self.logger.error(
                    f"Serialization failed for message with request_id={message.request_id}"
                )
                return False

            self.socket.send_multipart([client_id, message_raw])

            self.logger.debug(
                f"Sent message to client {client_id!r}: "
                f"type={message.msg_type.value}, request_id={message.request_id}"
            )

            return True

        except zmq.ZMQError as e:
            self.logger.error(
                f"ZMQ error while sending message to client {client_id!r}: {e}"
            )
            return False

        except Exception as e:
            self.logger.error(
                f"Unexpected error while sending message to client {client_id!r}: {e}"
            )
            return False
        

    def add_client(self, client_id: bytes) -> None:
        """Add a client to the connected clients list"""
        if client_id not in self.connected_clients:
            self.connected_clients.append(client_id)
            self.logger.info(f"Client {client_id.decode(errors='ignore')} connected. Total clients: {len(self.connected_clients)}")
    
    def remove_client(self, client_id: bytes) -> None:
        """Remove a client from the connected clients list"""
        if client_id in self.connected_clients:
            self.connected_clients.remove(client_id)
            self.logger.info(f"Client {client_id.decode(errors='ignore')} disconnected. Total clients: {len(self.connected_clients)}")
    
    def list_connected_clients(self) -> List[bytes]:
        """List all the connected clients"""
        return list(self.connected_clients)
    
    def handshake_core(self, timeout_ms: int = 20000) -> bool:
        if self.socket is None:
            self.logger.error("Communication not yet established. Cannot perform handshake.")
            return False

        client_id, message, reason = self.receive_message(timeout_ms)

        if message is None:
            self.logger.error(f"Handshake failed while waiting for client hello: {reason}")
            return False

        if message.msg_type != MessageType.HANDSHAKE:
            self.logger.error(
                f"Unexpected message type during handshake from {client_id!r}: {message.msg_type}"
            )
            return False

        if message.phase != "hello":
            self.logger.error(
                f"Unexpected handshake phase from {client_id!r}: {message.phase}"
            )
            return False

        if message.payload.get("message") != "Ping":
            self.logger.error(
                f"Unexpected handshake payload from {client_id!r}: {message.payload}"
            )
            return False

        ping_response = self.message_handler.create_handshake(
            phase="hello_ack",
            payload={"message": "Alive"},
            in_reply_to=message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=ping_response):
            self.logger.error(f"Failed to send Alive response to client {client_id!r}")
            return False

        client_id_ready, ready_message, reason = self.receive_message(timeout_ms)
        if ready_message is None:
            self.logger.error(f"Handshake failed while waiting for Ready: {reason}")
            return False

        if client_id_ready != client_id:
            self.logger.error("Received Ready from a different client during handshake")
            return False

        if ready_message.msg_type != MessageType.HANDSHAKE:
            self.logger.error("Expected handshake message for Ready phase")
            return False

        if ready_message.phase != "ready":
            self.logger.error(f"Unexpected final handshake phase: {ready_message.phase}")
            return False

        if ready_message.payload.get("message") != "Ready":
            self.logger.error(f"Unexpected Ready payload: {ready_message.payload}")
            return False

        if ready_message.in_reply_to != ping_response.request_id:
            self.logger.error(
                f"Ready message does not match expected reply target: "
                f"{ready_message.in_reply_to} != {ping_response.request_id}"
            )
            return False
        
        identity_mode_message = self.message_handler.create_handshake(
            phase="startup",
            payload={
                "message": "Startup",
                "acq_mode": self.acq_mode,
            },
            in_reply_to=ready_message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=identity_mode_message):
            self.logger.error(f"Failed to send acquisition mode to client {client_id!r}")
            return False


        client_id_startup_message, startup_message, reason = self.receive_message(timeout_ms)

        if startup_message is None:
            self.logger.error(f"Handshake failed while waiting for client identity: {reason}")
            return False

        if client_id_startup_message != client_id:
            self.logger.error("Received identity from a different client during handshake")
            return False

        if startup_message.msg_type != MessageType.HANDSHAKE:
            self.logger.error("Expected handshake message for startup_ack phase")
            return False

        if startup_message.phase != "startup_ack":
            self.logger.error(f"Unexpected handshake phase: {startup_message.phase}")
            return False

        if startup_message.payload.get("message") != "Identity":
            self.logger.error(f"Unexpected identity payload: {startup_message.payload}")
            return False

        if startup_message.in_reply_to != identity_mode_message.request_id:
            self.logger.error(
                f"Identity message does not match expected reply target: "
                f"{startup_message.in_reply_to} != {identity_mode_message.request_id}"
            )
            return False

        identity_payload = startup_message.payload.get("identity")

        if not isinstance(identity_payload, dict):
            self.logger.error(f"Invalid identity payload: {startup_message.payload}")
            return False

        multipmt_id = identity_payload.get("multipmt_id")
        batch_id = identity_payload.get("batch_id")

        if not multipmt_id:
            self.logger.error(f"Missing multipmt_id in identity payload: {identity_payload}")
            return False

        if not batch_id:
            self.logger.error(f"Missing batch_id in identity payload: {identity_payload}")
            return False

        if multipmt_id in self.client_id_by_multipmt_id:
            self.logger.error(
                f"Duplicate multipmt_id received: {multipmt_id}. "
                f"Already mapped to {self.client_id_by_multipmt_id[multipmt_id]!r}"
            )
            return False

        self.client_id_by_multipmt_id[multipmt_id] = client_id
        self.identity_by_client_id[client_id] = identity_payload

        self.add_client(client_id)

        if self.acq_mode == "multipmt":
            config_file_service = JsonParser(
                multipmt_id=multipmt_id,
                batch_id=batch_id,
            )

            channels_acq_info = config_file_service.get_ch_configuration(pe_thr=1)

            if channels_acq_info is None:
                self.logger.error(
                    f"Cannot build multipmt acquisition configuration for "
                    f"multipmt_id={multipmt_id}, batch_id={batch_id}"
                )
                return False

            acq_mode_message = self.message_handler.create_handshake(
                phase="multipmt_acq_config",
                payload={
                    "message": "ChannelsConfig",
                    "pe_thr": 1,
                    "acquisition_configuration": channels_acq_info,
                },
                in_reply_to=startup_message.request_id,
                sender="server",
                status=MessageStatus.OK,
            )

            if not self.send_message(client_id=client_id, message=acq_mode_message):
                self.logger.error(
                    f"Failed to send multipmt acquisition config to client {client_id!r}"
                )
                return False

        self.logger.info(
            f"Handshake completed successfully with client {client_id!r}, "
            f"multipmt_id={multipmt_id}, batch_id={batch_id}, acq_mode={self.acq_mode}"
        )

        return True
    
    def handshake(self) -> bool:
        self.logger.info(f"Waiting for {self.num_multi_clients} clients to connect...")

        initial_count = len(self.list_connected_clients())

        while len(self.list_connected_clients()) < self.num_multi_clients:
            retries = 0
            success = False

            while retries < MAX_RETRIES and not success:
                self.logger.info(f"Trying handshake with a client (attempt {retries+1}/{MAX_RETRIES})")
                success = self.handshake_core(timeout_ms=20000)

                if not success:
                    retries += 1
                    self.logger.warning("Handshake attempt failed, retrying...")

            if not success:
                self.logger.warning(
                    "No more clients connected after maximum attempts. "
                    "Server will remain operative with currently connected clients."
                )
                break

        final_count = len(self.list_connected_clients())
        new_clients = final_count - initial_count

        if final_count > 0:
            self.logger.info(
                f"Control plane ready. Connected clients: {final_count}/{self.num_multi_clients}. "
                f"New clients in this connect call: {new_clients}"
            )
            return True

        self.logger.error("No clients connected.")
        return False
    
    def notify_shutdown_to_all_clients(self) -> bool:
        if self.socket is None:
            self.logger.error("Communication not yet established with any clients. Standard server shutdown.")
            return True

        connected_clients = self.list_connected_clients()
        if not connected_clients:
            self.logger.warning("No connected clients to notify about shutdown.")
            return True
        
        listener_was_running = (
            self.listener_thread is not None
            and self.listener_thread.is_alive()
        )

        if listener_was_running:
            self.stop_listener()
            
        success = True

        for client_id in connected_clients:
            shutdown_message = self.message_handler.create_command(
                channel=Channel.SYSTEM,
                command="server_shutdown",
                payload={"message": "Server is shutting down"},
                sender="server"
            )

            if not self.send_message(client_id=client_id, message=shutdown_message):
                self.logger.error(f"Failed to send shutdown command to connected clients")
                success = False
                
        return success
    
    def queue_message(self, client_id: bytes, message: ProtocolMessage) -> None:
        self.outgoing_queue.put((client_id, message))
        
    def wait_for_reply(
        self,
        *,
        client_id: bytes,
        in_reply_to: str,
        timeout_s: float = 10.0,
    ) -> tuple[Optional[ProtocolMessage], str]:

        try:
            while True:
                reply_client_id, message, reason = self.incoming_queue.get(timeout=timeout_s)

                if message is None:
                    return None, reason

                if reply_client_id != client_id:
                    self.incoming_queue.put((reply_client_id, message, reason))
                    continue

                if message.in_reply_to != in_reply_to:
                    self.incoming_queue.put((reply_client_id, message, reason))
                    continue

                return message, "ok"

        except queue.Empty:
            return None, "timeout waiting for reply"
        
    def _control_io_loop(self) -> None:
        """
        Owns the ROUTER socket after the handshake.
        Receives messages and sends queued outgoing messages.
        """

        while not self.stop_listening.is_set():

            client_id, message, reason = self.receive_message(timeout_ms=100)

            if message is not None:
                if message.msg_type == MessageType.EVENT:
                    self.event_queue.put((client_id, message,))
                else:
                    self.incoming_queue.put((client_id, message, reason))
            elif reason != "timeout elapsed":
                self.logger.warning(f"Receive problem: {reason}")

            while True:
                try:
                    client_id_out, outgoing_message = self.outgoing_queue.get_nowait()
                except queue.Empty:
                    break

                if not self.send_message(client_id=client_id_out, message=outgoing_message):
                    self.logger.error(
                        f"Failed to send queued message to {client_id_out!r}: "
                        f"request_id={outgoing_message.request_id}"
                    )
    
    def _event_loop(self) -> None:
        while not self.stop_listening.is_set():
            try:
                client_id, message = self.event_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            
            client_name = client_id.decode(errors="ignore")
            payload = message.payload
            
            if message.channel == Channel.HV:
                event = payload.get("event", "unknown_event")
                severity = payload.get("severity", "info")
                
                if self.event_callback is not None:
                    try:
                        self.event_callback(message)
                    except Exception as e:
                        self.logger.error(f"Event callback failed: {e}")

                if severity == "warning":
                    self.logger.warning(
                        f"HV warning from {client_name}: {event} - {payload}"
                    )
                else:
                    self.logger.info(
                        f"HV event from {client_name}: {event} - {payload}"
                    )
            
            self.event_queue.task_done()
        

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
        
        if self.event_thread is None or not self.event_thread.is_alive():
            self.event_thread = threading.Thread(
                target=self._event_loop,
                daemon=True,
            )
            self.event_thread.start()

        self.logger.info("Control listener started")
        return True
    

    def stop_listener(self) -> None:
        self.stop_listening.set()

        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=2.0)
        
        if self.event_thread and self.event_thread.is_alive():
            self.event_thread.join(timeout=2.0)

        self.logger.info("Control listener stopped")
        
        
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
        
        while not self.event_queue.empty():
            try:
                self.event_queue.get_nowait()
            except queue.Empty:
                break

    def close_connection(self) -> None:
        """Close only the current control socket connection."""

        self.stop_listener()

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
                self.endpoint = None

        self.logger.info("Control connection closed")
    
             