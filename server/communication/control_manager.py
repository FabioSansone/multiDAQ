import zmq
from typing import Optional, List
from dataclasses import dataclass
import time
from common.message_handler import MessageHandler, ProtocolMessage, MessageStatus, MessageType, Channel
from common.reply_handler import ReplyHandler
from server.communication.liveness_probe import LivenessProbe
from server.utils.logger import get_logger
from server.core.server_state import ServerState
from server.core.mac_id_registry import MacIdentityRegistry
from common.timeouts import HANDSHAKE_STEP_TIMEOUT_S
import threading
import queue

MAX_RETRIES = 5

@dataclass
class ControlHandshakeSession:
    phase: str
    last_sent_request_id: str
    deadline: float
    identity: dict | None = None
    mac_client_id: int | None = None


class ControlPlaneManager:
    def __init__(self, context: zmq.Context, num_multi_clients:int, state: ServerState, mac_identity_registry: MacIdentityRegistry):
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.endpoint: Optional[str] = None
        self.recv_poller = zmq.Poller()

        self.server_state = state
        self.num_multi_clients = num_multi_clients

        self.message_handler = MessageHandler(logger=get_logger("message_handler"))
        
        self.stop_listening = threading.Event()
        self.outgoing_queue = queue.Queue()
        self.listener_thread: Optional[threading.Thread] = None
        
        self.event_queue = queue.Queue()
        self.event_thread: Optional[threading.Thread] = None
        self.event_callback = None
        self.client_lost_callback = None
        
        self.mac_register = mac_identity_registry

        self.reply_handler = ReplyHandler(logger=get_logger("reply_handler"))

        self._handshake_sessions : dict[bytes, ControlHandshakeSession] = {}
        self._handshake_handlers = {
            "hello": self._on_hello,
            "ready": self._on_ready,
            "startup_ack": self._on_startup_ack,
            "end": self._on_end,
        }

        
        self.logger = get_logger("control_manager")
        self.liveness_probe = LivenessProbe(self.logger)
        self.logger.debug("ZMQ Control Server Manager initialized")
    
    def list_connected_clients(self) -> List[bytes]:
        return self.server_state.list_connected_clients()

    def get_identity(self, client_id: bytes) -> Optional[dict]:
        return self.server_state.get_identity(client_id)

    def add_client(self, client_id: bytes, identity: Optional[dict] = None) -> None:
        self.server_state.add_client(client_id, identity)

    def remove_client(self, client_id: bytes) -> None:
        self.server_state.remove_client(client_id)
        
    def start_connection(self, port: int) -> bool:
        """Start a ZMQ connection with DEALER client (ROUTER - DEALER)"""
        # The sessions belong to the previous socket
        self._handshake_sessions.clear()

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
            self.socket.setsockopt(zmq.ROUTER_HANDOVER, 1)
            self.socket.setsockopt(zmq.HEARTBEAT_IVL, 30000)
            self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, 60000)
            self.socket.setsockopt(zmq.HEARTBEAT_TTL, 60000)
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
            self.logger.error(f"ZMQ error while sending message to client {client_id!r}: {e}")
            if e.errno == zmq.EHOSTUNREACH:
                self._notify_client_lost(client_id)
            return False

        except Exception as e:
            self.logger.error(
                f"Unexpected error while sending message to client {client_id!r}: {e}"
            )
            return False
        
    
    def handshake_core(self, timeout_ms: int = 20000) -> bool:
        if self.socket is None:
            self.logger.error("Communication not yet established. Cannot perform handshake.")
            return False

        client_id, message, reason = self.receive_message(timeout_ms)

        if message is None:
            self.logger.error(f"Handshake failed while waiting for client hello: {reason}")
            return False

        error = self._check_handshake_message(message, phase="hello", expected_message="Ping")
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on hello: {error}")
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

        error = self._check_handshake_message(
            ready_message,
            phase="ready",
            expected_message="Ready",
            in_reply_to=ping_response.request_id,
        )
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on ready: {error}")
            return False

        identity_mode_message = self.message_handler.create_handshake(
            phase="startup",
            payload={
                "message": "Startup",
                "acq_mode": self.server_state.get_mode(),
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

        error = self._check_handshake_message(
            startup_message,
            phase="startup_ack",
            expected_message="Identity",
            in_reply_to=identity_mode_message.request_id,
        )
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on startup_ack: {error}")
            return False

        identity_payload = startup_message.payload.get("identity")

        error = self._validate_handshake_identity(client_id, identity_payload)
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on identity: {error}")
            return False

        multipmt_id = identity_payload.get("multipmt_id")
        batch_id = identity_payload.get("batch_id")
        mac_id = identity_payload.get("mac_address")

        mac_client_id = self.mac_register.get_id_from_mac(mac_id)

        if mac_client_id is None:
            self.logger.error(
                f"Failed to assign/retrieve numeric id for MAC {mac_id} "
                f"(client {client_id!r}); aborting handshake."
            )
            return False

        end_mode_message = self.message_handler.create_handshake(
            phase="end",
            payload={
                "message": "End",
                "mac_client_id": int(mac_client_id),
            },
            in_reply_to=startup_message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=end_mode_message):
            self.logger.error(f"Failed to send mac client id for evproducer client {client_id!r}")
            return False
        
        client_id_conclusion_message, conclusion_message, reason = self.receive_message(timeout_ms)
        
        if conclusion_message is None:
            self.logger.error(f"Handshake failed while waiting for client conclusion: {reason}")
            return False

        if client_id_conclusion_message != client_id:
            self.logger.error("Received conclusion from a different client during handshake")
            return False

        error = self._check_handshake_message(
            conclusion_message,
            phase="end",
            expected_message="Conclusion_ack",
            in_reply_to=end_mode_message.request_id,
        )
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on end: {error}")
            return False

        if self.server_state.is_client_on_plane(client_id=client_id, plane="control"):
            self.logger.warning(f"Client {client_id!r} is already registered on the control plane: previous session considered lost")
            self._notify_client_lost(client_id)

        self.add_client(client_id, identity_payload)

        self.logger.info(
            f"Handshake completed successfully with client {client_id!r}, "
            f"multipmt_id={multipmt_id}, batch_id={batch_id}, acq_mode={self.server_state.get_mode()}"
        )

        return True


    def _check_handshake_message(self, message, *, phase, expected_message, in_reply_to=None) -> str | None:

        if in_reply_to is not None and message.in_reply_to != in_reply_to:
            return f"Expecting a different message in reply {message.in_reply_to} != {in_reply_to}"

        if message.msg_type != MessageType.HANDSHAKE:
            return f"Unexpected message type during handshake: {message.msg_type}"

        if message.phase != phase:
            return f"Unexpected handshake phase: {message.phase}"

        if message.payload.get("message") != expected_message:
            return f"Unexpected handshake payload: {message.payload}"

        return None


    def _validate_handshake_identity(self, client_id: bytes, identity_payload: dict) -> str | None:

        if not isinstance(identity_payload, dict):
            return f"Invalid identity payload"
        
        multipmt_id = identity_payload.get("multipmt_id")
        batch_id = identity_payload.get("batch_id")
        mac_id = identity_payload.get("mac_address")
        
        if not multipmt_id:
            return f"Missing multipmt_id in identity payload: {identity_payload}"

        if not batch_id:
            return f"Missing batch_id in identity payload: {identity_payload}"
        
        if not mac_id:
            return f"Missing mac address in identity payload: {identity_payload}"

        existing_client_id = self.server_state.get_client_id_by_multipmt_id(multipmt_id)

        if existing_client_id is not None and existing_client_id != client_id:
            return f"Duplicate multipmt_id received: {multipmt_id}. Already mapped to {existing_client_id!r}"

        return None

    def _handle_handshake_message(self, client_id: bytes, message: ProtocolMessage) -> None:
        handshake_method = self._handshake_handlers.get(message.phase, None)
        if handshake_method is None:
            self.logger.error(f"Handshake message from {client_id!r} with unknown phase {message.phase!r}: ignored")
            return

        if message.phase != "hello" and self._handshake_sessions.get(client_id, None) is None:
            self.logger.error(
                f"Handshake message in phase {message.phase!r} from {client_id!r} without an open "
                "session (expired or never started): ignored"
            )
            return

        # An exception here would end the I/O thread of the whole control plane
        try:
            handshake_method(client_id, message)
        except Exception:
            self.logger.exception(f"Handshake with {client_id!r} failed in phase {message.phase!r}")
            self._handshake_sessions.pop(client_id, None)

    def _expire_handshake_sessions(self) -> None:
        now = time.monotonic()
        expired = [cid for cid, session in self._handshake_sessions.items() if session.deadline < now]
        for cid in expired:
            self.logger.warning(f"Handshake with {cid!r} expired in phase {self._handshake_sessions.get(cid).phase}")
            self._handshake_sessions.pop(cid)



    def _on_hello(self, client_id: bytes, message: ProtocolMessage) -> None:
        error = self._check_handshake_message(message, phase="hello", expected_message="Ping")
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on hello: {error}")
            # The client is starting over: whatever it had open is no longer valid
            self._handshake_sessions.pop(client_id, None)
            return

        previous = self._handshake_sessions.get(client_id)
        if previous is not None:
            self.logger.warning(
                f"Hello from {client_id!r} while its previous handshake was in phase "
                f"{previous.phase!r}: restarting the handshake"
            )

        ping_response = self.message_handler.create_handshake(
            phase="hello_ack",
            payload={"message": "Alive"},
            in_reply_to=message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=ping_response):
            self.logger.error(f"Failed to send Alive response to client {client_id!r}")
            self._handshake_sessions.pop(client_id, None)
            return

        deadline = time.monotonic() + HANDSHAKE_STEP_TIMEOUT_S

        client_handshake_hello_session = ControlHandshakeSession(
            phase="hello",
            last_sent_request_id=ping_response.request_id,
            deadline=deadline,
            identity=None,
            mac_client_id=None
        )

        self._handshake_sessions[client_id] = client_handshake_hello_session
        self.logger.info(f"Handshake with {client_id!r}: hello accepted, Alive sent")


    def _on_ready(self, client_id: bytes, message: ProtocolMessage) -> None:
        # The dispatcher guarantees the session exists
        session = self._handshake_sessions[client_id]

        error = self._check_handshake_message(
            message,
            phase="ready",
            expected_message="Ready",
            in_reply_to=session.last_sent_request_id,
        )
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on ready: {error}")
            self._handshake_sessions.pop(client_id, None)
            return

        startup_message = self.message_handler.create_handshake(
            phase="startup",
            payload={
                "message": "Startup",
                "acq_mode": self.server_state.get_mode(),
            },
            in_reply_to=message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=startup_message):
            self.logger.error(f"Failed to send acquisition mode to client {client_id!r}")
            self._handshake_sessions.pop(client_id, None)
            return

        session.phase = "ready"
        session.last_sent_request_id = startup_message.request_id
        session.deadline = time.monotonic() + HANDSHAKE_STEP_TIMEOUT_S
        self.logger.info(f"Handshake with {client_id!r}: ready accepted, Startup sent")


    def _on_startup_ack(self, client_id: bytes, message: ProtocolMessage) -> None:
        session = self._handshake_sessions[client_id]
        
        error = self._check_handshake_message(
            message,
            phase="startup_ack",
            expected_message="Identity",
            in_reply_to=session.last_sent_request_id,
        )
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on startup acknowledge: {error}")
            self._handshake_sessions.pop(client_id, None)
            return

        identity_payload = message.payload.get("identity")
        
        error = self._validate_handshake_identity(client_id, identity_payload)
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on identity: {error}")
            self._handshake_sessions.pop(client_id, None)
            return 

        session.identity = identity_payload

        mac_id = identity_payload.get("mac_address")

        mac_client_id = self.mac_register.get_id_from_mac(mac_id)

        if mac_client_id is None:
            self.logger.error(
                f"Failed to assign/retrieve numeric id for MAC {mac_id} "
                f"(client {client_id!r}); aborting handshake."
            )
            self._handshake_sessions.pop(client_id, None)
            return 

        end_mode_message = self.message_handler.create_handshake(
            phase="end",
            payload={
                "message": "End",
                "mac_client_id": int(mac_client_id),
            },
            in_reply_to=message.request_id,
            sender="server",
            status=MessageStatus.OK,
        )

        if not self.send_message(client_id=client_id, message=end_mode_message):
            self.logger.error(f"Failed to send mac client id for evproducer client {client_id!r}")
            self._handshake_sessions.pop(client_id, None)
            return 

        session.phase = "startup_ack"
        session.last_sent_request_id = end_mode_message.request_id
        session.deadline = time.monotonic() + HANDSHAKE_STEP_TIMEOUT_S

        self.logger.info(f"Handshake with {client_id!r}: Startup accepted, End sent")

    def _on_end(self, client_id: bytes, message: ProtocolMessage) -> None:
        session = self._handshake_sessions[client_id]

        error = self._check_handshake_message(
            message,
            phase="end",
            expected_message="Conclusion_ack",
            in_reply_to=session.last_sent_request_id,
        )
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on end: {error}")
            self._handshake_sessions.pop(client_id, None)
            return

        # Another client may have been admitted with the same multipmt_id since startup_ack
        error = self._validate_handshake_identity(client_id, session.identity)
        if error:
            self.logger.error(f"Handshake with {client_id!r} failed on identity before admission: {error}")
            self._handshake_sessions.pop(client_id, None)
            return

        if self.server_state.is_client_on_plane(client_id=client_id, plane="control"):
            self.logger.warning(
                f"Client {client_id!r} is already registered on the control plane: "
                "previous session considered lost"
            )
            self._notify_client_lost(client_id)

        self.add_client(client_id, session.identity)
        self._handshake_sessions.pop(client_id, None)

        self.logger.info(
            f"Handshake completed successfully with client {client_id!r}, "
            f"multipmt_id={session.identity.get('multipmt_id')}, "
            f"batch_id={session.identity.get('batch_id')}, acq_mode={self.server_state.get_mode()}"
        )



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
        # The waiter must exist before the command leaves, or a fast reply is lost.
        self.reply_handler.register_pending_reply(client_id=client_id, request_id=message.request_id)
        self.outgoing_queue.put((client_id, message))

    def wait_for_reply(
        self,
        *,
        client_id: bytes,
        in_reply_to: str,
        timeout_s: float = 10.0,
    ) -> tuple[Optional[ProtocolMessage], str]:
        """Wait for the reply to `in_reply_to` from `client_id` (see ReplyHandler)."""
        return self.reply_handler.wait_pending_reply(
            client_id=client_id,
            request_id=in_reply_to,
            timeout_s=timeout_s,
        )

    def _notify_client_lost(self, client_id: bytes) -> None:
        """The ROUTER told us the client is gone: let the registered callback decide what to do.
        It runs in the I/O thread, so it must not block."""
        self.logger.warning(f"Control client {client_id!r} lost")

        if self.client_lost_callback is None:
            return

        try:
            self.client_lost_callback(client_id)
        except Exception as exc:
            self.logger.exception(f"Control client lost callback failed: client={client_id!r}: {exc}")


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
                elif message.msg_type == MessageType.HANDSHAKE:
                    self._handle_handshake_message(client_id=client_id, message=message)
                else:
                    self.reply_handler.deliver_pending_reply(client_id=client_id, message=message)
            elif reason != "timeout elapsed":
                self.logger.warning(f"Receive problem: {reason}")

            self.liveness_probe.run_if_due(
                self.socket, self.server_state.list_connected_clients(), self._notify_client_lost,
            )

            self._expire_handshake_sessions()

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

        # Nobody serves the open sessions any more: their next message would meet a blocking handshake
        self._handshake_sessions.clear()

        self.logger.info("Control listener stopped")
        
        
    def clear_queues(self) -> None:
        self.reply_handler.cancel_all("queues cleared")

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
        self._handshake_sessions.clear()

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
    
             