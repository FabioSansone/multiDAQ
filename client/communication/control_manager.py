import zmq
from typing import Optional
from experimental.client.utils.logger import get_logger
from experimental.client.communication.identity import ClientIdentity
from experimental.common.message_handler import MessageHandler, ProtocolMessage, MessageStatus, MessageType
import time
from threading import Thread

MAX_RETRIES = 10

class ControlPlaneManager:
    def __init__(self, context: zmq.Context, server_ip: str, identity: ClientIdentity) -> None:
        self.context = context
        self.socket: Optional[zmq.Socket] = None
        self.recv_poller = zmq.Poller()
        self.server_endpoint: Optional[str] = None
        
        self.server_ip = server_ip
        self.identity = identity
    
        self.message_handler = MessageHandler(logger=get_logger("message_handler"))


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
        
        self.logger.info(f"Handshake completed successfully with server {self.server_endpoint}")
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
                self.logger.info("Handshake completed successfully")
                return True

            self.logger.warning("Handshake attempt failed, retrying...")
            time.sleep(retry_delay_s)

        self.logger.error("Handshake failed after maximum number of attempts")
        return False


    def start_listener(self) -> None:

        while True:
            message, reason = self.receive_message(timeout_ms=0)



                    
                    
                    
                    
                    
                    
                    
                    
                    
                     

        