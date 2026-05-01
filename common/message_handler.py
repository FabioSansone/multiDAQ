import json
import logging
import uuid
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, Any


PROTOCOL_VERSION = 1

class MessageType(str, Enum):
    HANDSHAKE = "handshake"
    COMMAND = "command"
    REPLY = "reply"
    EVENT = "event"
    ERROR = "error"
    
class Channel(str, Enum):
    SYSTEM = "system"
    RC = "rc"
    HV = "hv"
    MONITORING = "monitoring"
    ACQUISITION = "acquisition"
    
class MessageStatus(str, Enum):
    OK = "ok"
    ERROR = "error"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    BUSY = "busy"
    


@dataclass(slots=True)
class ProtocolMessage:
    protocol_version: int
    msg_type: MessageType
    request_id: str
    timestamp: float
    payload: dict[str, Any] = field(default_factory=dict)

    channel: Optional[Channel] = None
    command: Optional[str] = None
    phase: Optional[str] = None
    in_reply_to: Optional[str] = None
    sender: Optional[str] = None
    status: Optional[MessageStatus] = None
    
    
    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        
        data["msg_type"] = self.msg_type.value
        
        if self.channel is not None:
            data["channel"] = self.channel.value
        
        if self.status is not None:
            data["status"] = self.status.value
        
        return data
    
    @classmethod
    def from_dict(cls, data:dict[str, Any]) -> "ProtocolMessage":
        
        return cls(
            protocol_version=data["protocol_version"],
            msg_type=MessageType(data["msg_type"]),
            request_id=data["request_id"],
            timestamp=data["timestamp"],
            payload=data.get("payload", {}),
            channel=Channel(data["channel"]) if data.get("channel") is not None else None,
            command=data.get("command"),
            phase=data.get("phase"),
            in_reply_to=data.get("in_reply_to"),
            sender=data.get("sender"),
            status=MessageStatus(data["status"]) if data.get("status") is not None else None,
        )
        
        
        
class MessageHandler:
    
    
    def __init__(self, logger = None):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("Message Handler initialized")
    
    @staticmethod
    def new_request_id() -> str:
        return str(uuid.uuid4())
    
    def build_message(
    self,
    msg_type: MessageType,
    *,
    payload: Optional[dict[str, Any]] = None,
    channel: Optional[Channel] = None,
    command: Optional[str] = None,
    phase: Optional[str] = None,
    request_id: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    sender: Optional[str] = None,
    status: Optional[MessageStatus] = None,
    timestamp: Optional[float] = None,
    ) -> ProtocolMessage:

        return ProtocolMessage(
            protocol_version=PROTOCOL_VERSION,
            msg_type=msg_type,
            request_id=request_id or self.new_request_id(),
            timestamp=timestamp if timestamp is not None else time.time(),
            payload=payload or {},
            channel=channel,
            command=command,
            phase=phase,
            in_reply_to=in_reply_to,
            sender=sender,
            status=status,
        )
    
    def encode(self, message: ProtocolMessage | dict[str, Any]) -> bytes:
        
        try:
            if isinstance(message, ProtocolMessage):
                serializable = message.to_dict()
            elif isinstance(message, dict):
                serializable = message
            else:
                raise TypeError(f"Unsupported message type for encoding: {type(message)}")
            
            return json.dumps(serializable, separators=(",", ":"), ensure_ascii=False).encode("utf-8")  
        
        except TypeError as e:
            self.logger.error(f"TypeError while encoding JSON message: {e}")
            return b""
        except ValueError as e:
            self.logger.error(f"ValueError while encoding JSON message: {e}")
            return b""
        except Exception as e:
            self.logger.error(f"Generic Exception while encoding JSON message: {e}")
            return b""  
    
    def decode(self, message_bytes: bytes) -> dict[str, Any]:
        try:
            decoded = json.loads(message_bytes.decode("utf-8"))
            if not isinstance(decoded, dict):
                self.logger.error("Decoded JSON is not a dictionary")
                return {}
            return decoded
        except json.JSONDecodeError as e:
            self.logger.error(f"JSONDecodeError while decoding message: {e}")
            return {}
        except UnicodeDecodeError as e:
            self.logger.error(f"UnicodeDecodeError while decoding message bytes: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Generic Exception while decoding message: {e}")
            return {}  
    
    def validate_raw_dict(self, data: dict[str, Any]) -> tuple[bool, str]:
        required_fields = {
            "protocol_version",
            "msg_type",
            "request_id",
            "timestamp",
            "payload"
        }
        
        missing = required_fields - set(data.keys())
        if missing:
            return False, f"Missing required fields: {sorted(missing)}"
        
        if data["protocol_version"] != PROTOCOL_VERSION:
            return False, f"Unsupported protocol version: {data['protocol_version']}"
        
        if not isinstance(data["request_id"], str):
            return False, "request_id must be a string"

        if not isinstance(data["timestamp"], (int, float)):
            return False, "timestamp must be numeric"

        if not isinstance(data["payload"], dict):
            return False, "payload must be a dictionary"
        
        try:
            MessageType(data["msg_type"])
        except ValueError:
            return False, f"Invalid msg_type: {data['msg_type']}"
        
        if "channel" in data and data["channel"] is not None:
            try:
                Channel(data["channel"])
            except ValueError:
               return False, f"Invalid channel: {data['channel']}"
        
        if "status" in data and data["status"] is not None:
            try:
                MessageStatus(data["status"])
            except ValueError:
                return False, f"Invalid status: {data['status']}"
    
        if "command" in data and data["command"] is not None and not isinstance(data["command"], str):
            return False, "command must be a string"

        if "phase" in data and data["phase"] is not None and not isinstance(data["phase"], str):
            return False, "phase must be a string"

        if "in_reply_to" in data and data["in_reply_to"] is not None and not isinstance(data["in_reply_to"], str):
            return False, "in_reply_to must be a string"

        if "sender" in data and data["sender"] is not None and not isinstance(data["sender"], str):
            return False, "sender must be a string"

        return True, "ok"
    
    
    def validate_semantics(self, data: dict[str, Any]) -> tuple[bool, str]:
        try:
            msg_type = MessageType(data["msg_type"])
        except ValueError:
            return False, "Invalid msg_type"

        if msg_type == MessageType.HANDSHAKE:
            if not data.get("phase"):
                return False, "Handshake message requires 'phase'"

        elif msg_type == MessageType.COMMAND:
            if not data.get("command"):
                return False, "Command message requires 'command'"
            if data.get("channel") is None:
                return False, "Command message requires 'channel'"

        elif msg_type == MessageType.REPLY:
            if not data.get("in_reply_to"):
                return False, "Reply message requires 'in_reply_to'"

        elif msg_type == MessageType.ERROR:
            if not data.get("in_reply_to"):
                return False, "Error message should reference 'in_reply_to'"

        return True, "ok"
    
    
    def deserialize(self, message_bytes: bytes) -> tuple[Optional[ProtocolMessage], str]:
        raw = self.decode(message_bytes)
        if not raw:
            return None, "Empty or invalid decoded message"

        valid, reason = self.validate_raw_dict(raw)
        if not valid:
            return None, reason

        valid, reason = self.validate_semantics(raw)
        if not valid:
            return None, reason

        try:
            return ProtocolMessage.from_dict(raw), "ok"
        except Exception as e:
            self.logger.error(f"Failed to convert dict to ProtocolMessage: {e}")
            return None, str(e)
        
    def serialize(self, message: ProtocolMessage) -> bytes:
        valid, reason = self.validate_message_object(message)
        if not valid:
            self.logger.error(f"Cannot serialize invalid ProtocolMessage: {reason}")
            return b""

        return self.encode(message)
        
    def validate_message_object(self, message: ProtocolMessage) -> tuple[bool, str]:
        try:
            data = message.to_dict()

            valid, reason = self.validate_raw_dict(data)
            if not valid:
                return False, reason

            valid, reason = self.validate_semantics(data)
            if not valid:
                return False, reason

            return True, "ok"

        except Exception as e:
            return False, str(e)

    
    def create_handshake(
        self,
        *,
        phase: str,
        payload: Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        sender: Optional[str] = None,
        status: Optional[MessageStatus] = None,
    ) -> ProtocolMessage:
        
        return self.build_message(
            msg_type=MessageType.HANDSHAKE,
            channel=Channel.SYSTEM,
            phase=phase,
            payload=payload,
            request_id=request_id,
            in_reply_to=in_reply_to,
            sender=sender,
            status=status,
        )
        
    def create_command(
        self,
        *,
        channel: Channel,
        command: str,
        payload: Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
        sender: Optional[str] = None,
    ) -> ProtocolMessage:
        return self.build_message(
            msg_type=MessageType.COMMAND,
            channel=channel,
            command=command,
            payload=payload,
            request_id=request_id,
            sender=sender,
        )

    def create_reply(
        self,
        *,
        channel: Channel,
        in_reply_to: str,
        payload: Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
        sender: Optional[str] = None,
        status: MessageStatus = MessageStatus.OK,
    ) -> ProtocolMessage:
        return self.build_message(
            msg_type=MessageType.REPLY,
            channel=channel,
            payload=payload,
            request_id=request_id,
            in_reply_to=in_reply_to,
            sender=sender,
            status=status,
        )

    def create_event(
        self,
        *,
        channel: Channel,
        payload: Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
        sender: Optional[str] = None,
        status: Optional[MessageStatus] = None,
    ) -> ProtocolMessage:
        return self.build_message(
            msg_type=MessageType.EVENT,
            channel=channel,
            payload=payload,
            request_id=request_id,
            sender=sender,
            status=status,
        )

    def create_error(
        self,
        *,
        channel: Channel = Channel.SYSTEM,
        payload: Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        sender: Optional[str] = None,
        status: MessageStatus = MessageStatus.ERROR,
    ) -> ProtocolMessage:
        return self.build_message(
            msg_type=MessageType.ERROR,
            channel=channel,
            payload=payload,
            request_id=request_id,
            in_reply_to=in_reply_to,
            sender=sender,
            status=status,
        )
