from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Any
import queue
from common.message_handler import MessageStatus


PROTOCOL_VERSION = 1


class RCMessagePriority(IntEnum):
    EMERGENCY = 0
    CONTROL = 1
    ACQUISITION = 2
    MONITORING = 3


@dataclass(slots=True)
class RCRequest:
    protocol_version: int
    request_id: str
    command: str
    payload: dict[str, Any] = field(default_factory=dict)
    sender: Optional[str] = None
    status: Optional[MessageStatus] = None
    response_queue: Optional[queue.Queue] = None


@dataclass(slots=True)
class RCResponse:
    protocol_version: int
    request_id: str
    status: MessageStatus
    in_reply_to: Optional[str] = None
    result: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None