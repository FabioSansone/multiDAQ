from typing import Optional
import threading

from client.utils.logger import get_logger
from client.hardware.rc.rc_interface import RC
from client.hardware.rc.rc_messages import (
    PROTOCOL_VERSION,
    RCRequest,
    RCResponse,
    RCMessagePriority,
)
from common.message_handler import MessageStatus
from client.hardware.rc.rc_commands import COMMAND_HANDLERS


class RCService:

    def __init__(self):
        self.logger = get_logger("rc_service")
        self.logger.debug("RC Service Initialized")

        self.rc = RC()
        self.rc_lock = threading.RLock()

    def execute_response(
        self,
        rc_request: RCRequest,
        priority: RCMessagePriority = RCMessagePriority.CONTROL,
        timeout_s: float = 5.0,
    ) -> RCResponse:

        try:
            handler = COMMAND_HANDLERS.get(rc_request.command)

            if handler is None:
                return RCResponse(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=rc_request.request_id,
                    in_reply_to=rc_request.request_id,
                    status=MessageStatus.ERROR,
                    error=f"Unknown RC command: {rc_request.command}",
                )

            with self.rc_lock:
                return handler(
                    protocol_version=PROTOCOL_VERSION,
                    rc_interface=self.rc,
                    rc_request=rc_request,
                )

        except Exception as e:
            self.logger.error(f"RC command failed: {e}")

            return RCResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=rc_request.request_id,
                in_reply_to=rc_request.request_id,
                status=MessageStatus.ERROR,
                error=str(e),
            )