import threading
import logging
from dataclasses import dataclass, field

from common.message_handler import ProtocolMessage


@dataclass
class PendingReply:
    event: threading.Event = field(default_factory=threading.Event)
    message: ProtocolMessage | None = None
    error: str | None = None


class ReplyHandler:

    def __init__(self, logger = None):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("Reply Handler initialized")

        self._reply_waiters: dict[tuple[bytes, str], PendingReply] = {}
        self._reply_waiter_lock = threading.Lock()


    def register_pending_reply(self, client_id: bytes, request_id: str) -> bool:
        key = (client_id, request_id)
        with self._reply_waiter_lock:
            if key in self._reply_waiters:
                self.logger.error(f"Pending reply already registered for {key}")
                return False
            self._reply_waiters[key] = PendingReply()
            return True

    def deliver_pending_reply(self, client_id: bytes, message: ProtocolMessage) -> bool:
        key = (client_id, message.in_reply_to)
        with self._reply_waiter_lock:
            pending = self._reply_waiters.get(key)

            if pending is None:
                self.logger.warning(
                    f"Reply from {client_id!r} for request {message.in_reply_to} "
                    "has no waiter (late or unexpected), discarded"
                )
                return False

            pending.message = message
            pending.event.set()
            return True

    def wait_pending_reply(self, client_id: bytes, request_id: str, timeout_s: float,) -> tuple[ProtocolMessage | None, str]:
        key = (client_id, request_id)
        with self._reply_waiter_lock:
            pending = self._reply_waiters.get(key)

        if pending is None:
            self.logger.error(f"No pending reply registered for {key}")
            return None, "reply not found"

        try:
            if pending.event.wait(timeout_s):
                if pending.message is None:
                    # woken by cancel_client / cancel_all, no reply will come
                    return None, pending.error or "wait cancelled"

                self.logger.debug(f"Reply received for {key}")
                return pending.message, "ok"

            self.logger.error(f"Timeout after {timeout_s}s waiting for reply {key}")
            return None, "timeout waiting for reply"
        finally:
            with self._reply_waiter_lock:
                self._reply_waiters.pop(key, None)

    def _cancel_locked(self, keys: list[tuple[bytes, str]], reason: str) -> int:
        """Wake the waiters of `keys` with an error. Call with the lock held."""
        for key in keys:
            pending = self._reply_waiters.pop(key)
            pending.error = reason
            pending.event.set()
        return len(keys)

    def cancel_all(self, reason: str = "wait cancelled") -> int:
        with self._reply_waiter_lock:
            cancelled = self._cancel_locked(list(self._reply_waiters), reason)

        if cancelled:
            self.logger.warning(f"Cancelled {cancelled} pending replies: {reason}")
        return cancelled

    def cancel_client(self, client_id: bytes, reason: str = "client disconnected") -> int:
        with self._reply_waiter_lock:
            keys = [key for key in self._reply_waiters if key[0] == client_id]
            cancelled = self._cancel_locked(keys, reason)

        if cancelled:
            self.logger.warning(
                f"Cancelled {cancelled} pending replies of client {client_id!r}: {reason}"
            )
        return cancelled
