import threading
import time
import queue
import itertools

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

        self.input_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._counter = itertools.count()

        self.stop_event = threading.Event()
        self.worker_thread: threading.Thread | None = None

    def _submit_command(
        self, *, command: str, payload: dict, sender: str,
        priority: RCMessagePriority = RCMessagePriority.CONTROL, timeout_s: float = 35.0,
    ) -> RCResponse:
        rc_request = RCRequest(
            protocol_version=PROTOCOL_VERSION,
            request_id=f"{sender}_{command}_{time.time()}",
            command=command,
            payload=payload,
            sender=sender,
            deadline_s=time.time() + timeout_s,
        )

        return self.request(
            rc_request=rc_request,
            priority=priority,
            timeout_s=timeout_s,
        )

    def _execute_response(self, rc_request: RCRequest) -> RCResponse:
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

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                _, _, rc_request = self.input_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                if (
                    rc_request.deadline_s is not None
                    and time.time() > rc_request.deadline_s
                ):
                    self.logger.warning(f"Skipping expired RC request: {rc_request.request_id}")
                    if rc_request.response_queue is not None:
                        rc_request.response_queue.put(
                            RCResponse(
                                protocol_version=PROTOCOL_VERSION,
                                request_id=rc_request.request_id,
                                in_reply_to=rc_request.request_id,
                                status=MessageStatus.ERROR,
                                error="RC request expired before execution",
                            )
                        )
                    continue

                response = self._execute_response(rc_request)

                if rc_request.response_queue is not None:
                    rc_request.response_queue.put(response)

            finally:
                self.input_queue.task_done()

    def request(
        self,
        rc_request: RCRequest,
        priority: RCMessagePriority = RCMessagePriority.CONTROL,
        timeout_s: float = 5.0,
    ) -> RCResponse:

        response_queue: queue.Queue = queue.Queue()
        rc_request.response_queue = response_queue

        if rc_request.deadline_s is None:
            rc_request.deadline_s = time.time() + timeout_s

        self.input_queue.put((priority, next(self._counter), rc_request))

        try:
            return response_queue.get(timeout=timeout_s)
        except queue.Empty:
            return RCResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=rc_request.request_id,
                in_reply_to=rc_request.request_id,
                status=MessageStatus.ERROR,
                error="RC request timeout",
            )

    def start(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            self.logger.warning("RCService already running")
            return

        self.stop_event.clear()
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        self.logger.info("RCService worker started")

    def stop(self) -> None:
        self.stop_event.set()

        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=2.0)

        self.logger.info("RCService worker stopped")