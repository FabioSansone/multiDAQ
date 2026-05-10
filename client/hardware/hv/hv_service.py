from typing import Optional
import queue
import threading
import time
import itertools

from client.utils.logger import get_logger
from client.hardware.hv.hv_interface import HV
from client.hardware.hv.hv_messages import (
    PROTOCOL_VERSION,
    HVRequest,
    HVResponse,
    HVMessagePriority,
)
from common.message_handler import MessageStatus
from client.hardware.hv.hv_commands import COMMAND_HANDLERS



class HVService:
    
    CHECK_CHANNELS_PERIOD_S = 5.0
    SAFETY_CHECK_DEADLINE_S = 5.0
    RECOVERY_CHECK_PERIOD_S = 60.0
    RECOVERY_CHECK_DEADLINE_S = 30.0

    def __init__(self, hv_port: str):
        self.logger = get_logger("hv_service")
        self.logger.debug("HV Service Initialized")

        self.hv = HV(hv_port=hv_port)

        self.input_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._counter = itertools.count()

        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None

        self.stop_check_channels = threading.Event()
        self.check_thread: Optional[threading.Thread] = None
    
    def _execute_response(self, hv_request: HVRequest) -> HVResponse:
        try:
            handler = COMMAND_HANDLERS.get(hv_request.command)

            if handler is None:
                return HVResponse(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=hv_request.request_id,
                    in_reply_to=hv_request.request_id,
                    status=MessageStatus.ERROR,
                    error=f"Unknown HV command: {hv_request.command}",
                )

            return handler(
                protocol_version=PROTOCOL_VERSION,
                hv_interface=self.hv,
                hv_request=hv_request,
            )

        except Exception as e:
            self.logger.error(f"HV command failed: {e}")

            return HVResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=hv_request.request_id,
                in_reply_to=hv_request.request_id,
                status=MessageStatus.ERROR,
                error=str(e),
            )
        
        
    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                _, _, hv_request = self.input_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                if (
                    hv_request.deadline_s is not None
                    and time.time() > hv_request.deadline_s
                ):
                    self.logger.warning(
                        f"Skipping expired HV request: {hv_request.request_id}"
                    )

                    if hv_request.response_queue is not None:
                        hv_request.response_queue.put(
                            HVResponse(
                                protocol_version=PROTOCOL_VERSION,
                                request_id=hv_request.request_id,
                                in_reply_to=hv_request.request_id,
                                status=MessageStatus.ERROR,
                                error="HV request expired before execution",
                            )
                        )

                    continue

                response = self._execute_response(hv_request)

                if hv_request.response_queue is not None:
                    hv_request.response_queue.put(response)

            finally:
                self.input_queue.task_done()
                
                
    def request(
        self,
        hv_request: HVRequest,
        priority: HVMessagePriority = HVMessagePriority.CONTROL,
        timeout_s: float = 5.0,
    ) -> HVResponse:

        response_queue: queue.Queue = queue.Queue()
        hv_request.response_queue = response_queue
        
        if hv_request.deadline_s is None:
            hv_request.deadline_s = time.time() + timeout_s

        self.input_queue.put(
            (
                priority,
                next(self._counter),
                hv_request,
            )
        )

        try:
            return response_queue.get(timeout=timeout_s)

        except queue.Empty:
            return HVResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=hv_request.request_id,
                in_reply_to=hv_request.request_id,
                status=MessageStatus.ERROR,
                error="HV request timeout",
            )
        
    
    def start(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            self.logger.warning("HVService already running")
            return
        
        self.stop_event.clear()
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        
        self.worker_thread.start()

        self.start_check()

        self.logger.info("HVService worker started")
        
    def stop(self) -> None:
        self.stop_check()

        self.stop_event.set()
        
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=2.0)
            
        self.logger.info("HVService worker stopped")


    def _check_channels_loop(self) -> None:
        last_recovery_check = 0.0

        while not self.stop_check_channels.is_set():
            now = time.time()

            channels_to_check = self.hv.getOnChannels()
            bad_channels = self.hv.getBadChannels()

            if channels_to_check:
                safety_request = HVRequest(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=f"safety_check_{now}",
                    command="check_channel_safety",
                    payload={"channels": channels_to_check},
                    sender="hv_safety_check",
                    deadline_s=now + self.SAFETY_CHECK_DEADLINE_S,
                )

                self.input_queue.put(
                    (
                        HVMessagePriority.EMERGENCY,
                        next(self._counter),
                        safety_request,
                    )
                )

            if (
                bad_channels
                and (now - last_recovery_check) > self.RECOVERY_CHECK_PERIOD_S
            ):
                recovery_request = HVRequest(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=f"recovery_bad_{now}",
                    command="check_recovery_bad",
                    payload={},
                    sender="hv_bad_recovery",
                    deadline_s=now + self.RECOVERY_CHECK_DEADLINE_S,
                )

                self.input_queue.put(
                    (
                        HVMessagePriority.MONITORING,
                        next(self._counter),
                        recovery_request,
                    )
                )

                last_recovery_check = now

            self.stop_check_channels.wait(self.CHECK_CHANNELS_PERIOD_S)
            


    def start_check(self) -> None:
        if self.check_thread and self.check_thread.is_alive():
            self.logger.warning("Check Channels worker already running")
            return
        
        self.stop_check_channels.clear()
        self.check_thread = threading.Thread(target=self._check_channels_loop, daemon=True)

        self.check_thread.start()
        self.logger.info("Check Channels worker started")

    def stop_check(self) -> None:
        self.stop_check_channels.set()

        if self.check_thread and self.check_thread.is_alive():
            self.check_thread.join(timeout=2.0)
        
        self.logger.info("Check Channels worker stopped")
        
        
