from dataclasses import dataclass, field, asdict
from enum import IntEnum
from typing import Optional, Any
import queue
from utils.logger import get_logger
from hv_interface import HV
import threading
import time
from common.message_handler import MessageStatus


PROTOCOL_VERSION = 1

class HVMessagePriority(IntEnum):
    EMERGENCY = 0
    CONTROL = 1
    ACQUISITION = 2
    MONITORING = 3



@dataclass(slots=True)
class HVRequest:
    protocol_version: int
    request_id: str
    command: str
    payload: dict[str, Any] = field(default_factory=dict)
    sender: Optional[str] = None
    status: Optional[MessageStatus] = None
    response_queue: Optional[queue.Queue] = None


@dataclass(slots=True)
class HVResponse:
    protocol_version: int
    request_id: str
    status: MessageStatus
    in_reply_to: Optional[str] = None
    result: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class HVService:

    def __init__(self, hv_port: str):
        self.logger = get_logger('hv_service')
        self.logger.debug("HV Service Initialized")
        
        self.hv = HV(hv_port=hv_port)
        
        self.input_queue: queue.PriorityQueue = queue.PriorityQueue()
        
        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
    
    def _execute_response(self, hv_request: HVRequest) -> HVResponse:
        
        try:
            if hv_request.command == "set_common_voltage":
                result = self.hv.set_common_voltage(
                    channels=hv_request.payload["channels"],
                    common_voltage=hv_request.payload["common_voltage"],
                )
                
                return HVResponse(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=hv_request.request_id,
                    in_reply_to=hv_request.request_id,
                    status=MessageStatus.OK,
                    result=result or {}
                )
            
            return HVResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=hv_request.request_id,
                in_reply_to=hv_request.request_id,
                status=MessageStatus.ERROR,
                error=f"Unknown HV command: {hv_request.command}",
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
            
            response = self._execute_response(hv_request)
            
            if hv_request.response_queue is not None:
                hv_request.response_queue.put(response)
            
    def request(self, hv_request: HVRequest, priority: HVMessagePriority = HVMessagePriority.CONTROL, timeout_s: int = 5.0) -> HVResponse:
        
        response_queue: queue.Queue = queue.Queue()
        hv_request.response_queue = response_queue
        
        self.input_queue.put((priority, time.time(), hv_request))
        
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
        self.logger.info("HVService worker started")
        
    def stop(self) -> None:
        self.stop_event.set()
        
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=2.0)
            
        self.logger.info("HVService worker stopped")
        
        
