from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Any
import queue
from client.utils.logger import get_logger
from client.hardware.hv_interface import HV
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

        self.stop_check_channels = threading.Event()
        self.check_thread: Optional[threading.Thread] = None
        self.possible_alarms = ['OV', 'UV', 'OC', 'UC']
    
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
            
            if hv_request.command == "check_channel_safety":
                chs = hv_request.payload["channels"]

                status_result = self.hv.get_ch_status(channels=chs)
                alarm_result = self.hv.get_ch_alarm(channels=chs)

                unsafe_channels = []
                checked_channels = []

                for ch in chs:
                    status = status_result["status"].get(ch)
                    alarm = alarm_result["alarm"].get(ch)

                    checked_channels.append({
                        "channel": ch,
                        "status": status,
                        "alarm": alarm,
                    })

                    unsafe = (
                        status == "TRIP"
                        or alarm in self.possible_alarms
                    )

                    if unsafe:
                        try:
                            self.hv.reset(channels=ch)
                            self.hv.off(channels=ch)
                        finally:
                            self.hv.moveToBad(channel=ch)

                        unsafe_channels.append({
                            "channel": ch,
                            "status": status,
                            "alarm": alarm,
                            "action": "reset_off_moved_to_bad",
                        })

                if unsafe_channels:
                    return HVResponse(
                        protocol_version=PROTOCOL_VERSION,
                        request_id=hv_request.request_id,
                        in_reply_to=hv_request.request_id,
                        status=MessageStatus.ERROR,
                        result={
                            "checked_channels": checked_channels,
                            "unsafe_channels": unsafe_channels,
                        },
                        error=f"Unsafe HV channels detected: {unsafe_channels}",
                    )
                

                return HVResponse(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=hv_request.request_id,
                    in_reply_to=hv_request.request_id,
                    status=MessageStatus.OK,
                    result={
                        "checked_channels": checked_channels,
                        "unsafe_channels": [],
                        "action": "none",
                    },
                )
            

            if hv_request.command == "check_recovery_bad":
                result = self.hv.recover_bad_channels()

                return HVResponse(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=hv_request.request_id,
                    in_reply_to=hv_request.request_id,
                    status=MessageStatus.OK,
                    result=result or {},
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
                )

                self.input_queue.put(
                    (HVMessagePriority.EMERGENCY, now, safety_request)
                )

            if bad_channels and (now - last_recovery_check) > 60.0:
                recovery_request = HVRequest(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=f"recovery_bad_{now}",
                    command="check_recovery_bad",
                    payload={},
                    sender="hv_bad_recovery",
                )

                self.input_queue.put(
                    (HVMessagePriority.MONITORING, now, recovery_request)
                )

                last_recovery_check = now

            time.sleep(5.0)
            


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
        
        
