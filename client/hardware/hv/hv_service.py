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


HV_POLICY_MONITOR_ONLY = "monitor_only"
HV_POLICY_FULL_CONTROL = "full_control"

MONITOR_ONLY_ALLOWED_COMMANDS = {
    "set_hv_sync",
    "check_channel_safety",
    "check_channel_power",
    "check_recovery_bad",
    "check_channel_presence",
    "hv_off",
    "feb_change_address",
}



class HVService:
    
    CHECK_CHANNELS_PERIOD_S = 5.0
    SAFETY_CHECK_DEADLINE_S = 30.0
    RECOVERY_CHECK_PERIOD_S = 300.0
    RECOVERY_CHECK_DEADLINE_S = 30.0
    POWER_CHECK_PERIOD_S = 300.0

    def __init__(self, hv_port: str, state_change_callback=None, hv_policy: str = HV_POLICY_FULL_CONTROL):
        self.logger = get_logger("hv_service")
        self.logger.debug("HV Service Initialized")

        self.hv = HV(hv_port=hv_port)
        self.hv_policy = hv_policy

        self.input_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._counter = itertools.count()

        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None

        self.stop_check_channels = threading.Event()
        self.check_thread: Optional[threading.Thread] = None
        self.warning_queue: queue.Queue = queue.Queue()
        
        self.safety_check_pending = False
        self.recovery_check_pending = False
        self.power_check_pending = False
        self.pending_lock = threading.Lock()  

        self.state_change_callback = state_change_callback
        
        
    def set_policy(self, hv_policy: str) -> None:
        self.hv_policy = hv_policy
        self.logger.info(f"HVService policy set to {hv_policy}")

    def _is_command_allowed(self, command: str) -> bool:
        if self.hv_policy == HV_POLICY_FULL_CONTROL:
            return True

        if self.hv_policy == HV_POLICY_MONITOR_ONLY:
            return command in MONITOR_ONLY_ALLOWED_COMMANDS

        return False


    def _notify_state_change(self, source: str, result: dict) -> None:
        if self.state_change_callback is None:
            return

        try:
            self.state_change_callback()
        except Exception as e:
            self.logger.error(
                f"Error while synchronizing RC register 19 after {source}: {e}"
            )
        
    def _submit_command(self, *, command: str, payload: dict, sender: str, priority: HVMessagePriority = HVMessagePriority.CONTROL, timeout_s: float = 35.0,) -> HVResponse:
        hv_request = HVRequest(
            protocol_version=PROTOCOL_VERSION,
            request_id=f"{sender}_{command}_{time.time()}",
            command=command,
            payload=payload,
            sender=sender,
            deadline_s=time.time() + timeout_s,
        )

        return self.request(
            hv_request=hv_request,
            priority=priority,
            timeout_s=timeout_s,
        )
    def _execute_response(self, hv_request: HVRequest) -> HVResponse:
        if not self._is_command_allowed(hv_request.command):
            self.logger.warning(
                f"HV command blocked by policy {self.hv_policy}: {hv_request.command}"
            )

            return HVResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=hv_request.request_id,
                in_reply_to=hv_request.request_id,
                status=MessageStatus.ERROR,
                error=(
                    f"HV command '{hv_request.command}' not allowed "
                    f"with policy '{self.hv_policy}'"
                ),
                result={
                    "blocked_by_policy": True,
                    "policy": self.hv_policy,
                    "command": hv_request.command,
                },
            )
    
    
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
    
    def _hv_warnings(self, hv_request: HVRequest, hv_response: HVResponse) -> None:
        if hv_request.sender not in {"hv_safety_check", "hv_bad_recovery", "hv_power_check"}:
            return

        if hv_request.command == "check_channel_safety":
            unsafe = hv_response.result.get("unsafe_channels", [])

            if unsafe:
                self.warning_queue.put({
                    "event": "hv_channels_became_bad",
                    "severity": "warning",
                    "source_request_id": hv_request.request_id,
                    "channels": [item["channel"] for item in unsafe],
                    "details": unsafe,
                    "error": hv_response.error,
                })

            return
        
        if hv_request.command == "check_channel_power":
            moved_to_on = hv_response.result.get("moved_to_on_channels", [])
            moved_to_off = hv_response.result.get("moved_to_off_channels", [])

            if moved_to_on or moved_to_off:
                self.warning_queue.put({
                    "event": "hv_power_state_aligned",
                    "severity": "info",
                    "source_request_id": hv_request.request_id,
                    "moved_to_on_channels": moved_to_on,
                    "moved_to_off_channels": moved_to_off,
                    "details": hv_response.result,
                    "error": hv_response.error,
                })

            return

        if hv_request.command == "check_recovery_bad":
            recovered = hv_response.result.get("recovered_channels", [])

            if recovered:
                self.warning_queue.put({
                    "event": "hv_channels_recovered",
                    "severity": "info",
                    "source_request_id": hv_request.request_id,
                    "channels": recovered,
                    "details": hv_response.result,
                    "error": hv_response.error,
                })

            return
        
        
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
                self._hv_warnings(hv_request, response)

                if hv_request.command in {
                    "hv_on",
                    "hv_off",
                    "hv_on_and_wait",
                    "set_hv_sync",
                    "check_channel_safety",
                    "check_channel_power",
                    "check_recovery_bad",
                }:
                    self._notify_state_change(
                        source=hv_request.command,
                        result=response.result or {},
    )

                if hv_request.response_queue is not None:
                    hv_request.response_queue.put(response)

            finally:
                if hv_request.command == "check_channel_safety":
                    with self.pending_lock:
                        self.safety_check_pending = False

                elif hv_request.command == "check_recovery_bad":
                    with self.pending_lock:
                        self.recovery_check_pending = False
                elif hv_request.command == "check_channel_power":
                    with self.pending_lock:
                        self.power_check_pending = False 
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

        try:
            self.hv.close()
        except Exception as e:
            self.logger.error(f"Error while closing HV interface: {e}")
            
        self.logger.info("HVService worker stopped")


    def _check_channels_loop(self) -> None:
        last_recovery_check = time.time()
        last_power_check = time.time()

        while not self.stop_check_channels.is_set():
            now = time.time()

            channels_to_check = self.hv.getOnChannels()
            channels_to_check_power = self.hv.getOkChannels()
            bad_channels = self.hv.getBadChannels()

            if channels_to_check:
                with self.pending_lock:
                    if not self.safety_check_pending:
                        self.safety_check_pending = True
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

            if (channels_to_check_power and (now - last_power_check) > self.POWER_CHECK_PERIOD_S):
                with self.pending_lock:
                    if not self.power_check_pending:
                        self.power_check_pending = True
                        power_request = HVRequest(
                            protocol_version=PROTOCOL_VERSION,
                            request_id=f"power_check_{now}",
                            command="check_channel_power",
                            payload={"channels": channels_to_check_power},
                            sender="hv_power_check",
                            deadline_s=now + self.SAFETY_CHECK_DEADLINE_S,
                        )

                        self.input_queue.put(
                            (
                                HVMessagePriority.MONITORING,
                                next(self._counter),
                                power_request,
                            )
                        )

            if (
                bad_channels
                and (now - last_recovery_check) > self.RECOVERY_CHECK_PERIOD_S
            ):
                with self.pending_lock:
                    if not self.recovery_check_pending:
                        self.recovery_check_pending = True
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
        
        
