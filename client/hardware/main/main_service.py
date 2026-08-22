from typing import Optional
import queue
import threading
import time
import itertools

from client.utils.logger import get_logger
from client.hardware.main.main_interface import MAIN
from client.hardware.main.main_messages import (
    PROTOCOL_VERSION,
    MainRequest,
    MainResponse,
    MainMessagePriority,
)
from common.message_handler import MessageStatus
from client.hardware.main.main_commands import COMMAND_HANDLERS


class MainService:

    CHECK_THRESHOLDS_PERIOD_S = 300.0
    CHECK_THRESHOLDS_DEADLINE_S = 30.0

    CHECK_EVENTS_PERIOD_S = 5.0
    CHECK_EVENTS_DEADLINE_S = 20.0


    def __init__(self, cached_i2c_bus: int | None = None):
        self.logger = get_logger("main_service")
        self.logger.debug("Main Service Initialized")

        self.main = MAIN(cached_i2c_bus=cached_i2c_bus)

        self.input_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._counter = itertools.count()

        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None

        self.warning_queue: queue.Queue = queue.Queue()

        self.stop_check_sensors = threading.Event()
        self.check_thread: Optional[threading.Thread] = None
        self.sensors_check_pending = False
        self.event_check_pending = False
        self.pending_lock = threading.Lock()
        
        self.active_threshold_alarms: set[str] = set()
        self.threshold_alarm_lock = threading.Lock()
    
    def get_i2c_bus(self) -> int | None:
        return self.main.i2c_bus

    def _submit_command(
        self,
        *,
        command: str,
        payload: dict,
        sender: str,
        priority: MainMessagePriority = MainMessagePriority.MONITORING,
        timeout_s: float = 35.0,
    ) -> MainResponse:
        main_request = MainRequest(
            protocol_version=PROTOCOL_VERSION,
            request_id=f"{sender}_{command}_{time.time()}",
            command=command,
            payload=payload,
            sender=sender,
            deadline_s=time.time() + timeout_s,
        )

        return self.request(
            main_request=main_request,
            priority=priority,
            timeout_s=timeout_s,
        )

    def _execute_response(self, main_request: MainRequest) -> MainResponse:
        try:
            handler = COMMAND_HANDLERS.get(main_request.command)

            if handler is None:
                return MainResponse(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=main_request.request_id,
                    in_reply_to=main_request.request_id,
                    status=MessageStatus.ERROR,
                    error=f"Unknown Main command: {main_request.command}",
                )

            return handler(
                protocol_version=PROTOCOL_VERSION,
                main_interface=self.main,
                main_request=main_request,
            )

        except Exception as e:
            self.logger.error(f"Main command failed: {e}")

            return MainResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=main_request.request_id,
                in_reply_to=main_request.request_id,
                status=MessageStatus.ERROR,
                error=str(e),
            )

    
    def _handle_threshold_result(self, main_request: MainRequest, main_response: MainResponse) -> None:
        
        out_of_range = main_response.result.get("out_of_range", [])
        
        current_alarms = {item["sensor"] for item in out_of_range}
        alarm_data = {item["sensor"]: item for item in out_of_range}
        unavailable = set(main_response.result.get("unavailable", []))
        
        with self.threshold_alarm_lock:
            previous_alarms = set(self.active_threshold_alarms)
            new_alarms = (current_alarms - previous_alarms)
            recovered_alarms = (previous_alarms - current_alarms - unavailable)
            still_active_unavailable = (previous_alarms & unavailable)
            
            
            self.active_threshold_alarms = (current_alarms | still_active_unavailable)
            
        for sensor in sorted(new_alarms):
            details = alarm_data[sensor]
            
            self.logger.warning(
                "MAIN sensor threshold exceeded: "
                f"sensor={sensor}, "
                f"value={details.get('value')}, "
                f"min={details.get('min')}, "
                f"max={details.get('max')}"
            )

            self.warning_queue.put({
                "event": "sensor_threshold_exceeded",
                "severity": "warning",
                "source_request_id": (
                    main_request.request_id
                ),
                "details": details,
                "error": None,
            })
            
            values = main_response.result.get("values", {})
            
            for sensor in sorted(recovered_alarms):
                value = values.get(sensor)
                
                self.logger.info(
                    "MAIN sensor threshold recovered: "
                    f"sensor={sensor}, "
                    f"value={value}"
                )

                self.warning_queue.put({
                    "event": "sensor_threshold_recovered",
                    "severity": "info",
                    "source_request_id": (
                        main_request.request_id
                    ),
                    "details": {
                        "sensor": sensor,
                        "value": value,
                    },
                    "error": None,
                })
                
        
        
    def _main_warnings(self, main_request: MainRequest, main_response: MainResponse) -> None:

        if main_request.sender not in {"main_sensors_check", "main_event_check"}:
            return

        if main_request.command not in {"main_check_thresholds", "main_check_events"}:
            return
        
        if main_response.status != MessageStatus.OK:
            self.logger.warning(
                f"Periodic MAIN check failed: "
                f"command={main_request.command}, "
                f"error={main_response.error}"
            )
            return

        if main_request.command == "main_check_thresholds":
            self._handle_threshold_result(
                main_request,
                main_response,
            )

            return
        
        if main_request.command == "main_check_events":
            events = main_response.result.get("events", [])
            
            for event in events:
                self.warning_queue.put({
                    "event": event.get(
                        "event",
                        "main_sensor_event",
                    ),
                    "severity": "warning",
                    "source_request_id": main_request.request_id,
                    "details": event,
                    "error": None,
                })

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                _, _, main_request = self.input_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                if (
                    main_request.deadline_s is not None
                    and time.time() > main_request.deadline_s
                ):
                    self.logger.warning(
                        f"Skipping expired Main request: {main_request.request_id}"
                    )

                    if main_request.response_queue is not None:
                        main_request.response_queue.put(
                            MainResponse(
                                protocol_version=PROTOCOL_VERSION,
                                request_id=main_request.request_id,
                                in_reply_to=main_request.request_id,
                                status=MessageStatus.ERROR,
                                error="Main request expired before execution",
                            )
                        )

                    continue

                response = self._execute_response(main_request)
                self._main_warnings(main_request, response)

                if main_request.response_queue is not None:
                    main_request.response_queue.put(response)

            finally:
                if main_request.command == "main_check_thresholds":
                    with self.pending_lock:
                        self.sensors_check_pending = False

                elif main_request.command == "main_check_events":
                    with self.pending_lock:
                        self.event_check_pending = False

                self.input_queue.task_done()

    def request(
        self,
        main_request: MainRequest,
        priority: MainMessagePriority = MainMessagePriority.MONITORING,
        timeout_s: float = 5.0,
    ) -> MainResponse:

        response_queue: queue.Queue = queue.Queue()
        main_request.response_queue = response_queue

        if main_request.deadline_s is None:
            main_request.deadline_s = time.time() + timeout_s

        self.input_queue.put(
            (
                priority,
                next(self._counter),
                main_request,
            )
        )

        try:
            return response_queue.get(timeout=timeout_s)

        except queue.Empty:
            return MainResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=main_request.request_id,
                in_reply_to=main_request.request_id,
                status=MessageStatus.ERROR,
                error="Main request timeout",
            )

    def start(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            self.logger.warning("MainService already running")
            return

        self.stop_event.clear()
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)

        self.worker_thread.start()

        self.start_check()

        self.logger.info("MainService worker started")

    def stop(self) -> None:
        self.stop_check()

        self.stop_event.set()

        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=2.0)

        try:
            self.main.close()
        except Exception as e:
            self.logger.error(f"Error while closing Main interface: {e}")

        self.logger.info("MainService worker stopped")

    def _check_sensors_loop(self) -> None:
        
        next_threshold_check = time.monotonic()
        next_event_check = time.monotonic()
        
        while not self.stop_check_sensors.is_set():
            
            now_monotonic = time.monotonic()
            now_wall = time.time()
            
            if now_monotonic >= next_threshold_check:
                with self.pending_lock:
                    threshold_pending = self.sensors_check_pending
                    if not threshold_pending:
                        self.sensors_check_pending = True
                    if not threshold_pending:
                        threshold_request = MainRequest(
                            protocol_version=PROTOCOL_VERSION,
                            request_id=f"sensors_check_{now_wall}",
                            command="main_check_thresholds",
                            payload={},
                            sender="main_sensors_check",
                            deadline_s=now_wall + self.CHECK_THRESHOLDS_DEADLINE_S,
                        )

                        self.input_queue.put(
                            (MainMessagePriority.MONITORING, next(self._counter), threshold_request)
                        )
                        
                        next_threshold_check += self.CHECK_THRESHOLDS_PERIOD_S
                        
                        while next_threshold_check <= now_monotonic:
                            next_threshold_check += self.CHECK_THRESHOLDS_PERIOD_S
                
            
            if now_monotonic >= next_event_check:

                with self.pending_lock:
                    event_pending = self.event_check_pending

                    if not event_pending:
                        self.event_check_pending = True

                if not event_pending:

                    event_request = MainRequest(
                        protocol_version=PROTOCOL_VERSION,
                        request_id=f"event_check_{now_wall}",
                        command="main_check_events",
                        payload={},
                        sender="main_event_check",
                        deadline_s=(
                            now_wall
                            + self.CHECK_EVENTS_DEADLINE_S
                        ),
                    )

                    self.input_queue.put(
                        (
                            MainMessagePriority.MONITORING,
                            next(self._counter),
                            event_request,
                        )
                    )

                next_event_check += (
                    self.CHECK_EVENTS_PERIOD_S
                )

                while next_event_check <= now_monotonic:
                    next_event_check += (
                        self.CHECK_EVENTS_PERIOD_S
                    )   
                

            next_check = min(next_threshold_check, next_event_check,)
            wait_s = max(0.0, next_check - time.monotonic(),)
            self.stop_check_sensors.wait(wait_s)

    def start_check(self) -> None:
        if self.check_thread and self.check_thread.is_alive():
            self.logger.warning("Check Sensors worker already running")
            return

        self.stop_check_sensors.clear()
        self.check_thread = threading.Thread(target=self._check_sensors_loop, daemon=True)

        self.check_thread.start()
        self.logger.info("Check Sensors worker started")

    def stop_check(self) -> None:
        self.stop_check_sensors.set()

        if self.check_thread and self.check_thread.is_alive():
            self.check_thread.join(timeout=2.0)

        self.logger.info("Check Sensors worker stopped")