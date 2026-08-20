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

    CHECK_SENSORS_PERIOD_S = 300.0   # 5 minuti
    SENSORS_CHECK_DEADLINE_S = 30.0


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
        self.pending_lock = threading.Lock()
    
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

    def _main_warnings(self, main_request: MainRequest, main_response: MainResponse) -> None:

        if main_request.sender != "main_sensors_check":
            return

        if main_request.command != "check_sensor_thresholds":
            return

        out_of_range = main_response.result.get("out_of_range", [])

        if out_of_range:
            self.warning_queue.put({
                "event": "sensor_threshold_exceeded",
                "severity": "warning",
                "source_request_id": main_request.request_id,
                "details": out_of_range,
                "error": main_response.error,
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
                if main_request.command == "check_sensor_thresholds":
                    with self.pending_lock:
                        self.sensors_check_pending = False

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

        #self.start_check()

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
        while not self.stop_check_sensors.is_set():
            now = time.time()

            with self.pending_lock:
                if not self.sensors_check_pending:
                    self.sensors_check_pending = True
                    sensors_request = MainRequest(
                        protocol_version=PROTOCOL_VERSION,
                        request_id=f"sensors_check_{now}",
                        command="check_sensor_thresholds",
                        payload={},
                        sender="main_sensors_check",
                        deadline_s=now + self.SENSORS_CHECK_DEADLINE_S,
                    )

                    self.input_queue.put(
                        (MainMessagePriority.MONITORING, next(self._counter), sensors_request)
                    )

            self.stop_check_sensors.wait(self.CHECK_SENSORS_PERIOD_S)

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