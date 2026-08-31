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


TRACKED_CONFIGURATION_REGISTERS = {
    31,
    39,
}


class RCService:

    def __init__(
        self,
        configuration_change_callback=None,
    ):
        self.logger = get_logger("rc_service")
        self.logger.debug("RC Service Initialized")

        self.rc = RC()

        self.input_queue: queue.PriorityQueue = (
            queue.PriorityQueue()
        )

        self._counter = itertools.count()

        self.stop_event = threading.Event()
        self.worker_thread: threading.Thread | None = None

        self.configuration_change_callback = (
            configuration_change_callback
        )


    def _submit_command(
        self,
        *,
        command: str,
        payload: dict,
        sender: str,
        priority: RCMessagePriority = (
            RCMessagePriority.CONTROL
        ),
        timeout_s: float = 35.0,
    ) -> RCResponse:

        rc_request = RCRequest(
            protocol_version=PROTOCOL_VERSION,
            request_id=(
                f"{sender}_{command}_{time.time()}"
            ),
            command=command,
            payload=payload,
            sender=sender,
            deadline_s=(
                time.time() + timeout_s
            ),
        )

        return self.request(
            rc_request=rc_request,
            priority=priority,
            timeout_s=timeout_s,
        )


    def _read_configuration_register(
        self,
        address: int,
    ) -> int | None:

        try:

            read_request = RCRequest(
                protocol_version=PROTOCOL_VERSION,
                request_id=(
                    f"rc_service_prewrite_read_"
                    f"{address}_{time.time()}"
                ),
                command="rc_read_register",
                payload={
                    "address": address,
                },
                sender=(
                    "rc_service_configuration_tracking"
                ),
                deadline_s=None,
            )

            handler = COMMAND_HANDLERS.get(
                "rc_read_register"
            )

            if handler is None:

                self.logger.error(
                    "Cannot track RC configuration "
                    "change: rc_read_register "
                    "handler not found"
                )

                return None

            response = handler(
                protocol_version=PROTOCOL_VERSION,
                rc_interface=self.rc,
                rc_request=read_request,
            )

            if response.status != MessageStatus.OK:

                self.logger.warning(
                    "Cannot read previous RC "
                    "configuration value: "
                    f"register={address}, "
                    f"error={response.error}"
                )

                return None

            return (
                response.result or {}
            ).get("value")

        except Exception as exc:

            self.logger.exception(
                "Failed to read previous RC "
                "configuration value: "
                f"register={address}, "
                f"error={exc}"
            )

            return None


    def _notify_configuration_change(
        self,
        *,
        rc_request: RCRequest,
        old_value,
        new_value,
    ) -> None:

        if self.configuration_change_callback is None:
            return

        address = rc_request.payload.get(
            "address"
        )

        if address not in (
            TRACKED_CONFIGURATION_REGISTERS
        ):
            return

        if old_value == new_value:
            return

        try:

            self.configuration_change_callback(
                register=address,
                old_value=old_value,
                new_value=new_value,
                reason=rc_request.sender,
                source_request_id=(
                    rc_request.request_id
                ),
            )

        except Exception as exc:

            #
            # The hardware write has already succeeded.
            # Failure to emit monitoring metadata/event
            # must not turn the RC command into a hardware
            # failure.
            #
            self.logger.exception(
                "Failed to notify RC configuration "
                "change: "
                f"register={address}, "
                f"old_value={old_value}, "
                f"new_value={new_value}, "
                f"error={exc}"
            )


    def _execute_response(
        self,
        rc_request: RCRequest,
    ) -> RCResponse:

        try:

            handler = COMMAND_HANDLERS.get(
                rc_request.command
            )

            if handler is None:

                return RCResponse(
                    protocol_version=PROTOCOL_VERSION,
                    request_id=rc_request.request_id,
                    in_reply_to=rc_request.request_id,
                    status=MessageStatus.ERROR,
                    error=(
                        f"Unknown RC command: "
                        f"{rc_request.command}"
                    ),
                )

            #
            # Capture the previous value only for
            # configuration registers we want to track.
            #
            tracked_configuration_write = False
            previous_value = None

            if rc_request.command == "rc_write_register":

                address = rc_request.payload.get(
                    "address"
                )

                if address in (
                    TRACKED_CONFIGURATION_REGISTERS
                ):

                    tracked_configuration_write = True

                    previous_value = (
                        self._read_configuration_register(
                            address
                        )
                    )

            #
            # Execute the actual requested command.
            #
            response = handler(
                protocol_version=PROTOCOL_VERSION,
                rc_interface=self.rc,
                rc_request=rc_request,
            )

            #
            # Only successful hardware writes become
            # configuration-change events.
            #
            if (
                tracked_configuration_write
                and response.status
                == MessageStatus.OK
            ):

                new_value = rc_request.payload.get(
                    "value"
                )

                self._notify_configuration_change(
                    rc_request=rc_request,
                    old_value=previous_value,
                    new_value=new_value,
                )

            return response

        except Exception as exc:

            self.logger.error(
                f"RC command failed: {exc}"
            )

            return RCResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=rc_request.request_id,
                in_reply_to=rc_request.request_id,
                status=MessageStatus.ERROR,
                error=str(exc),
            )


    def _worker_loop(self) -> None:

        while not self.stop_event.is_set():

            try:

                _, _, rc_request = (
                    self.input_queue.get(
                        timeout=0.2
                    )
                )

            except queue.Empty:
                continue

            try:

                if (
                    rc_request.deadline_s is not None
                    and time.time()
                    > rc_request.deadline_s
                ):

                    self.logger.warning(
                        "Skipping expired RC request: "
                        f"{rc_request.request_id}"
                    )

                    if (
                        rc_request.response_queue
                        is not None
                    ):

                        rc_request.response_queue.put(
                            RCResponse(
                                protocol_version=(
                                    PROTOCOL_VERSION
                                ),
                                request_id=(
                                    rc_request.request_id
                                ),
                                in_reply_to=(
                                    rc_request.request_id
                                ),
                                status=(
                                    MessageStatus.ERROR
                                ),
                                error=(
                                    "RC request expired "
                                    "before execution"
                                ),
                            )
                        )

                    continue

                response = (
                    self._execute_response(
                        rc_request
                    )
                )

                if (
                    rc_request.response_queue
                    is not None
                ):

                    rc_request.response_queue.put(
                        response
                    )

            finally:

                self.input_queue.task_done()


    def request(
        self,
        rc_request: RCRequest,
        priority: RCMessagePriority = (
            RCMessagePriority.CONTROL
        ),
        timeout_s: float = 5.0,
    ) -> RCResponse:

        response_queue: queue.Queue = (
            queue.Queue()
        )

        rc_request.response_queue = (
            response_queue
        )

        if rc_request.deadline_s is None:

            rc_request.deadline_s = (
                time.time() + timeout_s
            )

        self.input_queue.put(
            (
                priority,
                next(self._counter),
                rc_request,
            )
        )

        try:

            return response_queue.get(
                timeout=timeout_s
            )

        except queue.Empty:

            return RCResponse(
                protocol_version=PROTOCOL_VERSION,
                request_id=rc_request.request_id,
                in_reply_to=rc_request.request_id,
                status=MessageStatus.ERROR,
                error="RC request timeout",
            )


    def start(self) -> None:

        if (
            self.worker_thread
            and self.worker_thread.is_alive()
        ):

            self.logger.warning(
                "RCService already running"
            )

            return

        self.stop_event.clear()

        self.worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
        )

        self.worker_thread.start()

        self.logger.info(
            "RCService worker started"
        )


    def stop(self) -> None:

        self.stop_event.set()

        if (
            self.worker_thread
            and self.worker_thread.is_alive()
        ):

            self.worker_thread.join(
                timeout=2.0
            )

        self.logger.info(
            "RCService worker stopped"
        )