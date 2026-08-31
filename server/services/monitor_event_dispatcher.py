import threading

from common.message_handler import ProtocolMessage
from server.utils.logger import get_logger


class MonitorEventDispatcher:

    def __init__(self):

        self._consumer_handlers: dict = {}
        self._consumer_lock = threading.Lock()

        self.logger = get_logger(
            "monitor_event_dispatcher"
        )

        self.logger.debug(
            "Monitoring Event Dispatcher initialized"
        )


    def register_consumer(
        self,
        consumer_id: str,
        consumer_handler,
    ) -> bool:

        with self._consumer_lock:

            if consumer_id in self._consumer_handlers:

                self.logger.warning(
                    "Cannot register monitoring event "
                    f"consumer: consumer_id="
                    f"{consumer_id!r} is already registered"
                )

                return False

            self._consumer_handlers[
                consumer_id
            ] = consumer_handler

        self.logger.info(
            "Monitoring event consumer registered: "
            f"consumer_id={consumer_id!r}"
        )

        return True


    def unregister_consumer(
        self,
        consumer_id: str,
    ) -> bool:

        with self._consumer_lock:

            if consumer_id not in self._consumer_handlers:

                self.logger.warning(
                    "Cannot unregister monitoring event "
                    f"consumer: consumer_id="
                    f"{consumer_id!r} is not registered"
                )

                return False

            del self._consumer_handlers[
                consumer_id
            ]

        self.logger.info(
            "Monitoring event consumer unregistered: "
            f"consumer_id={consumer_id!r}"
        )

        return True


    def dispatch(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        with self._consumer_lock:

            handlers = tuple(
                self._consumer_handlers.items()
            )

        success = True

        for consumer_id, handler in handlers:

            try:
                accepted = handler(
                    client_id,
                    message,
                )

            except Exception as exc:

                self.logger.exception(
                    "Monitoring event consumer failed: "
                    f"client={client_id!r}, "
                    f"consumer_id={consumer_id!r}: "
                    f"{exc}"
                )

                success = False
                continue

            if accepted is False:

                self.logger.warning(
                    "Monitoring event consumer "
                    "rejected event: "
                    f"client={client_id!r}, "
                    f"consumer_id={consumer_id!r}"
                )

                success = False

        return success