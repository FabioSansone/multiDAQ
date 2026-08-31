from server.utils.logger import get_logger
from common.message_handler import ProtocolMessage
from server.services.monitor_stream_service import MonitorStreamService

import threading


class MonitorSampleDispatcher:
    
    
    def __init__(self, stream_service: MonitorStreamService):
        
        self._consumer_handlers: dict = {}
        self._consumer_lock = threading.Lock()
        
        self.stream_service = stream_service
        
        self.logger = get_logger("monitor_sample_dispatcher")
        self.logger.debug("Monitoring Sample Dispatcher initialized")
        
    
    def register_consumer(self, consumer_id: str, consumer_handler) -> bool:
        
        with self._consumer_lock:
            if consumer_id in self._consumer_handlers:
                self.logger.warning(
                    f"Cannot register monitoring sample consumer: "
                    f"consumer_id={consumer_id!r} is already registered"
                )
                return False
            
            self._consumer_handlers[consumer_id] = consumer_handler 
            
            self.logger.info(
                f"Monitoring sample consumer registered: "
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
                    f"Cannot unregister monitoring sample consumer: "
                    f"consumer_id={consumer_id!r} is not registered"
                )
                return False

            del self._consumer_handlers[consumer_id]

        self.logger.info(
            f"Monitoring sample consumer unregistered: "
            f"consumer_id={consumer_id!r}"
        )

        return True
    
    def dispatch(self, client_id: bytes, message: ProtocolMessage):
        
        section = message.channel
        
        possible_subscriptions = self.stream_service.get_due_subscriptions(client_id=client_id, section=section)
        if not possible_subscriptions:
            return True
        
        dispatch_success = True
        for subscription in possible_subscriptions:
            consumer_id = subscription.consumer_id
            with self._consumer_lock:
                consumer_handler = self._consumer_handlers.get(consumer_id)
            
            if consumer_handler is None:
                self.logger.error(
                    f"Monitoring sample consumer handler not found: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer_id={consumer_id!r}"
                )

                dispatch_success = False
                continue

            try:
                accepted = consumer_handler(client_id, message, subscription)
            except Exception as exc:
                self.logger.exception(
                    f"Monitoring sample consumer failed: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer_id={consumer_id!r}: "
                    f"{exc}"
                )
                dispatch_success = False
                continue
            
            if accepted is False:
                self.logger.warning(
                    f"Monitoring sample consumer rejected sample: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer_id={consumer_id!r}"
                )

                dispatch_success = False
                
        return dispatch_success