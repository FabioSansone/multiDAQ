from dataclasses import dataclass, field
import threading
import time
import copy
from enum import Enum

from common.timeouts import STREAM_RECONCILE_ATTEMPTS, STREAM_RECONCILE_RETRY_S
from server.utils.logger import get_logger
from common.message_handler import Channel

class StreamProducerAction(Enum):
    NONE = "none"
    START = "start"
    RECONFIGURE = "reconfigure"
    STOP = "stop"


@dataclass
class StreamSubscription:
    consumer_id: str
    requested_interval_ns: int
    next_delivery_ns: int
    
@dataclass
class MonitorStreamState:
    client_id: bytes
    section: Channel
    
    active: bool = False
    desired_producer_interval_ns: int | None = None
    active_producer_interval_ns: int | None = None
    
    consumers: dict[str, StreamSubscription] = field(default_factory=dict)

@dataclass(frozen=True)
class StreamUpdateResult:
    client_id: bytes
    section: Channel

    old_desired_producer_interval_ns: int | None
    new_desired_producer_interval_ns: int | None

    active: bool
    action: StreamProducerAction

    
    
class MonitorStreamService:
    
    def __init__(self, monitor_service):
        self._streamers: dict[tuple[bytes, Channel], MonitorStreamState] = {}
        self._lock = threading.Lock()

        self.mon_service = monitor_service

        self._producer_locks: dict[tuple[bytes, Channel],threading.Lock] = {}

        # Reconciliations running in the background, one at a time per client
        self._reconcile_lock = threading.Lock()
        self._reconcile_running: set[bytes] = set()
        self._reconcile_again: set[bytes] = set()
        
        self.logger = get_logger("monitor_stream_service")
        self.logger.debug("Monitoring Stream Service initialized")
            
    
    
    def _stream_key(self, client_id: bytes, section: Channel) -> tuple[bytes, Channel]:
        return client_id, section
    
    def _get_or_create_stream_locked(self, client_id: bytes, section: Channel) -> MonitorStreamState:
        monitor_state_key = self._stream_key(client_id=client_id, section=section)
        
        monitor_stream_state = self._streamers.get(monitor_state_key)
        
        if monitor_stream_state is not None:
            return monitor_stream_state
        
        monitor_stream_state = MonitorStreamState(
            client_id=client_id,
            section=section,
        )
    
        self._streamers[monitor_state_key] = monitor_stream_state
        return monitor_stream_state
    
    def _compute_producer_interval_ns_locked(self, client_id: bytes, section: Channel) -> int | None:
        
        monitor_state_key = self._stream_key(client_id=client_id, section=section)
        monitor_stream_state = self._streamers.get(monitor_state_key)

        if monitor_stream_state is None or not monitor_stream_state.consumers:
            return None
        
        return min(stream_sub.requested_interval_ns for stream_sub in monitor_stream_state.consumers.values())
        
            # min_requested_interval = None
            # for stream_sub in consumers.values():
            #     if min_requested_interval is None:
            #         min_requested_interval = stream_sub.requested_interval_ns
            #     elif stream_sub.requested_interval_ns < min_requested_interval:
            #         min_requested_interval = stream_sub.requested_interval_ns
            
            # return min_requested_interval
    
    def _determine_producer_action_locked(self, active: bool, active_interval_ns: int | None, desired_interval_ns: int | None) -> StreamProducerAction:

        if not active and desired_interval_ns is not None:
            return StreamProducerAction.START

        elif active and desired_interval_ns is None:
            return StreamProducerAction.STOP

        elif active and desired_interval_ns == active_interval_ns:
            return StreamProducerAction.NONE

        elif active and desired_interval_ns != active_interval_ns:
            return StreamProducerAction.RECONFIGURE

        return StreamProducerAction.NONE


    def _set_active_producer_state(self, client_id: bytes, section: Channel, active: bool = False, active_interval_ns: int | None = None) -> None:

        with self._lock:
            monitor_stream_key = self._stream_key(client_id=client_id, section=section)

            monitor_stream_state = self._streamers.get(monitor_stream_key)
            if monitor_stream_state is None:
                return

            if not active and active_interval_ns is not None:
                raise ValueError("")

            if active and active_interval_ns is None:
                raise ValueError
            
            monitor_stream_state.active = active
            monitor_stream_state.active_producer_interval_ns = active_interval_ns

    def _apply_producer_action(
        self,
        update_result: StreamUpdateResult
    ) -> bool:

        action_to_do = update_result.action
        client_id = update_result.client_id
        section = update_result.section
        desired_interval_ns = (
            update_result.new_desired_producer_interval_ns
        )

        if action_to_do == StreamProducerAction.NONE:
            return True

        if action_to_do in {
            StreamProducerAction.START,
            StreamProducerAction.RECONFIGURE,
        }:

            if desired_interval_ns is None:
                raise RuntimeError(
                    "START/RECONFIGURE requires "
                    "a desired producer interval"
                )

            result = self.mon_service.start_sample(
                client_id=client_id,
                section=section,
                interval_s=(
                    desired_interval_ns
                    / 1_000_000_000
                ),
            )

            if not result.get("success"):
                self.logger.error(
                    f"Failed to {action_to_do.value} "
                    f"monitoring producer for "
                    f"client={client_id!r}, "
                    f"section={section}: "
                    f"{result.get('error')}"
                )
                return False

            self._set_active_producer_state(
                client_id=client_id,
                section=section,
                active=True,
                active_interval_ns=desired_interval_ns,
            )

            return True

        if action_to_do == StreamProducerAction.STOP:

            result = self.mon_service.stop_sample(
                client_id=client_id,
                section=section,
            )

            if not result.get("success"):
                self.logger.error(
                    f"Failed to stop monitoring producer "
                    f"for client={client_id!r}, "
                    f"section={section}: "
                    f"{result.get('error')}"
                )
                return False

            self._set_active_producer_state(
                client_id=client_id,
                section=section,
                active=False,
                active_interval_ns=None,
            )

            return True

        raise RuntimeError(
            f"Unsupported producer action: {action_to_do}"
        )

    def _get_producer_lock(
        self,
        client_id: bytes,
        section: Channel,
    ) -> threading.Lock:

        key = self._stream_key(
            client_id=client_id,
            section=section,
        )

        with self._lock:
            lock = self._producer_locks.get(key)

            if lock is None:
                lock = threading.Lock()
                self._producer_locks[key] = lock

            return lock
    
    
    def _restore_stream_state(
        self,
        client_id: bytes,
        section: Channel,
        previous_state: MonitorStreamState | None,
    ) -> None:

        key = self._stream_key(
            client_id=client_id,
            section=section,
        )

        with self._lock:

            if previous_state is None:
                self._streamers.pop(
                    key,
                    None,
                )
                return

            self._streamers[key] = copy.deepcopy(
                previous_state
            )

    def add_or_update_subscription(self, client_id: bytes, section: Channel, consumer: str, requested_consumer_interval_ns: int) -> StreamUpdateResult:
        
        now_ns = time.monotonic_ns()
        
        if requested_consumer_interval_ns <= 0:
            raise ValueError("Requested consumer interval must be greater than zero")
        
        with self._lock:
            monitor_stream_state = self._get_or_create_stream_locked(client_id=client_id, section=section)
            monitor_stream_sub = monitor_stream_state.consumers.get(consumer)
            
            if monitor_stream_sub is None:
                monitor_stream_state.consumers[consumer] = StreamSubscription(consumer_id=consumer, requested_interval_ns=requested_consumer_interval_ns, next_delivery_ns=now_ns)
            else:
                monitor_stream_sub.requested_interval_ns = requested_consumer_interval_ns
                monitor_stream_sub.next_delivery_ns = now_ns

            old_desired_producer_interval_ns = monitor_stream_state.desired_producer_interval_ns
            monitor_stream_state.desired_producer_interval_ns = self._compute_producer_interval_ns_locked(client_id=client_id, section=section)

            action = self._determine_producer_action_locked(active=monitor_stream_state.active, active_interval_ns=monitor_stream_state.active_producer_interval_ns, desired_interval_ns=monitor_stream_state.desired_producer_interval_ns)

            return StreamUpdateResult(client_id=client_id, section=section, old_desired_producer_interval_ns=old_desired_producer_interval_ns, new_desired_producer_interval_ns=monitor_stream_state.desired_producer_interval_ns, active=monitor_stream_state.active, action=action)
            
            
    def remove_subscription(
        self,
        client_id: bytes,
        section: Channel,
        consumer: str
    ) -> StreamUpdateResult | None:
        
        monitor_state_key = self._stream_key(
            client_id=client_id,
            section=section
        )
        
        with self._lock:
            monitor_stream_state = self._streamers.get(
                monitor_state_key
            )
            
            if monitor_stream_state is None:
                self.logger.warning(
                    f"Cannot remove subscription: stream not found "
                    f"for client={client_id!r}, section={section}"
                )
                return None
            
            if consumer not in monitor_stream_state.consumers:
                self.logger.warning(
                    f"Cannot remove subscription: consumer={consumer!r} "
                    f"not found for client={client_id!r}, section={section}"
                )
                return None
            
            old_desired_producer_interval_ns = (
                monitor_stream_state.desired_producer_interval_ns
            )
            
            del monitor_stream_state.consumers[consumer]
            
            monitor_stream_state.desired_producer_interval_ns = (
                self._compute_producer_interval_ns_locked(
                    client_id=client_id,
                    section=section
                )
            )
            
            action = self._determine_producer_action_locked(
                active=monitor_stream_state.active,
                active_interval_ns=monitor_stream_state.active_producer_interval_ns,
                desired_interval_ns=monitor_stream_state.desired_producer_interval_ns
            )
            
            return StreamUpdateResult(
                client_id=client_id,
                section=section,
                old_desired_producer_interval_ns=old_desired_producer_interval_ns,
                new_desired_producer_interval_ns=monitor_stream_state.desired_producer_interval_ns,
                active=monitor_stream_state.active,
                action=action
            )
    
    
    def get_stream(
        self,
        client_id: bytes,
        section: Channel
    ) -> MonitorStreamState | None:
        
        monitor_state_key = self._stream_key(
            client_id=client_id,
            section=section
        )
        
        with self._lock:
            monitor_stream_state = self._streamers.get(
                monitor_state_key
            )
            
            if monitor_stream_state is None:
                return None
            
            return copy.deepcopy(monitor_stream_state)
    
    def list_streams(self) -> list[MonitorStreamState]:
    
        with self._lock:
            return copy.deepcopy(
                list(self._streamers.values())
            )

    def subscribe(
        self,
        client_id: bytes,
        section: Channel,
        consumer: str,
        requested_interval_ns: int,
    ) -> bool:

        producer_lock = self._get_producer_lock(
            client_id,
            section,
        )

        with producer_lock:
            previous_state = self.get_stream(
                client_id=client_id,
                section=section,
            )
            
            update_result = self.add_or_update_subscription(
                client_id=client_id,
                section=section,
                consumer=consumer,
                requested_consumer_interval_ns=requested_interval_ns,
            )

            try:
                success = self._apply_producer_action(
                    update_result=update_result
                )
            except Exception as e:
                self.logger.exception(
                    f"Monitoring subscription producer "
                    f"update raised an exception: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer={consumer!r}: "
                    f"{e}"
                )

                success = False

            if not success:
                self._restore_stream_state(
                    client_id=client_id,
                    section=section,
                    previous_state=previous_state,
                )

                self.logger.error(
                    f"Monitoring subscription rolled back: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer={consumer!r}, "
                    f"producer_action="
                    f"{update_result.action.value}"
                )

                return False

        self.logger.info(
            f"Monitoring subscription active: "
            f"client={client_id!r}, "
            f"section={section.value}, "
            f"consumer={consumer!r}, "
            f"requested_interval="
            f"{requested_interval_ns / 1e9:.3f}s, "
            f"producer_action="
            f"{update_result.action.value}, "
            f"desired_producer_interval="
            f"{update_result.new_desired_producer_interval_ns / 1e9:.3f}s"
        )

        return True     


    def unsubscribe(
        self,
        client_id: bytes,
        section: Channel,
        consumer: str,
    ) -> bool:

        producer_lock = self._get_producer_lock(
            client_id,
            section,
        )

        with producer_lock:
            previous_state = self.get_stream(
                client_id=client_id,
                section=section,
            )
            
            if previous_state is None:
                self.logger.warning(
                    f"Monitoring unsubscribe failed: "
                    f"stream not found for "
                    f"client={client_id!r}, "
                    f"section={section.value}"
                )
                return False

            update_result = self.remove_subscription(
                client_id=client_id,
                section=section,
                consumer=consumer,
            )

            if update_result is None:
                self.logger.warning(
                    f"Monitoring unsubscribe failed: "
                    f"subscription not found for "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer={consumer!r}"
                )
                return False

            try:
                
                success = self._apply_producer_action(
                    update_result=update_result
                )
            except Exception as e:
                self.logger.exception(
                    f"Monitoring unsubscribe producer "
                    f"update raised an exception: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer={consumer!r}: "
                    f"{e}"
                )

                success = False

            if not success:
                self._restore_stream_state(
                    client_id=client_id,
                    section=section,
                    previous_state=previous_state,
                )

                self.logger.error(
                    f"Monitoring unsubscribe rolled back: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"consumer={consumer!r}, "
                    f"producer_action="
                    f"{update_result.action.value}"
                )

                return False

        desired_interval_ns = (
            update_result.new_desired_producer_interval_ns
        )

        desired_interval_text = (
            "none"
            if desired_interval_ns is None
            else f"{desired_interval_ns / 1e9:.3f}s"
        )

        self.logger.info(
            f"Monitoring subscription removed: "
            f"client={client_id!r}, "
            f"section={section.value}, "
            f"consumer={consumer!r}, "
            f"producer_action="
            f"{update_result.action.value}, "
            f"desired_producer_interval="
            f"{desired_interval_text}"
        )

        return True  
    
    
    def get_due_subscriptions(self, client_id: bytes, section: Channel, now_ns: int | None = None) -> tuple[StreamSubscription, ...]:
        
        if now_ns is None:
            now_ns = time.monotonic_ns()
            
        due_subscriptions: list[StreamSubscription] = []
        
        with self._lock:
            monitor_stream_key = self._stream_key(client_id=client_id, section=section)
            
            monitor_stream_state = self._streamers.get(monitor_stream_key)
            if monitor_stream_state is None:
                return ()
            
            for stream_subscription in monitor_stream_state.consumers.values():
                if now_ns < stream_subscription.next_delivery_ns:
                    continue
                
                
                while stream_subscription.next_delivery_ns <= now_ns:
                    stream_subscription.next_delivery_ns += stream_subscription.requested_interval_ns

                due_subscriptions.append(copy.deepcopy(stream_subscription))
                
        return tuple(due_subscriptions)
        
    
    
    def invalidate_client_actual_state(
        self,
        client_id: bytes,
    ) -> None:

        with self._lock:
            client_streams = [
                (
                    state.client_id,
                    state.section,
                )
                for state in self._streamers.values()
                if state.client_id == client_id
            ]

        for stream_client_id, section in client_streams:

            producer_lock = self._get_producer_lock(
                client_id=stream_client_id,
                section=section,
            )

            with producer_lock:
                with self._lock:

                    key = self._stream_key(
                        client_id=stream_client_id,
                        section=section,
                    )

                    state = self._streamers.get(key)

                    if state is None:
                        continue

                    state.active = False
                    state.active_producer_interval_ns = None

        if client_streams:
            self.logger.info(
                f"Monitoring producer actual state invalidated: "
                f"client={client_id!r}, "
                f"streams={len(client_streams)}"
            )


    def reconcile_client_streams(
        self,
        client_id: bytes,
    ) -> bool:

        with self._lock:
            sections = [
                state.section
                for state in self._streamers.values()
                if (
                    state.client_id == client_id
                    and state.desired_producer_interval_ns is not None
                )
            ]

        if not sections:
            self.logger.debug(
                f"No monitoring streams to reconcile for "
                f"client={client_id!r}"
            )
            return True

        all_success = True

        for section in sections:

            producer_lock = self._get_producer_lock(
                client_id=client_id,
                section=section,
            )

            with producer_lock:

                with self._lock:

                    key = self._stream_key(
                        client_id=client_id,
                        section=section,
                    )

                    state = self._streamers.get(key)

                    if state is None:
                        continue

                    desired_interval_ns = (
                        state.desired_producer_interval_ns
                    )

                    action = (
                        self._determine_producer_action_locked(
                            active=state.active,
                            active_interval_ns=(
                                state.active_producer_interval_ns
                            ),
                            desired_interval_ns=desired_interval_ns,
                        )
                    )

                    update_result = StreamUpdateResult(
                        client_id=client_id,
                        section=section,
                        old_desired_producer_interval_ns=(
                            desired_interval_ns
                        ),
                        new_desired_producer_interval_ns=(
                            desired_interval_ns
                        ),
                        active=state.active,
                        action=action,
                    )

                success = self._apply_producer_action(
                    update_result=update_result
                )

            if not success:

                all_success = False

                self.logger.error(
                    f"Monitoring stream reconciliation failed: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"action={action.value}"
                )

                continue

            self.logger.info(
                f"Monitoring stream reconciled: "
                f"client={client_id!r}, "
                f"section={section.value}, "
                f"action={action.value}"
            )

        return all_success


    def reconcile_client_streams_in_background(self, client_id: bytes, on_done=None) -> bool:
        """Reconcile the streams of a client that came back, in a worker thread: the callbacks of the
        monitoring plane run in its I/O thread and must not wait for the network, and the reconciliation
        sends commands and waits for their replies.

        `on_done(client_id, success)` is called by the worker when it is over. If a reconciliation of the
        same client is already running it is asked to go through again once it ends (the client may have
        restarted meanwhile) and this call returns False."""

        with self._reconcile_lock:
            if client_id in self._reconcile_running:
                self._reconcile_again.add(client_id)
                self.logger.info(
                    f"Monitoring stream reconciliation already running for client={client_id!r}: it will run again"
                )
                return False

            self._reconcile_running.add(client_id)

        threading.Thread(
            target=self._reconcile_worker,
            args=(client_id, on_done),
            daemon=True,
            name="monitor-stream-reconcile",
        ).start()

        return True

    def _reconcile_worker(self, client_id: bytes, on_done) -> None:
        success = False

        try:
            while True:
                success = self._reconcile_with_retries(client_id)

                with self._reconcile_lock:
                    if client_id in self._reconcile_again:
                        self._reconcile_again.discard(client_id)
                        continue

                    self._reconcile_running.discard(client_id)
                    break

        except Exception:
            self.logger.exception(f"Monitoring stream reconciliation crashed: client={client_id!r}")
            success = False

            with self._reconcile_lock:
                self._reconcile_running.discard(client_id)
                self._reconcile_again.discard(client_id)

        if on_done is None:
            return

        try:
            on_done(client_id, success)
        except Exception:
            self.logger.exception(f"Monitoring stream reconciliation callback failed: client={client_id!r}")

    def _reconcile_with_retries(self, client_id: bytes) -> bool:
        """The streams that are already running are left alone (action NONE): trying again is safe."""

        for attempt in range(1, STREAM_RECONCILE_ATTEMPTS + 1):
            if not self.mon_service.monitoring_manager.is_client_connected(client_id):
                self.logger.warning(
                    f"Monitoring stream reconciliation stopped: client={client_id!r} left the monitoring plane again"
                )
                return False

            if self.reconcile_client_streams(client_id=client_id):
                return True

            if attempt < STREAM_RECONCILE_ATTEMPTS:
                self.logger.warning(
                    f"Monitoring stream reconciliation failed for client={client_id!r} "
                    f"(attempt {attempt}/{STREAM_RECONCILE_ATTEMPTS}), trying again in {STREAM_RECONCILE_RETRY_S} s"
                )
                time.sleep(STREAM_RECONCILE_RETRY_S)

        self.logger.error(
            f"Monitoring stream reconciliation failed for client={client_id!r} "
            f"after {STREAM_RECONCILE_ATTEMPTS} attempts"
        )
        return False
