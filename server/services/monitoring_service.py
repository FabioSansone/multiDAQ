from server.services.client_command_service import CommandPlane
from common.message_handler import Channel, MessageStatus
from server.services.time_sync_service import ClientTimeSyncState
from server.utils.logger import get_logger

from typing import Optional
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor

TIME_SYNC_PERIOD_S = 300.0
TIME_SYNC_RETRY_S = 60.0
TIME_SYNC_MAX_AGE_S = 900.0
SYNC_WORKERS = 8

class MonitoringService:

    def __init__(
        self,
        command_service,
        monitoring_manager,
        time_sync_service,
        output_func=None,
    ) -> None:

        self.command_service = command_service
        self.monitoring_manager = monitoring_manager
        self.time_sync_service = time_sync_service

        self.poutput = output_func or (lambda message: None)
        
        self._time_sync_schedule_lock = threading.Lock()
        self._next_resync_ns: dict[bytes, int] = {}
        
        self._scheduler_stop_event = threading.Event()
        self._scheduler_wakeup_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None

        self._sync_executor : Optional[ThreadPoolExecutor] = None
        self._resync_in_progress: set[bytes] = set()

        self.logger = get_logger("monitoring_service")
        self.logger.debug("Monitoring Service initialized")
        
    
    @staticmethod
    def _time_sync_phase(client_id: bytes, period_ns: int) -> int:
        digest = hashlib.sha256(client_id + b":time_sync_resync").digest()
        
        value = int.from_bytes(digest[:8], byteorder="big", signed=False)
        
        return value % period_ns


    def _extract_reply(
        self,
        *,
        client_id: bytes,
        section: str,
        reply,
        reason: str,
    ) -> dict:

        client_name = client_id.decode(errors="ignore")

        if reply is None:
            self.logger.error(
                f"Monitoring {section} snapshot failed for "
                f"client {client_name}: {reason}"
            )

            return {
                "success": False,
                "result": {},
                "error": reason,
            }

        payload = reply.payload or {}

        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        return {
            "success": status == "ok",
            "status": status,
            "result": result,
            "error": error,
        }


    def read_main_snapshot(
        self,
        client_id: bytes,
        timeout_s: float = 35.0,
    ) -> dict:

        reply, reason = self.command_service.send_main_command(
            client_id=client_id,
            command="main_read_snapshot",
            payload={},
            plane=CommandPlane.MONITORING,
            timeout_s=timeout_s,
        )

        return self._extract_reply(
            client_id=client_id,
            section="main",
            reply=reply,
            reason=reason,
        )


    def read_rc_snapshot(
        self,
        client_id: bytes,
        channels="all",
        timeout_s: float = 35.0,
    ) -> dict:

        reply, reason = self.command_service.send_rc_command(
            client_id=client_id,
            command="rc_all_rate_monitoring",
            payload={
                "channels": channels,
            },
            plane=CommandPlane.MONITORING,
            timeout_s=timeout_s,
        )

        return self._extract_reply(
            client_id=client_id,
            section="rc",
            reply=reply,
            reason=reason,
        )


    def read_hv_snapshot(
        self,
        client_id: bytes,
        channels="all",
        timeout_s: float = 60.0,
    ) -> dict:

        reply, reason = self.command_service.send_hv_command(
            client_id=client_id,
            command="hv_monitor_snapshot",
            payload={
                "channels": channels,
            },
            plane=CommandPlane.MONITORING,
            timeout_s=timeout_s,
        )

        return self._extract_reply(
            client_id=client_id,
            section="hv",
            reply=reply,
            reason=reason,
        )


    def start_sample(
        self,
        client_id: bytes,
        section: Channel,
        interval_s: float,
        timeout_s: float = 10.0,
    ) -> dict:
            
            
        if section not in {
            Channel.RC,
            Channel.HV,
            Channel.MAIN,
        }:
            return {
                "success": False,
                "error": f"Unsupported sample section: {section}",
            }
            
        if not self.ensure_client_synchronized(client_id):
            client_name = client_id.decode(errors="ignore")
            self.logger.error(
                "Cannot start monitoring samples: "
                f"time synchronization failed for client {client_name}"
            )

            return {
                "success": False,
                "result": {},
                "error": "client time synchronization failed",
            }

        reply, reason = (
            self.command_service.send_monitoring_command(
                client_id=client_id,
                command="sample_start",
                payload={
                    "section": section.value,
                    "interval_s": interval_s,
                },
                timeout_s=timeout_s,
            )
        )

        if reply is None:
            client_name = client_id.decode(errors="ignore")

            self.logger.error(
                f"Failed to start {section.value} samples "
                f"for client {client_name}: {reason}"
            )

            return {
                "success": False,
                "result": {},
                "error": reason,
            }

        payload = reply.payload or {}

        return {
            "success": (
                reply.status == MessageStatus.OK
                and not payload.get("error")
            ),
            "result": payload.get("result", {}),
            "error": payload.get("error"),
        }


    def stop_sample(
        self,
        client_id: bytes,
        section: Channel,
        timeout_s: float = 10.0,
    ) -> dict:

        if section not in {
            Channel.RC,
            Channel.HV,
            Channel.MAIN,
        }:
            return {
                "success": False,
                "error": f"Unsupported sample section: {section}",
            }

        reply, reason = (
            self.command_service.send_monitoring_command(
                client_id=client_id,
                command="sample_stop",
                payload={
                    "section": section.value,
                },
                timeout_s=timeout_s,
            )
        )

        if reply is None:
            client_name = client_id.decode(errors="ignore")

            self.logger.error(
                f"Failed to stop {section.value} samples "
                f"for client {client_name}: {reason}"
            )

            return {
                "success": False,
                "result": {},
                "error": reason,
            }

        payload = reply.payload or {}

        return {
            "success": (
                reply.status == MessageStatus.OK
                and not payload.get("error")
            ),
            "result": payload.get("result", {}),
            "error": payload.get("error"),
        }
        

        
    def synchronize_client(self, client_id: bytes, *, probe_count: int = 5, probe_timeout_s: float = 2.0,) -> Optional[ClientTimeSyncState]:
            
        measurements = []
        
        was_synchronized = self.time_sync_service.is_synchronized(client_id)
        
        for _ in range(probe_count):
            probe_request_id = self.monitoring_manager.queue_time_sync_probe(client_id=client_id)
            
            measurement, reason = (
                self.monitoring_manager.wait_for_time_sync_measurement(
                    client_id=client_id,
                    request_id=probe_request_id,
                    timeout_s=probe_timeout_s,
                )
            )
            
            if measurement is None:
                self.logger.warning(
                    "Time-sync probe failed: "
                    f"client={client_id!r}, "
                    f"request_id={probe_request_id}, "
                    f"reason={reason}"
                )
                continue
            
            self.logger.info(
                "Time-sync probe completed: "
                f"client={client_id!r}, "
                f"request_id={measurement.request_id}, "
                f"rtt={measurement.network_rtt_ns / 1e6:.3f} ms, "
                f"offset={measurement.offset_ns / 1e9:.6f} s"
            )
                
            measurements.append(measurement)
        
        if not measurements:
            self.logger.error(
                f"Time synchronization failed for {client_id!r}"
            )
            return None
        
        state = self.time_sync_service.apply_measurements(
            client_id=client_id,
            measurements=measurements,
        )
        
        if state is None:
            return None
        
        self._schedule_next_time_sync(client_id=client_id, synced_at_ns=state.synced_at_server_monotonic_ns, initial_sync = not was_synchronized)
        
        return state
    
    def ensure_client_synchronized(self, client_id: bytes) -> bool:
        if self.time_sync_service.is_synchronized(client_id):
            return True
        
        state = self.synchronize_client(client_id)
        
        return state is not None
    
    def _next_time_sync_slot_ns(self, *, client_id: bytes, not_before_ns: int) -> int:
        
        period_ns = int(TIME_SYNC_PERIOD_S * 1_000_000_000)
        
        phase_ns = self._time_sync_phase(client_id, period_ns)
        
        if not_before_ns <= phase_ns:
            return phase_ns
        
        cycles = (not_before_ns - phase_ns + period_ns - 1) // period_ns
        
        return cycles * period_ns + phase_ns
    
    def _schedule_next_time_sync(
        self,
        *,
        client_id: bytes,
        synced_at_ns: int,
        initial_sync: bool,
    ) -> None:

        period_ns = int(
            TIME_SYNC_PERIOD_S * 1_000_000_000
        )

        if initial_sync:

            # First synchronization:
            # wait at least one full period before
            # entering the deterministic schedule.
            not_before_ns = (
                synced_at_ns
                + period_ns
            )

        else:

            # Already synchronized:
            # return to the first deterministic slot
            # strictly after the completed synchronization.
            not_before_ns = synced_at_ns + 1

        next_resync_ns = (
            self._next_time_sync_slot_ns(
                client_id=client_id,
                not_before_ns=not_before_ns,
            )
        )

        with self._time_sync_schedule_lock:
            self._next_resync_ns[
                client_id
            ] = next_resync_ns

        self._scheduler_wakeup_event.set()

    def _schedule_time_sync_retry(self, client_id: bytes) -> None:
        retry_ns = int(TIME_SYNC_RETRY_S * 1_000_000_000)

        with self._time_sync_schedule_lock:
            self._next_resync_ns[client_id] = time.monotonic_ns() + retry_ns

        self._scheduler_wakeup_event.set()

    def _handle_time_sync_failure(
        self,
        client_id: bytes,
    ) -> None:

        now_ns = time.monotonic_ns()

        current_state = (
            self.time_sync_service.get_state(
                client_id
            )
        )

        if current_state is not None:

            age_ns = (
                now_ns
                - current_state.synced_at_server_monotonic_ns
            )

            max_age_ns = int(
                TIME_SYNC_MAX_AGE_S
                * 1_000_000_000
            )

            if age_ns >= max_age_ns:
                self.time_sync_service.invalidate_client(
                    client_id,
                    reason="time synchronization expired",
                )

        self._schedule_time_sync_retry(
            client_id
        )

    def _time_sync_worker(self, client_id: bytes) -> None:
        try:
            self.logger.debug(
                f"Starting scheduled time sync "
                f"for client={client_id!r}"
            )

            state = self.synchronize_client(client_id=client_id)

            if state is None:
                self._handle_time_sync_failure(
                    client_id
                )

                self.logger.warning(
                    f"Scheduled time sync failed "
                    f"for client={client_id!r}; "
                    f"retry scheduled"
                )
                return

            self.logger.debug(
                f"Scheduled time sync completed "
                f"for client={client_id!r}"
            )
        except Exception as e:
            self.logger.exception(
                f"Unexpected scheduled time-sync error "
                f"for client={client_id!r}: {e}"
            )

            self._handle_time_sync_failure(
                client_id
            )

        finally:
            with self._time_sync_schedule_lock:
                self._resync_in_progress.discard(client_id)
            self._scheduler_wakeup_event.set()
    
    def _time_sync_scheduler_loop(self) -> None:

        while not self._scheduler_stop_event.is_set():

            now_ns = time.monotonic_ns()
            due_clients = []

            with self._time_sync_schedule_lock:

                available_slots = (
                    SYNC_WORKERS
                    - len(self._resync_in_progress)
                )

                for client_id, next_resync_ns in (
                    self._next_resync_ns.items()
                ):

                    if now_ns < next_resync_ns:
                        continue

                    if client_id in self._resync_in_progress:
                        continue

                    due_clients.append(
                        (next_resync_ns, client_id)
                    )

                due_clients.sort()
                due_clients = due_clients[:available_slots]

                for _, client_id in due_clients:
                    self._resync_in_progress.add(client_id)

            for _, client_id in due_clients:

                self.logger.debug(
                    "Time-sync resync due: "
                    f"client={client_id!r}"
                )

                if not self.monitoring_manager.is_client_connected(client_id):
                    with self._time_sync_schedule_lock:
                        self._resync_in_progress.discard(client_id)
                    self._schedule_time_sync_retry(client_id)
                    continue
                try:
                    self._sync_executor.submit(
                        self._time_sync_worker,
                        client_id,
                    )

                except Exception as exc:

                    with self._time_sync_schedule_lock:
                        self._resync_in_progress.discard(
                            client_id
                        )

                    self.logger.exception(
                        "Failed to submit time-sync worker: "
                        f"client={client_id!r}: {exc}"
                    )

                    self._schedule_time_sync_retry(
                        client_id
                    )

                    continue

            self._scheduler_wakeup_event.wait(
                timeout=1.0
            )
            self._scheduler_wakeup_event.clear()
    
    
    def start_time_sync_scheduler(self) -> None:

        if (
            self._scheduler_thread is not None
            and self._scheduler_thread.is_alive()
        ):
            return

        self._scheduler_stop_event.clear()

        self._sync_executor = ThreadPoolExecutor(max_workers=SYNC_WORKERS, thread_name_prefix="time-sync-worker")

        self._scheduler_thread = threading.Thread(
            target=self._time_sync_scheduler_loop,
            daemon=True,
            name="time-sync-scheduler",
        )

        self._scheduler_thread.start()

        self.logger.info(
            "Time-sync scheduler started"
        )
        
    
    def stop_time_sync_scheduler(self) -> None:

        self._scheduler_stop_event.set()
        self._scheduler_wakeup_event.set()

        if (
            self._scheduler_thread is not None
            and self._scheduler_thread.is_alive()
        ):
            self._scheduler_thread.join(
                timeout=2.0
            )

        self._scheduler_thread = None

        if self._sync_executor is not None:
            self._sync_executor.shutdown(wait=True)
            self._sync_executor = None

        self.logger.info(
            "Time-sync scheduler stopped"
        )