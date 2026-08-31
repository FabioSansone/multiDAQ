from server.utils.logger import get_logger
from common.message_handler import Channel, ProtocolMessage
from server.services.monitor_stream_service import StreamSubscription

from dataclasses import dataclass
from pathlib import Path
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import copy
import time
import csv
import json
from datetime import datetime, timezone

PERSISTENCE_QUEUE_MAXSIZE = 10000
PERSISTENCE_WORKERS = 4

PERSISTENCE_BATCH_SAMPLES = 100
PERSISTENCE_FLUSH_INTERVAL_S = 5.0
PERSISTENCE_FLUSH_CHECK_PERIOD_S = 1.0

PERSISTENCE_SECTION_FILENAMES = {
    Channel.MAIN: "main",
    Channel.RC: "rc",
    Channel.HV: "hv",
}


RC_COLUMNS = (
    "timestamp_utc_ns",
    "register",
    "value",
)

HV_COLUMNS = (
    "timestamp_utc_ns",
    "channel",
    "voltage",
    "current",
    "temperature",
    "channel_state",
    "power_state",
)

MAIN_COLUMNS = (
    "timestamp_utc_ns",
    "temperature_c",
    "humidity_pct",
    "pressure_hpa",
    "voltage_5v_v",
    "voltage_3v3_v",
    "current_a",
    "fpga_temperature_c"
)

MONITORING_METADATA_SCHEMA_VERSION = 1


EVENT_COLUMNS = (
    "timestamp_utc_ns",
    "channel",
    "event",
    "severity",
    "sender",
    "source_request_id",
    "details_json",
    "error",
)

@dataclass
class PersistenceStreamState:
    client_id: bytes
    section: Channel
    
    enabled: bool
    save_format: str
    
    requested_interval_ns: int
    
    samples_enqueued: int = 0
    samples_written: int = 0
    samples_dropped: int = 0
    
    queue_pending: int = 0
    
    rows_written: int = 0
    
    last_error: str | None = None
    

@dataclass
class PersistenceEventState:
    client_id: bytes

    enabled: bool
    save_format: str

    events_enqueued: int = 0
    events_written: int = 0
    events_dropped: int = 0

    queue_pending: int = 0

    rows_written: int = 0

    last_error: str | None = None
    
    
    
@dataclass
class PersistenceSession:
    session_id: str
    started_at_utc_ns: int
    root_folder: Path
    
@dataclass(frozen=True)
class PersistenceItem:
    client_id: bytes
    section: Channel
    message: ProtocolMessage
    

@dataclass(frozen=True)
class PersistenceEventItem:
    client_id: bytes
    message: ProtocolMessage
    
    
@dataclass(frozen=True)
class NormalizedPersistenceSample:
    client_id: bytes
    section: Channel
    rows: tuple[dict, ...]


@dataclass
class PersistenceBuffer:
    samples: list[NormalizedPersistenceSample]
    first_sample_monotonic_ns: int | None = None
    
    
    

class MonitorPersistenceService:
    
    def __init__(self):
        
        self._persistence_streams: dict[tuple[bytes, Channel], PersistenceStreamState] = {}
        self._persistence_lock = threading.Lock()
        self._stream_idle_condition = threading.Condition(self._persistence_lock)
        
        self.persistence_queue = queue.Queue(maxsize=PERSISTENCE_QUEUE_MAXSIZE)
        self._executor: Optional[ThreadPoolExecutor] = None
        self._stop_event = threading.Event()
        self._running = False

        self._session: PersistenceSession | None = None

        self.writer = MonitorPersistenceWriter(self)
        
        self._flush_schedule_lock = threading.Lock()
        self._next_flush_check_ns = 0
        
        self._client_metadata: dict[bytes, dict] = {}
        self._metadata_transaction_locks: dict[bytes,threading.Lock] = {}
        self._metadata_transaction_locks_lock = threading.Lock()
        
        self._event_states: dict[bytes, PersistenceEventState] = {}
        
        self.logger = get_logger("monitor_persistence_service")
        self.logger.debug("Monitoring Persistence Service initialized")
    
    def is_running(self) -> bool:

        with self._persistence_lock:
            return self._running           

    def _prepare_stream(self, client_id: bytes, section: Channel, save_format: str, requested_interval_ns:int) -> bool:
        
        if requested_interval_ns <= 0:
            self.logger.error(
                f"Cannot prepare persistence stream: "
                f"invalid requested interval "
                f"{requested_interval_ns} ns"
            )
            return False
        
        if save_format not in {"csv","parquet"}:
            self.logger.error(
                f"Cannot prepare persistence stream: "
                f"unsupported format={save_format!r}"
            )
            return False
    
        key = (client_id, section)
        
        with self._persistence_lock:
            exisisting_persistence_state = self._persistence_streams.get(key)
            
            if exisisting_persistence_state is not None:
                if exisisting_persistence_state.enabled:
                    self.logger.warning(
                        f"Persistence stream already active: "
                        f"client={client_id!r}, "
                        f"section={section.value}"
                    )
                    return False
                exisisting_persistence_state.save_format = save_format
                exisisting_persistence_state.requested_interval_ns = requested_interval_ns
                exisisting_persistence_state.last_error = None
                
                self.logger.debug(
                    f"Persistence stream prepared again: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"format={save_format}, "
                    f"interval="
                    f"{requested_interval_ns / 1e9:.3f}s"
                )
                
                return True

            new_persistence_state = PersistenceStreamState(
                client_id=client_id,
                section=section,
                enabled=False,
                save_format=save_format,
                requested_interval_ns=requested_interval_ns
            )
            self._persistence_streams[key] = new_persistence_state
        
        self.logger.info(
            f"Persistence stream prepared: "
            f"client={client_id!r}, "
            f"section={section.value}, "
            f"format={save_format}, "
            f"interval="
            f"{requested_interval_ns / 1e9:.3f}s"
        )

        return True
        
    def enqueue_sample(self, client_id: bytes, message: ProtocolMessage, subscription: StreamSubscription) -> bool:
        section = message.channel
        key = (client_id, section)

        persistence_item = PersistenceItem(
            client_id=client_id,
            section=section,
            message=message,
        )

        with self._persistence_lock:

            if not self._running:
                return False

            state = self._persistence_streams.get(key)

            if state is None:
                self.logger.error(
                    f"Persistence stream not configured: "
                    f"client={client_id!r}, "
                    f"section={section.value}"
                )
                return False

            if not state.enabled:
                return False

            try:
                self.persistence_queue.put_nowait(
                    persistence_item
                )

            except queue.Full:
                state.samples_dropped += 1
                state.last_error = "persistence queue full"
                return False

            state.samples_enqueued += 1
            state.queue_pending += 1

        return True 
    
    
    def activate_stream(self, client_id: bytes, section: Channel) -> bool:
        
        key = (client_id, section)
        
        with self._persistence_lock:
            existing_persistence_state = self._persistence_streams.get(key)
            if existing_persistence_state is None:
                self.logger.error(
                    f"Cannot activate persistence stream: "
                    f"stream not prepared for "
                    f"client={client_id!r}, "
                    f"section={section.value}"
                )
                return False
            if existing_persistence_state.enabled:
                self.logger.debug(
                    f"Persistence stream already active: "
                    f"client={client_id!r}, "
                    f"section={section.value}"
                )
                return True
            existing_persistence_state.enabled = True
            existing_persistence_state.last_error = None
        
        self.logger.info(
            f"Persistence stream activated: "
            f"client={client_id!r}, "
            f"section={section.value}"
        )
        
        return True
            
        
    def deactivate_stream(
        self,
        client_id: bytes,
        section: Channel,
    ) -> bool:

        key = (
            client_id,
            section,
        )

        with self._persistence_lock:

            existing_persistence_state = (
                self._persistence_streams.get(
                    key
                )
            )

            if existing_persistence_state is None:
                self.logger.error(
                    f"Cannot deactivate persistence stream: "
                    f"stream not prepared for "
                    f"client={client_id!r}, "
                    f"section={section.value}"
                )
                return False

            if not existing_persistence_state.enabled:
                self.logger.debug(
                    f"Persistence stream already inactive: "
                    f"client={client_id!r}, "
                    f"section={section.value}"
                )
                return True

            existing_persistence_state.enabled = False

        self.logger.info(
            f"Persistence stream deactivated: "
            f"client={client_id!r}, "
            f"section={section.value}"
        )

        return True
    
    def get_stream(
        self,
        client_id: bytes,
        section: Channel,
    ) -> PersistenceStreamState | None:

        key = (client_id, section,)

        with self._persistence_lock:

            state = self._persistence_streams.get(
                key
            )

            if state is None:
                return None

            return copy.deepcopy(state)
    
    
    def get_dataset_schemas(self) -> dict:

        return {
            "main": list(MAIN_COLUMNS),
            "rc": list(RC_COLUMNS),
            "hv": list(HV_COLUMNS),
            "events": list(EVENT_COLUMNS),
            "mon_metadata_version": MONITORING_METADATA_SCHEMA_VERSION
        }
    
    
    def list_streams(self) -> list[PersistenceStreamState]:

        with self._persistence_lock:
            return copy.deepcopy(
                list(
                    self._persistence_streams.values()
                )
            )

    def start_session(self, session_id: str, root_folder: Path, started_at_utc_ns: int) -> bool:

        with self._persistence_lock:

            if self._session is not None:
                return True

            self._session = PersistenceSession(
                session_id=session_id,
                started_at_utc_ns=started_at_utc_ns,
                root_folder=root_folder
            )


        try:
            root_folder.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            with self._persistence_lock:
                self._session = None

                self.logger.error(
                    f"Cannot create persistence session folder "
                    f"{root_folder}: {e}"
                )
                return False

        self.logger.info(
            f"Persistence session started: "
            f"id={session_id}, "
            f"folder={root_folder}"
        )

        return True
    
    def _get_metadata_transaction_lock(
        self,
        client_id: bytes,
    ) -> threading.Lock:

        with self._metadata_transaction_locks_lock:

            lock = (
                self._metadata_transaction_locks.get(
                    client_id
                )
            )

            if lock is None:

                lock = threading.Lock()

                self._metadata_transaction_locks[
                    client_id
                ] = lock

            return lock

    def get_session(self,) -> PersistenceSession | None:
        with self._persistence_lock:
            if self._session is None:
                return None
            return copy.deepcopy(self._session)
        
    def _mark_samples_written(
        self,
        client_id: bytes,
        section: Channel,
        count: int = 1,
    ) -> None:

        if count <= 0:
            return

        key = (
            client_id,
            section,
        )

        with self._persistence_lock:

            state = self._persistence_streams.get(
                key
            )

            if state is None:
                return

            state.samples_written += count
            state.last_error = None
    
    def _mark_samples_dropped(
        self,
        client_id: bytes,
        section: Channel,
        count: int = 1,
        reason: str | None = None,
    ) -> None:

        if count <= 0:
            return

        key = (
            client_id,
            section,
        )

        with self._persistence_lock:

            state = self._persistence_streams.get(
                key
            )

            if state is None:
                return

            state.samples_dropped += count

            if reason is not None:
                state.last_error = reason
    
    
    def _mark_enqueued(
        self,
        client_id: bytes,
        section: Channel,
    ) -> None:

        key = (
            client_id,
            section,
        )

        with self._persistence_lock:

            state = self._persistence_streams.get(
                key
            )

            if state is not None:
                state.samples_enqueued += 1    
                
    
    def _mark_rows_written(
        self,
        client_id: bytes,
        section: Channel,
        count: int,
    ) -> None:

        if count <= 0:
            return

        key = (
            client_id,
            section,
        )

        with self._persistence_lock:

            state = self._persistence_streams.get(
                key
            )

            if state is None:
                return

            state.rows_written += count    
    
    
    def _mark_item_processed(
        self,
        client_id: bytes,
        section: Channel,
    ) -> None:

        key = (
            client_id,
            section,
        )

        with self._stream_idle_condition:

            state = self._persistence_streams.get(
                key
            )

            if state is None:
                return

            if state.queue_pending <= 0:

                self.logger.error(
                    f"Invalid persistence pending counter: "
                    f"client={client_id!r}, "
                    f"section={section.value}, "
                    f"queue_pending={state.queue_pending}"
                )

                state.queue_pending = 0

            else:
                state.queue_pending -= 1

            if state.queue_pending == 0:
                self._stream_idle_condition.notify_all()       
     
     
    def _mark_events_written(
        self,
        client_id: bytes,
        count: int = 1,
    ) -> None:

        with self._persistence_lock:

            state = self._event_states.get(
                client_id
            )

            if state is None:
                return

            state.events_written += count
            state.rows_written += count
            state.last_error = None


    def _mark_events_dropped(
        self,
        client_id: bytes,
        count: int = 1,
        reason: str | None = None,
    ) -> None:

        with self._persistence_lock:

            state = self._event_states.get(
                client_id
            )

            if state is None:
                return

            state.events_dropped += count

            if reason is not None:
                state.last_error = reason


    def _mark_event_processed(
        self,
        client_id: bytes,
    ) -> None:

        with self._stream_idle_condition:

            state = self._event_states.get(
                client_id
            )

            if state is None:
                return

            if state.queue_pending > 0:
                state.queue_pending -= 1

            else:
                state.queue_pending = 0

            if state.queue_pending == 0:
                self._stream_idle_condition.notify_all()
        
     
     
                    
    def _set_stream_error(
        self,
        client_id: bytes,
        section: Channel,
        error: str,
    ) -> None:

        key = (
            client_id,
            section,
        )

        with self._persistence_lock:

            state = self._persistence_streams.get(
                key
            )

            if state is None:
                return

            state.last_error = error
    
    
    def get_queue_status(
        self,
    ) -> dict:

        return {
            "size": self.persistence_queue.qsize(),
            "maxsize": self.persistence_queue.maxsize,
            "full": self.persistence_queue.full(),
            "empty": self.persistence_queue.empty(),
        }
        
    
    def get_status(self) -> dict:

        with self._persistence_lock:

            running = self._running

            streams = copy.deepcopy(
                list(
                    self._persistence_streams.values()
                )
            )

            events = copy.deepcopy(
                list(
                    self._event_states.values()
                )
            )

        return {
            "running": running,
            "queue": self.get_queue_status(),
            "streams": streams,
            "events": events,
        }
        
    def has_active_streams(
        self,
    ) -> bool:

        with self._persistence_lock:

            sample_active = any(
                state.enabled
                for state
                in self._persistence_streams.values()
            )

            event_active = any(
                state.enabled
                for state
                in self._event_states.values()
            )

            return (
                sample_active
                or event_active
            )
    
    
    def list_client_streams(
        self,
        client_id: bytes,
    ) -> list[PersistenceStreamState]:

        with self._persistence_lock:

            states = [
                state
                for state
                in self._persistence_streams.values()
                if state.client_id == client_id
            ]

            return copy.deepcopy(states)


    def _maybe_flush_due(self) -> None:

        now_ns = time.monotonic_ns()
        check_period_ns = int(
            PERSISTENCE_FLUSH_CHECK_PERIOD_S
            * 1_000_000_000
        )

        with self._flush_schedule_lock:

            if now_ns < self._next_flush_check_ns:
                return

            self._next_flush_check_ns = (
                now_ns + check_period_ns
            )

        try:
            flush_ok = self.writer.flush_due()

            if not flush_ok:
                self.logger.warning(
                    "One or more persistence buffers "
                    "failed during periodic flush"
                )

        except Exception as exc:
            self.logger.exception(
                f"Unexpected error during persistence "
                f"periodic flush: {exc}"
            )
    
     
    def wait_stream_idle(
        self,
        client_id: bytes,
        section: Channel,
        timeout_s: float | None = 30.0,
    ) -> bool:

        key = (
            client_id,
            section,
        )

        if timeout_s is not None and timeout_s < 0:
            raise ValueError(
                "timeout_s must be >= 0 or None"
            )

        deadline = (
            None
            if timeout_s is None
            else time.monotonic() + timeout_s
        )

        with self._stream_idle_condition:

            while True:

                state = self._persistence_streams.get(
                    key
                )

                if state is None:
                    self.logger.error(
                        f"Cannot wait for persistence stream: "
                        f"stream not configured for "
                        f"client={client_id!r}, "
                        f"section={section.value}"
                    )
                    return False

                if state.queue_pending == 0:
                    return True

                if deadline is None:

                    self._stream_idle_condition.wait()
                    continue

                remaining_s = (
                    deadline - time.monotonic()
                )

                if remaining_s <= 0:

                    self.logger.error(
                        f"Timeout waiting for persistence stream "
                        f"to become idle: "
                        f"client={client_id!r}, "
                        f"section={section.value}, "
                        f"pending={state.queue_pending}"
                    )

                    return False

                self._stream_idle_condition.wait(
                    timeout=remaining_s
                )
                
    def wait_events_idle(
        self,
        client_id: bytes,
        timeout_s: float = 30.0,
    ) -> bool:

        deadline = (
            time.monotonic()
            + timeout_s
        )

        with self._stream_idle_condition:

            while True:

                state = self._event_states.get(
                    client_id
                )

                if state is None:
                    return False

                if state.queue_pending == 0:
                    return True

                remaining = (
                    deadline
                    - time.monotonic()
                )

                if remaining <= 0:
                    return False

                self._stream_idle_condition.wait(
                    remaining
                )
            
    def flush_stream(
        self,
        client_id: bytes,
        section: Channel,
    ) -> bool:

        try:
            return self.writer.flush_stream(
                client_id=client_id,
                section=section,
            )

        except Exception as exc:

            self.logger.exception(
                f"Failed to flush persistence stream: "
                f"client={client_id!r}, "
                f"section={section.value}: "
                f"{exc}"
            )

            self._set_stream_error(
                client_id=client_id,
                section=section,
                error="stream flush failed",
            )

            return False    
        
    def flush_events(
        self,
        client_id: bytes,
    ) -> bool:

        try:

            return self.writer.flush_events(
                client_id=client_id
            )

        except Exception as exc:

            self.logger.exception(
                f"Failed to flush event persistence: "
                f"client={client_id!r}: "
                f"{exc}"
            )

            with self._persistence_lock:

                state = self._event_states.get(
                    client_id
                )

                if state is not None:
                    state.last_error = (
                        "event flush failed"
                    )

            return False  
            
    def _worker_loop(self) -> None:

        while not self._stop_event.is_set():

            try:
                persistence_item = (
                    self.persistence_queue.get(
                        timeout=1.0
                    )
                )

            except queue.Empty:

                self._maybe_flush_due()
                continue

            is_event = isinstance(
                persistence_item,
                PersistenceEventItem,
            )

            try:

                if is_event:

                    write_ok = (
                        self.writer
                        .process_event_item(
                            persistence_item
                        )
                    )

                else:

                    write_ok = (
                        self.writer
                        .process_item(
                            persistence_item
                        )
                    )

                if not write_ok:

                    if is_event:

                        self.logger.warning(
                            "Persistence EVENT processing "
                            "failed: "
                            f"client="
                            f"{persistence_item.client_id!r}"
                        )

                    else:

                        self.logger.warning(
                            "Persistence sample processing "
                            "failed: "
                            f"client="
                            f"{persistence_item.client_id!r}, "
                            f"section="
                            f"{persistence_item.section.value}"
                        )

            except Exception as exc:

                if is_event:

                    self.logger.exception(
                        "Unexpected persistence worker "
                        "error while processing EVENT: "
                        f"client="
                        f"{persistence_item.client_id!r}, "
                        f"error={exc}"
                    )

                    self._mark_events_dropped(
                        client_id=(
                            persistence_item.client_id
                        ),
                        count=1,
                        reason=(
                            "unexpected persistence "
                            "worker error"
                        ),
                    )

                else:

                    self.logger.exception(
                        "Unexpected persistence worker "
                        "error while processing sample: "
                        f"client="
                        f"{persistence_item.client_id!r}, "
                        f"section="
                        f"{persistence_item.section.value}, "
                        f"error={exc}"
                    )

                    self._mark_samples_dropped(
                        client_id=(
                            persistence_item.client_id
                        ),
                        section=(
                            persistence_item.section
                        ),
                        count=1,
                        reason=(
                            "unexpected persistence "
                            "worker error"
                        ),
                    )

            finally:

                if is_event:

                    self._mark_event_processed(
                        client_id=(
                            persistence_item.client_id
                        )
                    )

                else:

                    self._mark_item_processed(
                        client_id=(
                            persistence_item.client_id
                        ),
                        section=(
                            persistence_item.section
                        ),
                    )

                self.persistence_queue.task_done()

            self._maybe_flush_due()


    
    def start(self) -> bool:
        
        with self._flush_schedule_lock:
            self._next_flush_check_ns = 0
        
        with self._persistence_lock:
            
            if self._executor is not None:
                return True
        
            self._stop_event.clear()
        
            self._executor = ThreadPoolExecutor(max_workers=PERSISTENCE_WORKERS, thread_name_prefix="persistence-thread")

            executor = self._executor
            
            self._running = True
        
        for _ in range(PERSISTENCE_WORKERS):
            executor.submit(self._worker_loop)
            
        self.logger.info(
            f"Monitoring persistence executor started "
            f"with {PERSISTENCE_WORKERS} workers"
        )
        
        return True
    
    def stop(self,) -> bool:
        
        with self._persistence_lock:
            
            if self._executor is None:
                self._running = False
                return True
            
            self._running = False
            
            executor = self._executor
        
        self.logger.info("Stopping monitoring persistence executor")

        self.persistence_queue.join()
        
        flush_ok = True
        try:
            flush_ok = self.writer.flush_all()
            
            if not flush_ok:
               self.logger.warning(
                    "One or more persistence buffers "
                    "failed during final flush"
                )
        
        except Exception as e:
            flush_ok = False
            self.logger.exception(
                f"Unexpected error during final "
                f"persistence flush: {e}"
            )
        
        self._stop_event.set()
        
        try:
            executor.shutdown(wait=True, cancel_futures=False)
        
        except Exception as e:
            self.logger.exception(
                f"Error while shutting down persistence "
                f"executor: {e}"
            )
            
            with self._persistence_lock:
                self._executor = None
                
            return False
        
        with self._persistence_lock:
            self._executor = None
        
        self.logger.info("Monitoring persistence executor stopped")
    
    
        return flush_ok
    
    
    def ensure_session(self) -> bool:

        with self._persistence_lock:
            if self._session is not None:
                return True

        started_at_utc_ns = time.time_ns()

        dt = datetime.fromtimestamp(
            started_at_utc_ns / 1_000_000_000,
            tz=timezone.utc,
        )

        date_folder = dt.strftime("%Y_%m_%d")

        session_id = dt.strftime("%H-%M-%S" + f"-{started_at_utc_ns // 1_000_000 % 1000:03d}")

        base_path = (
            Path("/swgo")
            if Path("/swgo").exists()
            else Path.home()
        )
        
        root_folder = (
            base_path
            / "multiPMT"
            / "monitoring"
            / date_folder
            / f"monitoring_{session_id}"
        )

        return self.start_session(
            session_id=session_id,
            root_folder=root_folder,
            started_at_utc_ns=started_at_utc_ns,
        )
        
    
    def get_client_metadata(
        self,
        client_id: bytes,
    ) -> dict | None:

        with self._persistence_lock:

            metadata = self._client_metadata.get(
                client_id
            )

            if metadata is None:
                return None

            return copy.deepcopy(metadata)
    
    
    def initialize_client_metadata(
        self,
        client_id: bytes,
        metadata: dict,
    ) -> bool:

        transaction_lock = (
            self._get_metadata_transaction_lock(
                client_id
            )
        )

        with transaction_lock:

            with self._persistence_lock:

                existing = (
                    self._client_metadata.get(
                        client_id
                    )
                )

                if existing is not None:
                    return True

                metadata_snapshot = copy.deepcopy(
                    metadata
                )

            #
            # Write first. RAM state becomes authoritative
            # only if disk commit succeeds.
            #
            write_ok = self.writer.write_metadata(
                client_id=client_id,
                metadata=metadata_snapshot,
            )

            if not write_ok:
                return False

            with self._persistence_lock:

                #
                # Because the transaction lock is held,
                # no other metadata mutation for this
                # client can have happened in between.
                #
                self._client_metadata[
                    client_id
                ] = metadata_snapshot

            return True
    
    
    def update_stream_metadata(
        self,
        client_id: bytes,
        section: Channel,
        *,
        enabled: bool,
        save_format: str,
        requested_interval_ns: int,
    ) -> bool:

        transaction_lock = (
            self._get_metadata_transaction_lock(
                client_id
            )
        )

        with transaction_lock:

            # ============================================================
            # Read current committed metadata
            # ============================================================

            with self._persistence_lock:

                current_metadata = (
                    self._client_metadata.get(
                        client_id
                    )
                )

                if current_metadata is None:

                    self.logger.error(
                        "Cannot update persistence metadata: "
                        "metadata not initialized for "
                        f"client={client_id!r}"
                    )

                    return False

                updated_metadata = copy.deepcopy(
                    current_metadata
                )

            # ============================================================
            # Mutate private working copy
            # ============================================================

            persistence = (
                updated_metadata.setdefault(
                    "persistence",
                    {},
                )
            )

            sections = persistence.setdefault(
                "sections",
                {},
            )

            sections[section.value] = {
                "enabled": enabled,
                "format": save_format,
                "requested_interval_ns": (
                    requested_interval_ns
                ),
                "updated_at_utc_ns": (
                    time.time_ns()
                ),
            }

            # ============================================================
            # Persist
            # ============================================================

            write_ok = self.writer.write_metadata(
                client_id=client_id,
                metadata=updated_metadata,
            )

            if not write_ok:
                return False

            # ============================================================
            # Commit RAM state
            # ============================================================

            with self._persistence_lock:

                self._client_metadata[
                    client_id
                ] = updated_metadata

            return True
    
    def update_event_metadata(
        self,
        client_id: bytes,
        *,
        enabled: bool,
        save_format: str,
    ) -> bool:

        transaction_lock = (
            self._get_metadata_transaction_lock(
                client_id
            )
        )

        with transaction_lock:

            # ============================================================
            # Read
            # ============================================================

            with self._persistence_lock:

                current_metadata = (
                    self._client_metadata.get(
                        client_id
                    )
                )

                if current_metadata is None:

                    self.logger.error(
                        "Cannot update EVENT persistence metadata: "
                        "metadata not initialized for "
                        f"client={client_id!r}"
                    )

                    return False

                updated_metadata = copy.deepcopy(
                    current_metadata
                )

            # ============================================================
            # Modify
            # ============================================================

            sections = (
                updated_metadata
                .setdefault(
                    "persistence",
                    {},
                )
                .setdefault(
                    "sections",
                    {},
                )
            )

            sections["events"] = {
                "enabled": enabled,
                "format": save_format,
                "updated_at_utc_ns": (
                    time.time_ns()
                ),
            }

            # ============================================================
            # Persist
            # ============================================================

            write_ok = self.writer.write_metadata(
                client_id=client_id,
                metadata=updated_metadata,
            )

            if not write_ok:
                return False

            # ============================================================
            # Commit
            # ============================================================

            with self._persistence_lock:

                self._client_metadata[
                    client_id
                ] = updated_metadata

            return True
    
    def record_configuration_change(
        self,
        client_id: bytes,
        register: int,
        new_value,
        *,
        timestamp_utc_ns: int | None = None,
    ) -> bool:

        if register not in {
            31,
            39,
        }:
            return True

        if timestamp_utc_ns is None:
            timestamp_utc_ns = time.time_ns()

        transaction_lock = (
            self._get_metadata_transaction_lock(
                client_id
            )
        )

        with transaction_lock:

            # ============================================================
            # Read current committed metadata
            # ============================================================

            with self._persistence_lock:

                current_metadata = (
                    self._client_metadata.get(
                        client_id
                    )
                )

                #
                # No active persistence metadata for this
                # client: configuration event is still valid,
                # but there is nothing to update.
                #
                if current_metadata is None:
                    return True

                updated_metadata = copy.deepcopy(
                    current_metadata
                )

            # ============================================================
            # Modify working copy
            # ============================================================

            configuration = (
                updated_metadata.setdefault(
                    "configuration",
                    {},
                )
            )

            current = configuration.setdefault(
                "current",
                {},
            )

            register_key = str(
                register
            )

            old_value = current.get(
                register_key
            )

            if old_value == new_value:
                return True

            current[
                register_key
            ] = new_value

            history = (
                updated_metadata.setdefault(
                    "configuration_history",
                    [],
                )
            )

            history.append({
                "timestamp_utc_ns": (
                    timestamp_utc_ns
                ),
                "register": register,
                "old_value": old_value,
                "new_value": new_value,
            })

            # ============================================================
            # Persist
            # ============================================================

            write_ok = self.writer.write_metadata(
                client_id=client_id,
                metadata=updated_metadata,
            )

            if not write_ok:

                self.logger.error(
                    "Cannot record persistence "
                    "configuration change: "
                    "metadata write failed for "
                    f"client={client_id!r}, "
                    f"register={register}"
                )

                return False

            # ============================================================
            # Commit RAM state
            # ============================================================

            with self._persistence_lock:

                self._client_metadata[
                    client_id
                ] = updated_metadata

            self.logger.info(
                "Persistence configuration change recorded: "
                f"client={client_id!r}, "
                f"register={register}, "
                f"old_value={old_value}, "
                f"new_value={new_value}"
            )

            return True



    def prepare_events(
        self,
        client_id: bytes,
        save_format: str,
    ) -> bool:

        if save_format not in {
            "csv",
            "parquet",
        }:

            self.logger.error(
                "Cannot prepare event persistence: "
                f"unsupported format={save_format!r}"
            )

            return False

        with self._persistence_lock:

            state = self._event_states.get(
                client_id
            )

            if state is not None:

                if state.enabled:

                    self.logger.warning(
                        "Event persistence already active: "
                        f"client={client_id!r}"
                    )

                    return False

                state.save_format = save_format
                state.last_error = None

                return True

            self._event_states[
                client_id
            ] = PersistenceEventState(
                client_id=client_id,
                enabled=False,
                save_format=save_format,
            )

        self.logger.info(
            "Event persistence prepared: "
            f"client={client_id!r}, "
            f"format={save_format}"
        )

        return True

    
    def activate_events(
        self,
        client_id: bytes,
    ) -> bool:

        with self._persistence_lock:

            state = self._event_states.get(
                client_id
            )

            if state is None:
                return False

            state.enabled = True
            state.last_error = None

        self.logger.info(
            "Event persistence activated: "
            f"client={client_id!r}"
        )

        return True


    def deactivate_events(
        self,
        client_id: bytes,
    ) -> bool:

        with self._persistence_lock:

            state = self._event_states.get(
                client_id
            )

            if state is None:
                return False

            state.enabled = False

        self.logger.info(
            "Event persistence deactivated: "
            f"client={client_id!r}"
        )

        return True 
    
    
    
    def enqueue_event(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        item = PersistenceEventItem(
            client_id=client_id,
            message=message,
        )

        with self._persistence_lock:

            if not self._running:
                return False

            state = self._event_states.get(
                client_id
            )

            if state is None:
                return True

            if not state.enabled:
                return True

            try:
                self.persistence_queue.put_nowait(
                    item
                )

            except queue.Full:

                state.events_dropped += 1
                state.last_error = (
                    "persistence queue full"
                )

                return False

            state.events_enqueued += 1
            state.queue_pending += 1

        return True 
    
    def get_event_state(
        self,
        client_id: bytes,
    ) -> PersistenceEventState | None:

        with self._persistence_lock:

            state = self._event_states.get(
                client_id
            )

            if state is None:
                return None

            return copy.deepcopy(state)
    
    
    
    
    
    
    
    
    
    







class MonitorPersistenceWriter:
    
    def __init__(self, persistence_service: MonitorPersistenceService):
        
        self._buffers: dict[tuple[bytes, Channel], PersistenceBuffer] = {}
        self._buffer_lock = threading.Lock()
        
        self._stream_write_locks: dict[tuple[bytes, Channel], threading.Lock] = {}
        self._stream_write_locks_lock = threading.Lock()
        
        self.persistence_service = persistence_service
        
        self._metadata_write_locks: dict[bytes,threading.Lock,] = {}

        self._metadata_write_locks_lock = (threading.Lock())
        
        self._event_buffers: dict[bytes, list[dict]] = {}
        self._event_buffer_first_ns: dict[bytes, int] = {}
        self._event_write_locks: dict[bytes, threading.Lock] = {}
        self._event_write_locks_lock  = threading.Lock()
        
        self.logger = get_logger("monitor_persistence_writer")
    
    
    def _normalize_main(
        self,
        item: PersistenceItem,
    ) -> NormalizedPersistenceSample | None:

        payload = item.message.payload or {}

        timestamp_utc_ns = payload.get(
            "timestamp_utc_ns"
        )

        if timestamp_utc_ns is None:
            self.logger.error(
                f"Cannot normalize MAIN persistence sample: "
                f"missing timestamp_utc_ns for "
                f"client={item.client_id!r}"
            )
            return None

        data = payload.get("data") or {}

        env = data.get("env") or {}
        power = data.get("power") or {}
        fpga = data.get("fpga") or {}

        row = {
            "timestamp_utc_ns": timestamp_utc_ns,
            "temperature_c": env.get(
                "temperature_c"
            ),
            "humidity_pct": env.get(
                "humidity_pct"
            ),
            "pressure_hpa": env.get(
                "pressure_hpa"
            ),
            "voltage_5v_v": power.get(
                "rail_ain0_v"
            ),
            "voltage_3v3_v": power.get(
                "rail_ain2_v"
            ),
            "current_a": power.get(
                "i_mon_1_a"
            ),
            "fpga_temperature_c": fpga.get(
                "temperature_c"
            )
        }

        return NormalizedPersistenceSample(
            client_id=item.client_id,
            section=item.section,
            rows=(row,),
        )
        
    def _normalize_hv(
        self,
        item: PersistenceItem,
    ) -> NormalizedPersistenceSample | None:

        payload = item.message.payload or {}

        timestamp_utc_ns = payload.get(
            "timestamp_utc_ns"
        )

        if timestamp_utc_ns is None:
            self.logger.error(
                f"Cannot normalize HV persistence sample: "
                f"missing timestamp_utc_ns for "
                f"client={item.client_id!r}"
            )
            return None

        data = payload.get("data") or {}

        electrical = (
            data.get("electrical") or {}
        )

        channels = (
            electrical.get("channels") or {}
        )

        rows = []

        for raw_channel in range(1, 8):

            channel_data = (
                channels.get(raw_channel)
                or channels.get(str(raw_channel))
                or {}
            )

            user_channel = raw_channel - 1

            row = {
                "timestamp_utc_ns": (
                    timestamp_utc_ns
                ),
                "channel": user_channel,
                "voltage": channel_data.get(
                    "voltage"
                ),
                "current": channel_data.get(
                    "current"
                ),
                "temperature": channel_data.get(
                    "temperature"
                ),
                "channel_state": channel_data.get(
                    "channel_state"
                ),
                "power_state": channel_data.get(
                    "power_state"
                ),
            }

            rows.append(row)

        return NormalizedPersistenceSample(
            client_id=item.client_id,
            section=item.section,
            rows=tuple(rows),
        )
        
    
    def _normalize_rc(
        self,
        item: PersistenceItem,
    ) -> NormalizedPersistenceSample | None:

        payload = item.message.payload or {}

        timestamp_utc_ns = payload.get(
            "timestamp_utc_ns"
        )

        if timestamp_utc_ns is None:
            self.logger.error(
                f"Cannot normalize RC persistence sample: "
                f"missing timestamp_utc_ns for "
                f"client={item.client_id!r}"
            )
            return None

        data = payload.get("data") or {}

        free = data.get("free") or {}
        trigger = data.get("trigger") or {}

        free_channels = (
            free.get("channels") or {}
        )

        trigger_channels = (
            trigger.get("channels") or {}
        )

        rows = []

        #
        # Registers 20-26
        #
        for channel in range(7):

            channel_data = (
                free_channels.get(str(channel))
                or free_channels.get(channel)
                or {}
            )

            rows.append(
                {
                    "timestamp_utc_ns": (
                        timestamp_utc_ns
                    ),
                    "register": 20 + channel,
                    "value": channel_data.get(
                        "value"
                    ),
                }
            )

        #
        # Register 27
        #
        external_trigger = (
            trigger.get(
                "external_trigger_rate"
            )
            or {}
        )

        rows.append(
            {
                "timestamp_utc_ns": (
                    timestamp_utc_ns
                ),
                "register": 27,
                "value": external_trigger.get(
                    "value"
                ),
            }
        )

        #
        # Register 28
        #
        auto_trigger = (
            trigger.get(
                "auto_trigger_rate"
            )
            or {}
        )

        rows.append(
            {
                "timestamp_utc_ns": (
                    timestamp_utc_ns
                ),
                "register": 28,
                "value": auto_trigger.get(
                    "value"
                ),
            }
        )

        #
        # Registers 32-38
        #
        for channel in range(7):

            channel_data = (
                trigger_channels.get(str(channel))
                or trigger_channels.get(channel)
                or {}
            )

            rows.append(
                {
                    "timestamp_utc_ns": (
                        timestamp_utc_ns
                    ),
                    "register": 32 + channel,
                    "value": channel_data.get(
                        "value"
                    ),
                }
            )

        return NormalizedPersistenceSample(
            client_id=item.client_id,
            section=item.section,
            rows=tuple(rows),
        )
        
    def _normalize_item(self, item: PersistenceItem) -> NormalizedPersistenceSample | None:
        
        if item.section == Channel.MAIN:
            return self._normalize_main(item)
        
        if item.section == Channel.RC:
            return self._normalize_rc(item)
        
        if item.section == Channel.HV:
            return self._normalize_hv(item)
        
        self.logger.error(
            f"Unsupported persistence section: "
            f"client={item.client_id!r}, "
            f"section={item.section}"
        )

        return None
    
    
    def _buffer_sample(self, normalized_sample: NormalizedPersistenceSample):
        
        batch = None
        client_id = normalized_sample.client_id
        section = normalized_sample.section
        key = (client_id, section)
        
        with self._buffer_lock:
            
            exisisting_norm_sample = self._buffers.get(key)
            
            if exisisting_norm_sample is None:
                exisisting_norm_sample = PersistenceBuffer(samples=[],
                                                           first_sample_monotonic_ns = time.monotonic_ns())
                
                self._buffers[key] = exisisting_norm_sample
            
            if not exisisting_norm_sample.samples:
                exisisting_norm_sample.first_sample_monotonic_ns = time.monotonic_ns()
                
            exisisting_norm_sample.samples.append(normalized_sample)     
            
            if len(exisisting_norm_sample.samples) >= PERSISTENCE_BATCH_SAMPLES:
                batch = exisisting_norm_sample.samples
                exisisting_norm_sample.samples = []
                exisisting_norm_sample.first_sample_monotonic_ns = None
        
        if batch is not None:
            return self._write_batch(client_id=client_id, section=section, samples=batch)
        
        return True

    
    def _get_stream_write_lock(self, client_id: bytes, section: Channel) -> threading.Lock:
        
        key = (client_id, section)
        
        with self._stream_write_locks_lock:
            lock = self._stream_write_locks.get(key)
            
            if lock is None:
                lock = threading.Lock()
                self._stream_write_locks[key] = lock
            
            return lock  
        
    def _get_event_write_lock(
        self,
        client_id: bytes,
    ) -> threading.Lock:

        with self._event_write_locks_lock:

            lock = self._event_write_locks.get(
                client_id
            )

            if lock is None:

                lock = threading.Lock()

                self._event_write_locks[
                    client_id
                ] = lock

            return lock 
    
    def _get_metadata_write_lock(
        self,
        client_id: bytes,
    ) -> threading.Lock:

        with self._metadata_write_locks_lock:

            lock = self._metadata_write_locks.get(
                client_id
            )

            if lock is None:
                lock = threading.Lock()
                self._metadata_write_locks[
                    client_id
                ] = lock

            return lock 
        
    def _get_metadata_transaction_lock(
        self,
        client_id: bytes,
    ) -> threading.Lock:

        with self._metadata_transaction_locks_lock:

            lock = (
                self._metadata_transaction_locks.get(
                    client_id
                )
            )

            if lock is None:

                lock = threading.Lock()

                self._metadata_transaction_locks[
                    client_id
                ] = lock

            return lock 

    def _write_batch(self, client_id: bytes, section: Channel, samples: list[NormalizedPersistenceSample]) -> bool:

        if not samples:
            return True


        stream_state = self.persistence_service.get_stream(client_id=client_id, section=section)

        if stream_state is None:
            self.logger.error(
                f"Cannot write persistence batch: "
                f"stream not configured for "
                f"client={client_id!r}, "
                f"section={section.value}"
            )

            return False

        rows = [row for sample in samples for row in sample.rows]

        if not rows:
            self.logger.warning(
                f"Persistence batch contains no rows: "
                f"client={client_id!r}, "
                f"section={section.value}"
            )

            return True

        stream_lock = self._get_stream_write_lock(client_id=client_id, section=section)

        try:
            with stream_lock:
                if stream_state.save_format == "csv":
                    write_ok = self._write_csv_batch(client_id=client_id, section=section, rows=rows)

                elif stream_state.save_format == "parquet":
                    self.logger.error(
                        f"Parquet persistence is not "
                        f"implemented yet: "
                        f"client={client_id!r}, "
                        f"section={section.value}"
                    )

                    write_ok = False

                else:
                    self.logger.error(
                        f"Unsupported persistence format "
                        f"{stream_state.save_format!r}: "
                        f"client={client_id!r}, "
                        f"section={section.value}"
                    )

                    write_ok = False

        except Exception as e:
            error_message = (
                f"Persistence batch write failed: "
                f"client={client_id!r}, "
                f"section={section.value}, "
                f"error={e}"
            )

            self.logger.exception(error_message)

            self.persistence_service._set_stream_error(client_id=client_id, section=section, error=error_message)

            self.persistence_service._mark_samples_dropped(
                client_id=client_id,
                section=section,
                count=len(samples),
                reason="batch write failed",
            )

            return False

        if not write_ok:
            self.persistence_service._set_stream_error(
                client_id=client_id,
                section=section,
                error="batch write failed",
            )

            self.persistence_service._mark_samples_dropped(
                client_id=client_id,
                section=section,
                count=len(samples),
                reason="batch write failed",
            )


            return False

        self.persistence_service._mark_samples_written(
            client_id=client_id,
            section=section,
            count=len(samples),
        )

        self.persistence_service._mark_rows_written(
            client_id=client_id,
            section=section,
            count=len(rows),
        )

        return True

    def _resolve_client_folder(
        self,
        client_id: bytes,
    ) -> Path | None:

        session = self.persistence_service.get_session()

        if session is None:
            self.logger.error(
                f"Cannot resolve persistence client folder: "
                f"no active persistence session for "
                f"client={client_id!r}"
            )
            return None

        try:
            client_identity = client_id.decode(
                "utf-8",
                errors="strict",
            )
        except UnicodeDecodeError:
            client_identity = client_id.hex()

        safe_identity = "".join(
            char
            if char.isalnum() or char in {"-", "_", "."}
            else "_"
            for char in client_identity
        )

        if not safe_identity:
            self.logger.error(
                f"Cannot resolve persistence client folder: "
                f"invalid client identity "
                f"client={client_id!r}"
            )
            return None

        client_folder = (
            session.root_folder
            / safe_identity
        )

        try:
            client_folder.mkdir(
                parents=True,
                exist_ok=True,
            )
        except Exception as exc:
            self.logger.error(
                f"Cannot create persistence client folder "
                f"{client_folder}: {exc}"
            )
            return None

        return client_folder

    def _resolve_stream_path(
        self,
        client_id: bytes,
        section: Channel,
        save_format: str,
    ) -> Path | None:

        client_folder = self._resolve_client_folder(
            client_id=client_id,
        )

        if client_folder is None:
            return None

        section_name = (
            PERSISTENCE_SECTION_FILENAMES.get(
                section
            )
        )

        if section_name is None:
            self.logger.error(
                f"Cannot resolve persistence stream path: "
                f"unsupported section={section}"
            )
            return None

        if save_format not in {
            "csv",
            "parquet",
        }:
            self.logger.error(
                f"Cannot resolve persistence stream path: "
                f"unsupported format={save_format!r}"
            )
            return None

        return (
            client_folder
            / f"{section_name}.{save_format}"
        )

    def _resolve_metadata_path(
        self,
        client_id: bytes,
    ) -> Path | None:

        client_folder = self._resolve_client_folder(
            client_id=client_id,
        )

        if client_folder is None:
            return None

        return client_folder / "metadata.json"
    
    def _resolve_event_path(
        self,
        client_id: bytes,
        save_format: str,
    ) -> Path | None:

        client_folder = (
            self._resolve_client_folder(
                client_id
            )
        )

        if client_folder is None:
            return None

        return (
            client_folder
            / f"events.{save_format}"
        )


    def _write_csv_batch(
        self,
        client_id: bytes,
        section: Channel,
        rows: list[dict],
    ) -> bool:

        if not rows:
            return True

        if section == Channel.MAIN:
            columns = MAIN_COLUMNS

        elif section == Channel.RC:
            columns = RC_COLUMNS

        elif section == Channel.HV:
            columns = HV_COLUMNS

        else:
            self.logger.error(
                f"Cannot write CSV persistence batch: "
                f"unsupported section={section}"
            )
            return False

        filepath = self._resolve_stream_path(
            client_id=client_id,
            section=section,
            save_format="csv",
        )

        if filepath is None:
            self.logger.error(
                f"Cannot write CSV persistence batch: "
                f"failed to resolve path for "
                f"client={client_id!r}, "
                f"section={section.value}"
            )
            return False

        try:
            file_exists = filepath.exists()

            with filepath.open(
                mode="a",
                newline="",
                encoding="utf-8",
            ) as csv_file:

                writer = csv.DictWriter(
                    csv_file,
                    fieldnames=columns,
                    extrasaction="ignore",
                )

                if not file_exists or filepath.stat().st_size == 0:
                    writer.writeheader()

                for row in rows:
                    normalized_row = {
                        column: row.get(column)
                        for column in columns
                    }

                    writer.writerow(
                        normalized_row
                    )

        except Exception as exc:
            self.logger.exception(
                f"Failed to write CSV persistence batch: "
                f"client={client_id!r}, "
                f"section={section.value}, "
                f"path={filepath}, "
                f"error={exc}"
            )
            return False

        self.logger.debug(
            f"CSV persistence batch written: "
            f"client={client_id!r}, "
            f"section={section.value}, "
            f"rows={len(rows)}, "
            f"path={filepath}"
        )

        return True
    
    
    def _write_event_batch(
        self,
        client_id: bytes,
        rows: list[dict],
    ) -> bool:

        if not rows:
            return True

        state = (
            self.persistence_service
            .get_event_state(
                client_id
            )
        )

        if state is None:
            return False

        if state.save_format != "csv":

            self.logger.error(
                "Only CSV event persistence "
                "is currently implemented"
            )

            self.persistence_service._mark_events_dropped(
                client_id,
                len(rows),
                "event batch write failed",
            )

            return False

        filepath = self._resolve_event_path(
            client_id,
            "csv",
        )

        if filepath is None:
            return False

        lock = self._get_event_write_lock(
            client_id
        )

        try:

            with lock:

                file_exists = filepath.exists()

                with filepath.open(
                    "a",
                    newline="",
                    encoding="utf-8",
                ) as csv_file:

                    writer = csv.DictWriter(
                        csv_file,
                        fieldnames=EVENT_COLUMNS,
                        extrasaction="ignore",
                    )

                    if (
                        not file_exists
                        or filepath.stat().st_size == 0
                    ):
                        writer.writeheader()

                    for row in rows:

                        writer.writerow({
                            column: row.get(column)
                            for column in EVENT_COLUMNS
                        })

        except Exception as exc:

            self.logger.exception(
                "Failed to write monitoring "
                f"events: client={client_id!r}, "
                f"error={exc}"
            )

            self.persistence_service._mark_events_dropped(
                client_id,
                len(rows),
                "event batch write failed",
            )

            return False

        self.persistence_service._mark_events_written(
            client_id,
            len(rows),
        )

        return True
    
    
    def _normalize_event(
        self,
        item: PersistenceEventItem,
    ) -> dict | None:

        message = item.message
        payload = message.payload or {}

        timestamp_utc_ns = payload.get(
            "timestamp_utc_ns"
        )

        if timestamp_utc_ns is None:

            self.logger.error(
                "Cannot normalize persistence event: "
                "missing timestamp_utc_ns for "
                f"client={item.client_id!r}"
            )

            return None

        try:
            channel = message.channel.value
        except AttributeError:
            channel = str(message.channel)

        details = payload.get(
            "details",
            {},
        )

        try:
            details_json = json.dumps(
                details,
                separators=(",", ":"),
                sort_keys=True,
            )

        except Exception:

            details_json = json.dumps(
                {
                    "serialization_error": True,
                    "repr": repr(details),
                },
                separators=(",", ":"),
            )

        return {
            "timestamp_utc_ns": timestamp_utc_ns,
            "channel": channel,
            "event": payload.get(
                "event",
                "unknown_event",
            ),
            "severity": payload.get(
                "severity",
                "info",
            ),
            "sender": (
                message.sender
                if message.sender is not None
                else ""
            ),
            "source_request_id": payload.get(
                "source_request_id"
            ),
            "details_json": details_json,
            "error": payload.get("error"),
        }
                    
    
    def process_item(self, item: PersistenceItem) -> bool:
        normalized_sample = self._normalize_item(item)
        
        if normalized_sample is None:
            self.persistence_service._mark_samples_dropped(
                client_id=item.client_id,
                section=item.section,
                count=1,
                reason="persistence normalization failed",
            )
            return False
        
        return self._buffer_sample(normalized_sample)
    
    
    def process_event_item(
        self,
        item: PersistenceEventItem,
    ) -> bool:

        row = self._normalize_event(
            item
        )

        if row is None:

            self.persistence_service._mark_events_dropped(
                client_id=item.client_id,
                count=1,
                reason="event normalization failed",
            )

            return False

        batch = None

        with self._buffer_lock:

            buffer = self._event_buffers.setdefault(
                item.client_id,
                [],
            )

            if not buffer:

                self._event_buffer_first_ns[
                    item.client_id
                ] = time.monotonic_ns()

            buffer.append(row)

            if len(buffer) >= PERSISTENCE_BATCH_SAMPLES:

                batch = list(buffer)
                buffer.clear()

                self._event_buffer_first_ns.pop(
                    item.client_id,
                    None,
                )

        if batch is None:
            return True

        return self._write_event_batch(
            client_id=item.client_id,
            rows=batch,
        )
    
    
    def flush_stream(
        self,
        client_id: bytes,
        section: Channel,
    ) -> bool:

        key = (
            client_id,
            section,
        )

        batch = None

        with self._buffer_lock:

            buffer = self._buffers.get(
                key
            )

            if buffer is not None:

                if buffer.samples:

                    batch = buffer.samples
                    buffer.samples = []

                buffer.first_sample_monotonic_ns = None

        if batch is not None:

            return self._write_batch(
                client_id=client_id,
                section=section,
                samples=batch,
            )

        
        stream_lock = self._get_stream_write_lock(
            client_id=client_id,
            section=section,
        )

        with stream_lock:
            pass

        return True
    
    def flush_due(self) -> bool:

        now_monotonic_ns = time.monotonic_ns()

        flush_interval_ns = int(
            PERSISTENCE_FLUSH_INTERVAL_S
            * 1_000_000_000
        )

        due_sample_batches = []
        due_event_batches = []

       
        with self._buffer_lock:

            
            for key, buffer in self._buffers.items():

                if not buffer.samples:
                    continue

                if buffer.first_sample_monotonic_ns is None:
                    continue

                elapsed_ns = (
                    now_monotonic_ns
                    - buffer.first_sample_monotonic_ns
                )

                if elapsed_ns < flush_interval_ns:
                    continue

                batch = buffer.samples

                buffer.samples = []
                buffer.first_sample_monotonic_ns = None

                client_id, section = key

                due_sample_batches.append(
                    (
                        client_id,
                        section,
                        batch,
                    )
                )

            
            for client_id, event_rows in (
                self._event_buffers.items()
            ):

                if not event_rows:
                    continue

                first_event_ns = (
                    self._event_buffer_first_ns.get(
                        client_id
                    )
                )

                if first_event_ns is None:
                    continue

                elapsed_ns = (
                    now_monotonic_ns
                    - first_event_ns
                )

                if elapsed_ns < flush_interval_ns:
                    continue

                batch = list(event_rows)

                event_rows.clear()

                self._event_buffer_first_ns.pop(
                    client_id,
                    None,
                )

                due_event_batches.append(
                    (
                        client_id,
                        batch,
                    )
                )

      
        success = True

        
        for (
            client_id,
            section,
            batch,
        ) in due_sample_batches:

            write_ok = self._write_batch(
                client_id=client_id,
                section=section,
                samples=batch,
            )

            if not write_ok:
                success = False

        
        for (
            client_id,
            batch,
        ) in due_event_batches:

            write_ok = self._write_event_batch(
                client_id=client_id,
                rows=batch,
            )

            if not write_ok:
                success = False

        return success
    
    
    def flush_all(self) -> bool:

        sample_batches = []
        event_batches = []

        
        with self._buffer_lock:

            
            for key, buffer in self._buffers.items():

                if not buffer.samples:

                    buffer.first_sample_monotonic_ns = None
                    continue

                batch = buffer.samples

                buffer.samples = []
                buffer.first_sample_monotonic_ns = None

                client_id, section = key

                sample_batches.append(
                    (
                        client_id,
                        section,
                        batch,
                    )
                )

            
            for client_id, event_rows in (
                self._event_buffers.items()
            ):

                if not event_rows:

                    self._event_buffer_first_ns.pop(
                        client_id,
                        None,
                    )

                    continue

                batch = list(event_rows)

                event_rows.clear()

                self._event_buffer_first_ns.pop(
                    client_id,
                    None,
                )

                event_batches.append(
                    (
                        client_id,
                        batch,
                    )
                )

        
        success = True

        
        for (
            client_id,
            section,
            batch,
        ) in sample_batches:

            write_ok = self._write_batch(
                client_id=client_id,
                section=section,
                samples=batch,
            )

            if not write_ok:
                success = False

        
        for (
            client_id,
            batch,
        ) in event_batches:

            write_ok = self._write_event_batch(
                client_id=client_id,
                rows=batch,
            )

            if not write_ok:
                success = False

        return success
    
    
    def flush_events(
        self,
        client_id: bytes,
    ) -> bool:

        batch = None

        with self._buffer_lock:

            rows = self._event_buffers.get(
                client_id
            )

            if rows:

                batch = list(rows)
                rows.clear()

            self._event_buffer_first_ns.pop(
                client_id,
                None,
            )

        if batch:

            return self._write_event_batch(
                client_id,
                batch,
            )

        lock = self._get_event_write_lock(
            client_id
        )

        with lock:
            pass

        return True
    
    
    def write_metadata(
        self,
        client_id: bytes,
        metadata: dict,
    ) -> bool:

        metadata_path = self._resolve_metadata_path(
            client_id=client_id
        )

        if metadata_path is None:
            return False

        temporary_path = metadata_path.with_name(
            metadata_path.name + ".tmp"
        )

        metadata_lock = (
            self._get_metadata_write_lock(
                client_id
            )
        )

        try:

            with metadata_lock:

                with temporary_path.open(
                    mode="w",
                    encoding="utf-8",
                ) as metadata_file:

                    json.dump(
                        metadata,
                        metadata_file,
                        indent=2,
                        sort_keys=True,
                    )

                    metadata_file.flush()

                temporary_path.replace(
                    metadata_path
                )

        except Exception as exc:

            self.logger.exception(
                f"Failed to write persistence metadata: "
                f"client={client_id!r}, "
                f"path={metadata_path}, "
                f"error={exc}"
            )

            try:
                temporary_path.unlink(
                    missing_ok=True
                )
            except Exception:
                pass

            return False

        self.logger.debug(
            f"Persistence metadata written: "
            f"client={client_id!r}, "
            f"path={metadata_path}"
        )

        return True
    
    
    
                
        
        