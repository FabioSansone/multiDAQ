from dataclasses import dataclass
import threading
import time
from typing import Optional

from server.utils.logger import get_logger



@dataclass(frozen=True)
class PendingTimeSyncProbe:
    client_id: bytes
    request_id: str
    
    t1_utc_ns: int
    t1_server_monotonic_ns: int
    
@dataclass(frozen=True)
class TimeSyncMeasurement:
    client_id: bytes
    request_id: str
    boot_id: str
    
    t1_utc_ns: int
    t1_server_monotonic_ns: int
    
    c2_client_monotonic_ns: int
    c3_client_monotonic_ns: int
    
    t4_server_monotonic_ns: int
    
    network_rtt_ns: int
    
    ref_client_monotonic_ns: int #client midpoint in sync c2 + c3 /2
    ref_utc_ns: int #server midpoint in sync t1_utc + t4_utc / 2
    
    @property
    def offset_ns(self) -> int:
        return self.ref_utc_ns - self.ref_client_monotonic_ns
    
    @property
    def uncertainty_ns(self) -> int:
        """Conservative estimate assuming unknown network asymmetry"""
        return self.network_rtt_ns // 2

@dataclass(frozen=True)
class ClientTimeSyncState:
    client_id: bytes
    boot_id: str
    
    ref_client_monotonic_ns: int
    ref_utc_ns: int
    
    network_rtt_ns: int
    uncertainty_ns: int
    
    synced_at_server_monotonic_ns: int
    
    @property
    def offset_ns(self) -> int:
        return self.ref_utc_ns - self.ref_client_monotonic_ns 


class TimeSyncService:
    
    
    def __init__(self):
        
        self._pending_probes: dict[str, PendingTimeSyncProbe] = {}
        self._client_states: dict[bytes, ClientTimeSyncState] = {}
        
        self._lock = threading.Lock()
        
        self.logger = get_logger("time_sync_service")
        self.logger.debug("TimeSyncService initialized")
        
    
    def register_probe_sent(self, *, client_id: bytes, request_id: str, t1_utc_ns: int, t1_server_monotonic_ns: int) -> None:
        """
        Register a time-sync probe immediately before it is sent
        by the Monitoring Plane socket.
        """
        
        probe = PendingTimeSyncProbe(
            client_id=client_id,
            request_id=request_id,
            t1_utc_ns=t1_utc_ns,
            t1_server_monotonic_ns=t1_server_monotonic_ns
        )
    
        with self._lock:
            self._pending_probes[request_id] = probe
            
    
    def cancel_probe(self, request_id: str) -> None:
        with self._lock:
            self._pending_probes.pop(request_id, None)
    
    
    def complete_probe(self, *, client_id: bytes, in_reply_to: str, boot_id: str, c2_client_monotonic_ns: int, c3_client_monotonic_ns: int, t4_server_monotonic_ns: int ) -> Optional[TimeSyncMeasurement]:
        
        with self._lock:
            probe = self._pending_probes.pop(in_reply_to, None)
        
        if probe is None:
            self.logger.warning(f"Received time-sync reply for unknown probe: {in_reply_to}")
            return None
        
        if probe.client_id != client_id:
            self.logger.error(f"Time-sync reply client mismatch: expected={probe.client_id!r}, received={client_id!r}")
            return None
        
        if not boot_id:
            self.logger.error("Time-sync reply without boot_id")
            return None
        
        if c3_client_monotonic_ns < c2_client_monotonic_ns:
            self.logger.error(f"Invalid client monotonic ordering in time-sync probe: C2={c2_client_monotonic_ns}, C3={c3_client_monotonic_ns}")
            return None
        
        if t4_server_monotonic_ns < probe.t1_server_monotonic_ns:
            self.logger.error("Invalid server monotonic ordering in time-sync probe")
            return None
        
        server_elapsed_ns = t4_server_monotonic_ns - probe.t1_server_monotonic_ns
        client_processing_ns = c3_client_monotonic_ns - c2_client_monotonic_ns
        network_rtt_ns = server_elapsed_ns - client_processing_ns
        
        if network_rtt_ns < 0:
            self.logger.error(f"Invalid negative network RTT in time-sync probe: server_elapsed={server_elapsed_ns}, client_processing={client_processing_ns}")
            return None
        
        client_midpoint_monotonic_ns = (c2_client_monotonic_ns + c3_client_monotonic_ns) // 2
        server_midpoint_utc_ns = (probe.t1_utc_ns + server_elapsed_ns // 2) 
        
        return TimeSyncMeasurement(
            client_id=client_id,
            request_id=in_reply_to,
            boot_id=boot_id,
            
            t1_utc_ns=probe.t1_utc_ns,
            t1_server_monotonic_ns=probe.t1_server_monotonic_ns,
            
            c2_client_monotonic_ns=c2_client_monotonic_ns,
            c3_client_monotonic_ns=c3_client_monotonic_ns,
            
            t4_server_monotonic_ns=t4_server_monotonic_ns,
            
            network_rtt_ns=network_rtt_ns,
            
            ref_client_monotonic_ns=client_midpoint_monotonic_ns,
            ref_utc_ns=server_midpoint_utc_ns
        )   
    
    def apply_measurements(self, *, client_id: bytes, measurements: list[TimeSyncMeasurement],) -> Optional[ClientTimeSyncState]:
        
        valid_measurements = [measurement for measurement in measurements if measurement.client_id == client_id]
        if not valid_measurements:
            self.logger.error(f"No valid time-sync measurements for {client_id!r}")
            return None
        
        boot_ids = {measurement.boot_id for measurement in valid_measurements}
        if len(boot_ids) != 1:
            self.logger.warning(f"Client boot changed during time synchronization: client={client_id!r}, boot_ids={boot_ids}")
            self.invalidate_client(client_id, reason="boot changed during synchronization")
            return None
        
        best = min(valid_measurements, key=lambda measurement: measurement.network_rtt_ns)
        state = ClientTimeSyncState(
            client_id=client_id,
            boot_id=best.boot_id,
            ref_client_monotonic_ns=best.ref_client_monotonic_ns,
            ref_utc_ns=best.ref_utc_ns,
            
            network_rtt_ns=best.network_rtt_ns,
            uncertainty_ns=best.uncertainty_ns,
            
            synced_at_server_monotonic_ns=best.t4_server_monotonic_ns
        )
        
        with self._lock:
            previous_state = self._client_states.get(client_id)
            self._client_states[client_id] = state
        
        if previous_state is not None and previous_state.boot_id != state.boot_id:
            self.logger.info(f"Client boot_id changed during time synchronization: client={client_id!r}, old={previous_state.boot_id}, new={state.boot_id}")
        
        self.logger.info(f"Client time synchronized: client={client_id!r}, boot_id={state.boot_id}, rtt={state.network_rtt_ns / 1e6:.3f} ms, uncertainty≈{state.uncertainty_ns / 1e6:.3f} ms")
        return state
    
    def invalidate_client(self, client_id: bytes, *, reason: str = "unspecified") -> None:
        with self._lock:
            removed_state = self._client_states.pop(client_id, None)
            
            pending_ids = [request_id for request_id, probe in self._pending_probes.items() if probe.client_id == client_id]
            for request_id in pending_ids:
                self._pending_probes.pop(request_id, None)
                
        if removed_state is not None:
            self.logger.info(f"Client time synchronization invalidated: client={client_id!r}, reason={reason}")
            
    
    def clear(self) -> None:
        with self._lock:
            self._client_states.clear()
            self._pending_probes.clear()
            
    def client_monotonic_to_utc(self, *, client_id: bytes, client_monotonic_ns: int,) -> tuple[Optional[int], str]:
        with self._lock:
            state = self._client_states.get(client_id)
        
        if state is None:
            return None, "client not synchronized"
        
        delta_ns = client_monotonic_ns - state.ref_client_monotonic_ns
        timestamp_utc_ns = state.ref_utc_ns + delta_ns
        
        return timestamp_utc_ns, "ok"
    
    def get_state(self, client_id: bytes) -> Optional[ClientTimeSyncState]:
        with self._lock:
            return self._client_states.get(client_id)
    
    def is_synchronized(self, client_id: bytes) -> bool:
        with self._lock:
            return client_id in self._client_states
    
    def need_resync(self, *, client_id: bytes, max_age_s: float) -> bool:
        
        with self._lock:
            state = self._client_states.get(client_id)
        
        if state is None:
            return True
        
        age_ns = time.monotonic_ns() - state.synced_at_server_monotonic_ns
        return age_ns >= int(max_age_s * 1_000_000_000)
    
        
