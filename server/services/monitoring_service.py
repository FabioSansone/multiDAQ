from server.services.client_command_service import CommandPlane
from common.message_handler import Channel, MessageStatus
from server.services.time_sync_service import ClientTimeSyncState
from server.utils.logger import get_logger

from typing import Optional

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

        self.logger = get_logger("monitoring_service")
        self.logger.debug("Monitoring Service initialized")
        
    


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
        
    def _queue_time_sync_probe(self, client_id: bytes,) -> str:
        probe = self.message_handler.create_command(
            channel=Channel.MONITORING,
            command="time_sync_probe",
            payload={
                "time_sync_protocol_version": 1,
            },
            sender="server",
        )
        
        self.queue_message(
            client_id,
            probe,
        )
        
        return probe.request_id

        
    def synchronize_client(self, client_id: bytes, *, probe_count: int = 5, probe_timeout_s: float = 2.0,) -> Optional[ClientTimeSyncState]:
            
        measurements = []
        
        
        
        for _ in range(probe_count):
            probe_request_id = self._queue_time_sync_probe(client_id=client_id)
            
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
        
        return self.time_sync_service.apply_measurements(
            client_id=client_id,
            measurements=measurements,
        )
    
    def ensure_client_synchronized(self, client_id: bytes) -> bool:
        if self.time_sync_service.is_synchronized(client_id):
            return True
        
        state = self.synchronize_client(client_id)
        
        return state is not None