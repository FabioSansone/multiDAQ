from server.services.client_command_service import CommandPlane
from server.utils.logger import get_logger


class MonitoringService:

    def __init__(
        self,
        command_service,
        output_func=None,
    ) -> None:

        self.command_service = command_service

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
        
    