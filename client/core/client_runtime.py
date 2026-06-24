from typing import Optional
import time
from client.utils.logger import get_logger
from client.communication.identity import ClientIdentity
from client.hardware.hv.hv_service import HVService
from client.hardware.rc.rc_service import RCService
from client.hardware.evproducer.ev_service import EVService
from client.acquisition.acquisition_service import AcquisitionService
from common.message_handler import MessageStatus


class ClientRunTime:
    def __init__(
        self,
        identity: ClientIdentity,
        server_ip: str,
        hv_port: str,
    ) -> None:
        self.logger = get_logger("client_runtime")

        self.identity = identity
        self.server_ip = server_ip
        self.hv_port = hv_port

        self.hv_service: Optional[HVService] = None
        self.rc_service = RCService()
        self.evproducer = EVService()

        self.acq_mode: Optional[str] = None
        self.acq_info: Optional[dict] = None
        self.start_thr: Optional[int | float] = None

        self.acquisition_service = AcquisitionService(self)
        
        self._last_rc39_sync_time = 0.0
        self._last_rc39_mask = None
        self._rc39_sync_period_s = 30.0

        self.logger.info("ClientRuntime initialized")

    def _sanitize_identity_part(self, value: str) -> str:
        return (
            str(value)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
        )

    def zmq_identity(self) -> str:
        multipmt_id = self._sanitize_identity_part(
            self.identity.multipmt_id or "unknown"
        )
        batch_id = self._sanitize_identity_part(
            self.identity.batch_id or "unknown"
        )
        mac_suffix = self.identity.mac.replace(":", "")[-6:]

        return f"{multipmt_id}-{batch_id}-{mac_suffix}"

    def ensure_hv_service(self) -> bool:
        if self.hv_service is not None:
            return True

        try:
            self.hv_service = HVService(hv_port=self.hv_port, state_change_callback=self.sync_rc_register_39_with_hv,)
            self.logger.info("HVService initialized")
            return True

        except Exception as e:
            self.logger.error(f"Cannot initialize HVService: {e}")
            self.hv_service = None
            return False

    def stop_hv_service(self) -> None:
        if self.hv_service is None:
            return

        try:
            self.hv_service.stop()
            self.logger.info("HVService stopped")
        except Exception as e:
            self.logger.error(f"Error while stopping HVService: {e}")
        finally:
            self.hv_service = None
    
    def sync_rc_register_39_with_hv(self) -> bool:
        if self.hv_service is None:
            self.logger.warning("Cannot sync RC register 39: HVService unavailable")
            return False

        ok_channels = set(self.hv_service.hv.getOkChannels())
        on_channels = set(self.hv_service.hv.getOnChannels())

        if self.acq_mode == "test":
            hv_enabled_channels = sorted(ok_channels)
        else:
            hv_enabled_channels = sorted(ok_channels & on_channels)

        rc_channels = [ch - 1 for ch in hv_enabled_channels]

        mask = 0
        for ch in rc_channels:
            if ch < 0 or ch >= 7:
                self.logger.error(f"Invalid RC channel derived from HV: {ch}")
                return False

            mask |= 1 << ch

        now = time.time()

        if (
            self._last_rc39_mask == mask
            and now - self._last_rc39_sync_time < self._rc39_sync_period_s
        ):
            return True

        response = self.rc_service._submit_command(
            command="rc_write_register",
            payload={
                "address": 39,
                "value": mask,
            },
            sender="client_runtime_rc39_sync",
        )

        if response.status != MessageStatus.OK:
            self.logger.error(
                f"Failed to sync RC register 39 with HV state: {response.error}"
            )
            return False

        self._last_rc39_mask = mask
        self._last_rc39_sync_time = now

        self.logger.info(
            f"RC register 39 synchronized: "
            f"mode={self.acq_mode}, hv_channels={hv_enabled_channels}, "
            f"rc_channels={rc_channels}, mask={mask}"
        )

        return True

    def set_acquisition_mode(
        self,
        acq_mode: str,
        acq_info: dict | None = None,
        start_thr: int | float | None = None,
    ) -> None:
        self.acq_mode = acq_mode
        self.acq_info = acq_info
        self.start_thr = start_thr

        self.logger.info(
            f"Client runtime acquisition mode set to {acq_mode}"
        )

    def clear_acquisition_configuration(self) -> None:
        self.acq_info = None
        self.start_thr = None
        self.logger.debug("Acquisition configuration cleared")

    def close(self) -> None:
        self.stop_hv_service()

        try:
            self.evproducer.stop()
        except Exception as e:
            self.logger.error(f"Error while stopping evproducer: {e}")

        self.logger.info("ClientRuntime closed")