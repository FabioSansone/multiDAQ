from typing import Optional

from client.utils.logger import get_logger
from client.communication.identity import ClientIdentity
from client.hardware.hv.hv_service import HVService
from client.hardware.rc.rc_service import RCService
from client.hardware.evproducer.ev_service import EVService
from client.acquisition.acquisition_service import AcquisitionService


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
            self.hv_service = HVService(hv_port=self.hv_port)
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