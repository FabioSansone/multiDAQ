from typing import TYPE_CHECKING

from common.message_handler import MessageStatus
from common.timeouts import client_timeout
from client.hardware.hv.hv_messages import HVMessagePriority
from client.hardware.rc.rc_messages import RCMessagePriority
from client.utils.channels import channels_definition
from client.utils.logger import get_logger

if TYPE_CHECKING:
    from client.core.client_runtime import ClientRunTime


class CalibrationService:
    """Hardware preparation of the calibrations, on the client."""

    def __init__(self, runtime: "ClientRunTime") -> None:
        self.runtime = runtime

        self.logger = get_logger("calibration_service")
        self.logger.info("CalibrationService initialized")

    def prepare_pedestal(self, channels: list[int]) -> bool:
        """
        Pedestal: HV off on the channels (waits for the ramp down), then RC12=1.

        channels: logical numbering 0..6 (the HV numbering is obtained here, once).
        True if both steps succeeded.
        """

        if self.runtime.hv_service is None:
            self.logger.error("Cannot prepare pedestal: HVService unavailable")
            return False

        if self.runtime.rc_service is None:
            self.logger.error("Cannot prepare pedestal: RCService unavailable")
            return False

        try:
            hv_channels = channels_definition(channels, hv_channels=True)
        except Exception as exc:
            self.logger.error(f"Cannot prepare pedestal: invalid channels {channels!r}: {exc}")
            return False

        if not hv_channels:
            self.logger.error("Cannot prepare pedestal: no channels")
            return False

        try:
            hv_response = self.runtime.hv_service._submit_command(
                command="hv_off_and_wait",
                payload={"channels": hv_channels},
                sender="client_calibration_pedestal",
                priority=HVMessagePriority.ACQUISITION,
                timeout_s=client_timeout("hv_off_and_wait"),
            )

            if hv_response.status != MessageStatus.OK:
                self.logger.error(f"Pedestal preparation failed while switching HV off: {hv_response.error}")
                return False

            rc_response = self.runtime.rc_service._submit_command(
                command="rc_write_register",
                payload={"address": 12, "value": 1},
                sender="client_calibration_pedestal",
                priority=RCMessagePriority.ACQUISITION,
                timeout_s=client_timeout("rc_write_register"),
            )

            if rc_response.status != MessageStatus.OK:
                self.logger.error(f"Pedestal preparation failed while setting RC12=1: {rc_response.error}")
                return False

            return True

        except Exception:
            self.logger.exception("Unexpected error while preparing the pedestal calibration")
            return False
