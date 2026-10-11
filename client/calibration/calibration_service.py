from typing import TYPE_CHECKING
from common.message_handler import MessageStatus
from client.hardware.hv.hv_service import HVMessagePriority
from client.hardware.rc.rc_service import RCMessagePriority
if TYPE_CHECKING:
    from client.core.client_runtime import ClientRunTime
from client.utils.logger import get_logger


class CalibrationService:

    def __init__(self, runtime: "ClientRunTime",):

        self.runtime = runtime

        self.logger = get_logger("calibration_service")
        self.logger.info("CalibrationService initialized")

    def prepare_pedestal(self, channels: list[int],) -> bool:

        if self.runtime.hv_service is None:
            self.logger.error(
                "Cannot prepare pedestal: "
                "HVService unavailable"
            )
            return False

        try:
            hv_channels = [
                channel + 1
                for channel in channels
            ]

            hv_response = (
                self.runtime.hv_service._submit_command(
                    command="hv_off_and_wait",
                    payload={
                        "channels": hv_channels,
                    },
                    sender="client_calibration_pedestal",
                    priority=HVMessagePriority.ACQUISITION,
                    timeout_s=300.0,
                )
            )

            if (
                hv_response.status
                != MessageStatus.OK
            ):
                self.logger.error(
                    "Pedestal preparation failed "
                    "while switching HV off: "
                    f"{hv_response.error}"
                )
                return False

            rc_response = (
                self.runtime.rc_service._submit_command(
                    command="rc_write_register",
                    payload={
                        "address": 12,
                        "value": 1,
                    },
                    sender="client_calibration_pedestal",
                    priority=RCMessagePriority.ACQUISITION,
                    timeout_s=15.0,
                )
            )

            if (
                rc_response.status
                != MessageStatus.OK
            ):
                self.logger.error(
                    "Pedestal preparation failed "
                    "while setting RC12=1: "
                    f"{rc_response.error}"
                )
                return False

            return True

        except Exception:
            self.logger.exception(
                "Unexpected error while preparing "
                "pedestal calibration"
            )
            return False


    
    def prepare_spe(
        self,
        channels: list[int],
        *,
        voltage: int,
        threshold: float,
    ) -> bool:

        if self.runtime.hv_service is None:
            self.logger.error(
                "Cannot prepare SPE: HVService unavailable"
            )
            return False

        if self.runtime.acq_mode != "calibration":
            self.logger.error(
                "Cannot prepare SPE: "
                "Acquisition mode is not calibration"
            )
            return False

        if not channels:
            self.logger.error(
                "Cannot prepare SPE: no channels selected"
            )
            return False

        try:
            hv_channels = [
                channel + 1
                for channel in channels
            ]

            acq_conf = {
                ch: {
                    "voltage": voltage,
                    "threshold": threshold,
                }
                for ch in channels
            }

            config_response = (
                self.runtime.hv_service._submit_command(
                    command="set_acquisition_configuration",
                    payload={
                        "channels": hv_channels,
                        "acquisition_configuration": acq_conf,
                    },
                    sender="client_calibration_spe",
                    priority=HVMessagePriority.ACQUISITION,
                    timeout_s=300.0,
                )
            )

            if config_response.status != MessageStatus.OK:
                self.logger.error(
                    "SPE HV configuration failed: "
                    f"{config_response.error}"
                )
                return False

            config_result = config_response.result or {}

            if (
                set(config_result.get("successful_channels", []))
                != set(hv_channels)
                or config_result.get("skipped_channels")
                or config_result.get("failed_channels")
            ):
                self.logger.error(
                    "SPE HV configuration incomplete: "
                    f"{config_result}"
                )
                return False

            on_response = (
                self.runtime.hv_service._submit_command(
                    command="hv_on_and_wait",
                    payload={
                        "channels": hv_channels,
                    },
                    sender="client_calibration_spe",
                    priority=HVMessagePriority.ACQUISITION,
                    timeout_s=300.0,
                )
            )

            if on_response.status != MessageStatus.OK:
                self.logger.error(
                    "SPE HV power-on failed: "
                    f"{on_response.error}"
                )
                return False

            on_result = on_response.result or {}

            if (
                set(on_result.get("successful_channels", []))
                != set(hv_channels)
                or on_result.get("skipped_channels")
                or on_result.get("failed_channels")
            ):
                self.logger.error(
                    "SPE HV power-on incomplete: "
                    f"{on_result}"
                )
                return False

            self.logger.info(
                "SPE preparation completed: "
                f"channels={channels}, "
                f"voltage={voltage}, "
                f"threshold={threshold}"
            )

            return True

        except Exception:
            self.logger.exception(
                "Unexpected error during SPE preparation"
            )
            return False

        