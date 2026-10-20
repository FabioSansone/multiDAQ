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


    
    def _prepare_calibration_point(
        self,
        channels: list[int],
        *,
        voltage: int,
        threshold: float,
        calibration_name: str,
    ) -> bool:

        if self.runtime.hv_service is None:
            self.logger.error(
                f"Cannot prepare {calibration_name}: HVService unavailable"
            )
            return False

        if self.runtime.acq_mode != "calibration":
            self.logger.error(
                f"Cannot prepare {calibration_name}: "
                "Acquisition mode is not calibration"
            )
            return False

        if not channels:
            self.logger.error(
                f"Cannot prepare {calibration_name}: no channels selected"
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
                    sender=f"{calibration_name}",
                    priority=HVMessagePriority.ACQUISITION,
                    timeout_s=300.0,
                )
            )

            if config_response.status != MessageStatus.OK:
                self.logger.error(
                    f"{calibration_name} HV configuration failed: "
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
                    f"{calibration_name} HV configuration incomplete: "
                    f"{config_result}"
                )
                return False

            on_response = (
                self.runtime.hv_service._submit_command(
                    command="hv_on_and_wait",
                    payload={
                        "channels": hv_channels,
                    },
                    sender=f"{calibration_name}",
                    priority=HVMessagePriority.ACQUISITION,
                    timeout_s=300.0,
                )
            )

            if on_response.status != MessageStatus.OK:
                self.logger.error(
                    f"{calibration_name} HV power-on failed: "
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
                    f"{calibration_name} HV power-on incomplete: "
                    f"{on_result}"
                )
                return False

            self.logger.info(
                f"{calibration_name} preparation completed: "
                f"channels={channels}, "
                f"voltage={voltage}, "
                f"threshold={threshold}"
            )

            return True

        except Exception:
            self.logger.exception(
                f"Unexpected error during {calibration_name} preparation"
            )
            return False


    def prepare_spe(
        self,
        channels: list[int],
        *,
        voltage: int,
        threshold: float,
    ) -> bool:
        return self._prepare_calibration_point(
            channels=channels,
            voltage=voltage,
            threshold=threshold,
            calibration_name="SPE",
        )


    def prepare_gain(
        self,
        channels: list[int],
        *,
        voltage: int,
        threshold: float,
    ) -> bool:
        return self._prepare_calibration_point(
            channels=channels,
            voltage=voltage,
            threshold=threshold,
            calibration_name="GAIN",
        )


    def prepare_threshold_calibration(
        self,
        channels: list[int],
        *,
        voltage_by_channel: dict[int, float],
        threshold: float,
    ) -> dict:

        requested_channels = sorted(set(channels))

        result = {
            "success": False,
            "successful_channels": [],
            "excluded_channels": requested_channels,
            "error": None,
        }

        if self.runtime.acq_mode not in {"multipmt", "calibration"}:
            result["error"] = (
                "Threshold HV preparation requires "
                "calibration or multipmt mode"
            )
            return result

        if self.runtime.hv_service is None:
            result["error"] = "HVService unavailable"
            return result

        if not requested_channels:
            result["error"] = "No channels selected"
            return result

        missing_channels = [
            ch
            for ch in requested_channels
            if ch not in voltage_by_channel
        ]

        if missing_channels:
            result["error"] = (
                f"Missing calibrated voltage for channels "
                f"{missing_channels}"
            )
            return result

        try:
            hv_channels = {ch + 1 for ch in requested_channels}

            acq_conf = {
                ch: {
                    "voltage": voltage_by_channel[ch],
                    "threshold": threshold,
                }
                for ch in requested_channels
            }

            config_response = self.runtime.hv_service._submit_command(
                command="set_acquisition_configuration",
                payload={
                    "channels": sorted(hv_channels),
                    "acquisition_configuration": acq_conf,
                },
                sender="Threshold Scan",
                priority=HVMessagePriority.ACQUISITION,
                timeout_s=300.0,
            )

            if config_response.status != MessageStatus.OK:
                result["error"] = (
                    config_response.error or "HV configuration failed"
                )
                return result

            config_result = config_response.result or {}

            configured_channels = (
                hv_channels
                & set(config_result.get("successful_channels") or [])
            )

            configured_channels -= (
                set(config_result.get("failed_channels") or [])
                | set(config_result.get("skipped_channels") or [])
            )

            if not configured_channels:
                result["error"] = (
                    "HV configuration failed for all selected channels"
                )
                return result

            if configured_channels != hv_channels:
                self.logger.warning(
                    f"THR HV configuration incomplete: {config_result}"
                )

            on_response = self.runtime.hv_service._submit_command(
                command="hv_on_and_wait",
                payload={
                    "channels": sorted(configured_channels),
                },
                sender="Threshold Scan",
                priority=HVMessagePriority.ACQUISITION,
                timeout_s=300.0,
            )

            if on_response.status != MessageStatus.OK:
                result["error"] = (
                    on_response.error or "HV power-on failed"
                )
                return result

            on_result = on_response.result or {}

            ready_hv_channels = (
                configured_channels
                & set(on_result.get("successful_channels") or [])
            )

            ready_hv_channels -= (
                set(on_result.get("failed_channels") or [])
                | set(on_result.get("skipped_channels") or [])
            )

            if not ready_hv_channels:
                result["error"] = (
                    "HV power-on failed for all configured channels"
                )
                return result

            if ready_hv_channels != configured_channels:
                self.logger.warning(
                    f"THR HV power-on incomplete: {on_result}"
                )

            ready_channels = {
                ch - 1
                for ch in ready_hv_channels
            }

            excluded_channels = (
                set(requested_channels) - ready_channels
            )

            result.update(
                success=True,
                successful_channels=sorted(ready_channels),
                excluded_channels=sorted(excluded_channels),
                error=None,
            )

            self.logger.info(
                "THR preparation completed: "
                f"successful_channels={result['successful_channels']}, "
                f"excluded_channels={result['excluded_channels']}, "
                f"common_threshold={threshold}"
            )

            return result

        except Exception as exc:
            self.logger.exception(
                "Unexpected error during THR preparation"
            )
            result["error"] = str(exc)
            return result
            

        
                


        