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

    def prepare_hv_point(
        self,
        channels: list[int],
        *,
        voltage: float,
        threshold: float,
        calibration_name: str,
    ) -> dict:
        """
        HV of a calibration point with a common voltage and threshold: configuration of the
        channels, then power on (waits for the ramp). Calibration mode only.

        channels: logical numbering 0..6 (the HV numbering is obtained here, once).

        A channel that cannot be configured or switched on is excluded, the others go on:
        success is True if at least one channel is ready. A channel that is excluded is not
        confirmed to be off.

        Returns {"success": bool, "successful_channels": [...], "excluded_channels": [...],
        "error": str | None}, with logical channels, serializable.
        """

        requested_channels = sorted(set(channels))

        result = {
            "success": False,
            "successful_channels": [],
            "excluded_channels": requested_channels,
            "error": None,
        }

        if self.runtime.acq_mode != "calibration":
            result["error"] = f"{calibration_name} preparation requires the calibration mode"
            self.logger.error(result["error"])
            return result

        if self.runtime.hv_service is None:
            result["error"] = "HVService unavailable"
            self.logger.error(f"Cannot prepare {calibration_name}: {result['error']}")
            return result

        if not requested_channels:
            result["error"] = "No channels selected"
            self.logger.error(f"Cannot prepare {calibration_name}: {result['error']}")
            return result

        try:
            hv_channels = set(channels_definition(requested_channels, hv_channels=True))

            # The keys of the configuration are logical channels, "channels" are HV channels.
            acquisition_configuration = {
                channel: {"voltage": voltage, "threshold": threshold}
                for channel in requested_channels
            }

            config_response = self.runtime.hv_service._submit_command(
                command="set_acquisition_configuration",
                payload={
                    "channels": sorted(hv_channels),
                    "acquisition_configuration": acquisition_configuration,
                },
                sender=f"client_calibration_{calibration_name}",
                priority=HVMessagePriority.ACQUISITION,
                timeout_s=client_timeout("set_acquisition_configuration"),
            )

            if config_response.status != MessageStatus.OK:
                result["error"] = config_response.error or "HV configuration failed"
                self.logger.error(f"{calibration_name} HV configuration failed: {result['error']}")
                return result

            config_result = config_response.result or {}

            configured_channels = hv_channels & set(config_result.get("successful_channels") or [])
            configured_channels -= set(config_result.get("failed_channels") or []) | set(config_result.get("skipped_channels") or [])

            if not configured_channels:
                result["error"] = "HV configuration failed for all selected channels"
                self.logger.error(f"{calibration_name}: {result['error']}: {config_result}")
                return result

            if configured_channels != hv_channels:
                self.logger.warning(f"{calibration_name} HV configuration incomplete: {config_result}")

            on_response = self.runtime.hv_service._submit_command(
                command="hv_on_and_wait",
                payload={"channels": sorted(configured_channels)},
                sender=f"client_calibration_{calibration_name}",
                priority=HVMessagePriority.ACQUISITION,
                timeout_s=client_timeout("hv_on_and_wait"),
            )

            if on_response.status != MessageStatus.OK:
                result["error"] = on_response.error or "HV power-on failed"
                self.logger.error(f"{calibration_name} HV power-on failed: {result['error']}")
                return result

            on_result = on_response.result or {}

            ready_hv_channels = configured_channels & set(on_result.get("successful_channels") or [])
            ready_hv_channels -= set(on_result.get("failed_channels") or []) | set(on_result.get("skipped_channels") or [])

            if not ready_hv_channels:
                result["error"] = "HV power-on failed for all configured channels"
                self.logger.error(f"{calibration_name}: {result['error']}: {on_result}")
                return result

            if ready_hv_channels != configured_channels:
                self.logger.warning(f"{calibration_name} HV power-on incomplete: {on_result}")

            ready_channels = {hv_channel - 1 for hv_channel in ready_hv_channels}

            result.update(
                success=True,
                successful_channels=sorted(ready_channels),
                excluded_channels=sorted(set(requested_channels) - ready_channels),
                error=None,
            )

            self.logger.info(
                f"{calibration_name} preparation completed: successful_channels={result['successful_channels']}, "
                f"excluded_channels={result['excluded_channels']}, voltage={voltage}, threshold={threshold}"
            )

            return result

        except Exception as exc:
            self.logger.exception(f"Unexpected error while preparing {calibration_name}")
            result.update(success=False, successful_channels=[], excluded_channels=requested_channels, error=str(exc))
            return result

    def prepare_spe(self, channels: list[int], *, voltage: float, threshold: float) -> dict:
        return self.prepare_hv_point(channels, voltage=voltage, threshold=threshold, calibration_name="spe")
