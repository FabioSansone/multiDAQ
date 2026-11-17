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
        success is True if at least one channel is ready. An excluded channel may be left
        powered (at the previous voltage, or half way), so it is forced off here, and the
        result says which ones are confirmed off.

        Returns {"success": bool, "successful_channels": [...], "excluded_channels": [...],
        "forced_off_channels": [...], "not_off_channels": [...], "error": str | None},
        with logical channels, serializable. not_off_channels: excluded channels that are
        not confirmed off (force-off failed or no answer).
        """

        result = self._configure_and_power_on(
            channels,
            voltage=voltage,
            threshold=threshold,
            calibration_name=calibration_name,
        )

        # Only after the HV was actually asked something: the guards (mode, no HVService, no
        # channels) return with nothing excluded by the hardware.
        hv_was_commanded = result.pop("_hv_commanded", False)

        if result["excluded_channels"] and hv_was_commanded:
            self._force_off_excluded(result, calibration_name)

        return result

    def _force_off_excluded(self, result: dict, calibration_name: str) -> None:
        """Best effort: hv_force_off of the excluded channels (it also acts on the channels that
        are not OK). Fills forced_off_channels and not_off_channels of the result."""

        excluded_channels = list(result["excluded_channels"])
        result["not_off_channels"] = excluded_channels

        try:
            hv_channels = channels_definition(excluded_channels, hv_channels=True)

            self.logger.warning(f"{calibration_name}: forcing OFF the excluded channels {excluded_channels}")

            response = self.runtime.hv_service._submit_command(
                command="hv_force_off",
                payload={"channels": hv_channels},
                sender=f"client_calibration_{calibration_name}",
                priority=HVMessagePriority.ACQUISITION,
                timeout_s=client_timeout("hv_force_off"),
            )

            if response.status != MessageStatus.OK:
                self.logger.error(f"{calibration_name}: forced HV off of {excluded_channels} failed: {response.error}")
                return

            failed_hv_channels = set((response.result or {}).get("failed_channels") or [])
            not_off = sorted(hv_channel - 1 for hv_channel in failed_hv_channels)

            if not_off:
                self.logger.error(f"{calibration_name}: HV channels that could not be forced OFF: {not_off}")

            result["not_off_channels"] = [channel for channel in excluded_channels if channel in not_off]
            result["forced_off_channels"] = [channel for channel in excluded_channels if channel not in not_off]

        except Exception:
            self.logger.exception(f"{calibration_name}: cannot force OFF the excluded channels {excluded_channels}")

    def _configure_and_power_on(
        self,
        channels: list[int],
        *,
        voltage: float,
        threshold: float,
        calibration_name: str,
    ) -> dict:
        """Configuration and power on of the channels (see prepare_hv_point); no force-off here."""

        requested_channels = sorted(set(channels))

        result = {
            "success": False,
            "successful_channels": [],
            "excluded_channels": requested_channels,
            "forced_off_channels": [],
            "not_off_channels": [],
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

            result["_hv_commanded"] = True  # from here on the channels may have been touched

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

    def prepare_gain(self, channels: list[int], *, voltage: float, threshold: float) -> dict:
        return self.prepare_hv_point(channels, voltage=voltage, threshold=threshold, calibration_name="gain")

    def prepare_polarizer(self, channels: list[int], *, voltage: float, threshold: float) -> dict:
        return self.prepare_hv_point(channels=channels, voltage=voltage, threshold=threshold, calibration_name="polarizer")
