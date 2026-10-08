from common.constants import ACQUISITION_MODES
from common.message_handler import MessageStatus
from client.utils.logger import get_logger


class AcquisitionService:
    def __init__(self, runtime):
        self.runtime = runtime

        self.logger = get_logger("acquisition_service")
        self.logger.info("AcquisitionService initialized")

    def apply_acquisition_mode(
        self,
        new_mode: str,
        acq_info: dict | None = None,
        pe_thr: int | float | None = None,
        fixed_bad_channels: list[int] | None = None,
    ) -> dict:

        runtime = self.runtime
        new_mode = new_mode.lower()
        old_mode = runtime.acq_mode
        fixed_bad_channels = fixed_bad_channels or []

        self.logger.info(
            f"Applying acquisition mode change: {old_mode} -> {new_mode}"
        )

        if new_mode not in ACQUISITION_MODES:
            self.logger.error(f"Unknown acquisition mode: {new_mode}")
            return {"success": False, "missing_serial_channels": []}

        if new_mode == "test":
            return self._apply_test_mode(fixed_bad_channels=fixed_bad_channels)
        if new_mode == "calibration":
            return self._apply_calibration_mode(acq_info=acq_info, fixed_bad_channels=fixed_bad_channels)
        if new_mode == "multipmt":
            return self._apply_multipmt_mode(acq_info=acq_info, pe_thr=pe_thr, fixed_bad_channels=fixed_bad_channels)
        
        self.logger.error(f"Unhandled acquisition mode: {new_mode}")
        return {"success": False, "missing_serial_channels": []}

    def _submit_hv_command(
        self,
        command: str,
        payload: dict,
        timeout_s: float,
    ) -> bool:
        runtime = self.runtime

        if runtime.hv_service is None:
            self.logger.error(
                f"Cannot execute HV command {command}: HVService unavailable"
            )
            return False

        response = runtime.hv_service._submit_command(
            command=command,
            payload=payload,
            sender="client_acquisition_service",
            timeout_s=timeout_s,
        )

        if response.status != MessageStatus.OK:
            self.logger.error(
                f"HV command {command} failed: {response.error}"
            )
            return False

        return True

    def _prepare_hv_service(
        self,
        fixed_bad_channels: list[int],
    ) -> bool:

        runtime = self.runtime

        fixed_bad_hv = [
            ch + 1
            for ch in fixed_bad_channels
        ]

        if runtime.hv_service is None:
            return runtime.ensure_hv_service(
                fixed_bad_channels=fixed_bad_hv,
            )

        hv = runtime.hv_service.hv

        old_fixed = set(hv.getFixedBad())
        new_fixed = set(fixed_bad_hv)

        newly_fixed = new_fixed - old_fixed

        newly_fixed_on = sorted(
            newly_fixed & set(hv.getOnChannels())
        )

        if newly_fixed_on:
            if not self._submit_hv_command(
                command="hv_off_and_wait",
                payload={
                    "channels": newly_fixed_on,
                    "timeout_s": 90.0,
                    "poll_s": 2.0,
                },
                timeout_s=120.0,
            ):
                self.logger.error(
                    "Cannot power off channels becoming FIXED BAD"
                )
                return False

        hv.set_fixed_bad_channels(
            fixed_bad_hv
        )

        if not self._submit_hv_command(
            command="set_hv_sync",
            payload={"channels": "all"},
            timeout_s=90.0,
        ):
            self.logger.error(
                "Cannot synchronize HV state after "
                "fixed bad channel update"
            )
            return False

        return True
    
    def _apply_test_mode(self, fixed_bad_channels: list[int],) -> dict:
        runtime = self.runtime

        
        rc_response = runtime.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        if rc_response.status != MessageStatus.OK:
            self.logger.error(
                f"Cannot apply test mode: failed to enable RC channels: "
                f"{rc_response.error}"
            )
            return {"success": False, "missing_serial_channels": []}

        if self._prepare_hv_service(fixed_bad_channels=fixed_bad_channels):
            runtime.hv_service.set_policy("monitor_only")
            runtime.hv_service.start()
        else:
            self.logger.warning(
                "Test mode applied without HVService. "
                "Acquisition will use RC fallback if needed."
            )

        runtime.set_acquisition_mode(
            acq_mode="test",
            acq_info=None,
            start_thr=None,
        )

        runtime.evproducer.start(runtime.server_ip, runtime.get_mac_to_id())
        return {"success": True, "missing_serial_channels": []}

    def _apply_calibration_mode(self, acq_info: dict | None, fixed_bad_channels: list[int],) -> dict:

        if acq_info is None:
            self.logger.error(
                "Cannot apply multipmt mode: missing acquisition configuration"
            )
            return {"success": False, "missing_serial_channels": []}

        runtime = self.runtime


        rc_response = runtime.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        if rc_response.status != MessageStatus.OK:
            self.logger.error(
                f"Cannot apply calibration mode: failed to enable RC channels: "
                f"{rc_response.error}"
            )
            return {
                "success": False,
                "missing_serial_channels": [],
            }

        if not self._prepare_hv_service(fixed_bad_channels):
            self.logger.error(
                "Cannot apply calibration mode: "
                "HVService unavailable"
            )
            return {
                "success": False,
                "missing_serial_channels": [],
            }
        
        runtime.hv_service.set_policy("full_control")

        runtime.hv_service.start()
        
        missing_result = runtime.hv_service.hv.check_missing_serial(
                runtime.hv_service.hv.getOkChannels()
            )
        
        missing_serial_channels = missing_result.get("missing_serial_channels", [])
        
        if not self._submit_hv_command(
            command="set_acquisition_configuration",
            payload={
                "channels": "all",
                "acquisition_configuration": acq_info,
            },
            timeout_s=300.0,
        ):
            return {
                "success": False,
                "missing_serial_channels": missing_serial_channels,
            }

        if not self._submit_hv_command(
            command="hv_on",
            payload={"channels": "all"},
            timeout_s=90.0,
        ):
            return {"success": False, "missing_serial_channels": missing_serial_channels}

        runtime.evproducer.start(runtime.server_ip, runtime.get_mac_to_id())

        runtime.set_acquisition_mode(
            acq_mode="calibration",
            acq_info=acq_info,
            start_thr=None,
        )

        return {"success": True, "missing_serial_channels": missing_serial_channels}

    def _apply_multipmt_mode(
        self,
        acq_info: dict | None,
        pe_thr: int | float | None,
        fixed_bad_channels: list[int],
    ) -> dict:
        runtime = self.runtime


        if acq_info is None:
            self.logger.error(
                "Cannot apply multipmt mode: missing acquisition configuration"
            )
            return {"success": False, "missing_serial_channels": []}

        rc_response = runtime.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        if rc_response.status != MessageStatus.OK:
            self.logger.error(
                f"Cannot apply calibration mode: failed to enable RC channels: "
                f"{rc_response.error}"
            )
            return {
                "success": False,
                "missing_serial_channels": [],
            }

        if not self._prepare_hv_service(fixed_bad_channels):
            self.logger.error(
                "Cannot apply calibration mode: "
                "HVService unavailable"
            )
            return {
                "success": False,
                "missing_serial_channels": [],
            }
        
        runtime.hv_service.set_policy("full_control")

        runtime.hv_service.start()
        
        missing_result = runtime.hv_service.hv.check_missing_serial(
                runtime.hv_service.hv.getOkChannels()
            )
        
        missing_serial_channels = missing_result.get("missing_serial_channels", [])
        
        if not self._submit_hv_command(
            command="set_acquisition_configuration",
            payload={
                "channels": "all",
                "acquisition_configuration": acq_info,
            },
            timeout_s=300.0,
        ):
            return {"success": False, "missing_serial_channels": missing_serial_channels}
        
        self.runtime.store_multipmt_hv_parameters(acq_info=acq_info)

        if not self._submit_hv_command(
            command="hv_on",
            payload={"channels": "all"},
            timeout_s=90.0,
        ):
            return {"success": False, "missing_serial_channels": missing_serial_channels}

        runtime.evproducer.start(runtime.server_ip, runtime.get_mac_to_id())

        runtime.set_acquisition_mode(
            acq_mode="multipmt",
            acq_info=acq_info,
            start_thr=pe_thr,
        )

        if runtime.hv_service is not None:
            runtime.hv_service.start_acq_check(
                get_hv_parameters=lambda: runtime.channel_hv_parameters
            )
        return {"success": True, "missing_serial_channels": missing_serial_channels}