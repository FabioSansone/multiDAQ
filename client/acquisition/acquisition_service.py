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
    ) -> bool:

        runtime = self.runtime
        new_mode = new_mode.lower()
        old_mode = runtime.acq_mode

        self.logger.info(
            f"Applying acquisition mode change: {old_mode} -> {new_mode}"
        )

        if new_mode not in ACQUISITION_MODES:
            self.logger.error(f"Unknown acquisition mode: {new_mode}")
            return False

        if new_mode == "test":
            return self._apply_test_mode()

        if new_mode == "calibration":
            return self._apply_calibration_mode()

        if new_mode == "multipmt":
            return self._apply_multipmt_mode(
                acq_info=acq_info,
                pe_thr=pe_thr,
            )

        self.logger.error(f"Unhandled acquisition mode: {new_mode}")
        return False

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

    def _apply_test_mode(self) -> bool:
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
            return False

        if runtime.ensure_hv_service():
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

        runtime.evproducer.start(runtime.server_ip)
        return True

    def _apply_calibration_mode(self) -> bool:
        runtime = self.runtime

        runtime.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        if not runtime.ensure_hv_service():
            self.logger.error("Cannot apply calibration mode: HVService unavailable")
            return False
        
        runtime.hv_service.set_policy("full_control")

        runtime.hv_service.start()

        if not self._submit_hv_command(
            command="set_common_voltage",
            payload={"channels": "all", "common_voltage": 1200},
            timeout_s=35.0,
        ):
            return False

        if not self._submit_hv_command(
            command="set_common_threshold",
            payload={"channels": "all", "common_threshold": 400},
            timeout_s=35.0,
        ):
            return False

        if not self._submit_hv_command(
            command="hv_on",
            payload={"channels": "all"},
            timeout_s=90.0,
        ):
            return False

        runtime.evproducer.start(runtime.server_ip)

        runtime.set_acquisition_mode(
            acq_mode="calibration",
            acq_info=None,
            start_thr=None,
        )

        return True

    def _apply_multipmt_mode(
        self,
        acq_info: dict | None,
        pe_thr: int | float | None,
    ) -> bool:
        runtime = self.runtime

        if acq_info is None:
            self.logger.error(
                "Cannot apply multipmt mode: missing acquisition configuration"
            )
            return False

        runtime.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        if not runtime.ensure_hv_service():
            self.logger.error("Cannot apply multipmt mode: HVService unavailable")
            return False
        
        runtime.hv_service.set_policy("full_control")

        runtime.hv_service.start()

        if not self._submit_hv_command(
            command="set_acquisition_configuration",
            payload={
                "channels": "all",
                "acquisition_configuration": acq_info,
            },
            timeout_s=300.0,
        ):
            return False

        if not self._submit_hv_command(
            command="hv_on",
            payload={"channels": "all"},
            timeout_s=90.0,
        ):
            return False

        runtime.evproducer.start(runtime.server_ip)

        runtime.set_acquisition_mode(
            acq_mode="multipmt",
            acq_info=acq_info,
            start_thr=pe_thr,
        )

        return True