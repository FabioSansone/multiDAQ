from common.constants import ACQUISITION_MODES
from client.utils.logger import get_logger


class AcquisitionService:
    def __init__(self, manager):
        self.manager = manager

        self.logger = get_logger("acquisition_service")
        self.logger.info("Acquisition Service initialized")


    def apply_acquisition_mode(
        self,
        new_mode: str,
        acq_info: dict | None = None,
        pe_thr: int | float | None = None,
    ) -> bool:

        manager = self.manager
        new_mode = new_mode.lower()
        old_mode = getattr(manager, "acq_mode", None)

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

    def _apply_test_mode(self) -> bool:
        manager = self.manager

        if manager.hv_service is not None:
            manager.hv_service._submit_command(
                command="hv_off",
                payload={"channels": "all"},
                sender="client_acquisition_service",
            )
            manager.hv_service.stop()
            manager.hv_service = None

        manager.acq_mode = "test"
        manager.acq_info = None
        manager.start_thr = None

        manager.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        manager.evproducer.start(manager.server_ip)
        return True

    def _apply_calibration_mode(self) -> bool:
        manager = self.manager

        manager.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        if not manager._ensure_hv_service():
            self.logger.error("Cannot apply calibration mode: HVService unavailable")
            return False

        manager.hv_service.start()

        manager.hv_service._submit_command(
            command="set_common_voltage",
            payload={"channels": "all", "common_voltage": 1200},
            sender="client_acquisition_service",
        )

        manager.hv_service._submit_command(
            command="set_common_threshold",
            payload={"channels": "all", "common_threshold": 400},
            sender="client_acquisition_service",
        )

        manager.evproducer.start(manager.server_ip)

        manager.acq_mode = "calibration"
        manager.acq_info = None
        manager.start_thr = None
        return True

    def _apply_multipmt_mode(
        self,
        acq_info: dict | None,
        pe_thr: int | float | None,
    ) -> bool:
        manager = self.manager

        if acq_info is None:
            self.logger.error(
                "Cannot apply multipmt mode: missing acquisition configuration"
            )
            return False

        manager.rc_service._submit_command(
            command="rc_acq_start",
            payload={"channels": "all"},
            sender="client_acquisition_service",
        )

        if not manager._ensure_hv_service():
            self.logger.error("Cannot apply multipmt mode: HVService unavailable")
            return False

        manager.hv_service.start()

        manager.hv_service._submit_command(
            command="set_acquisition_configuration",
            payload={
                "channels": "all",
                "acquisition_configuration": acq_info,
            },
            sender="client_acquisition_service",
        )

        manager.evproducer.start(manager.server_ip)

        manager.acq_mode = "multipmt"
        manager.acq_info = acq_info
        manager.start_thr = pe_thr
        return True
    