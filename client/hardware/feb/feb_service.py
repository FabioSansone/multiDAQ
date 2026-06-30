import subprocess
import time
from pathlib import Path
from typing import List

from client.utils.logger import get_logger
from client.utils.channels import channels_definition
from common.message_handler import MessageStatus


class FEBService:
    DEFAULT_FIRMWARE_NAMES = [
        "HKL031V4B.hex",
        "HKL031V4C.hex",
        "HKL031V4A.hex",
    ]

    def __init__(self, runtime):
        self.runtime = runtime
        self.logger = get_logger("feb_service")
        self.service_dir = Path(__file__).parent

    def _resolve_firmware_path(self, firmware: str | None) -> Path | None:
        if firmware is not None:
            path = Path(firmware).expanduser()

            if not path.is_absolute():
                path = self.service_dir / path

            if path.exists():
                return path

            return None

        for name in self.DEFAULT_FIRMWARE_NAMES:
            candidate = self.service_dir / name
            if candidate.exists():
                return candidate

        return None

    def _flash(self, baud: int, firmware: str | None, port: str) -> bool:
        firmware_path = self._resolve_firmware_path(firmware)

        if firmware_path is None:
            self.logger.error(
                f"Firmware not found. Provide --firmware or place one of "
                f"{self.DEFAULT_FIRMWARE_NAMES} in {self.service_dir}"
            )
            return False

        command = [
            "stm32flash",
            "-b", str(baud),
            "-w", str(firmware_path),
            "-e", "255",
            "-v",
            port,
        ]

        try:
            self.logger.info(
                f"Flashing FEB with firmware={firmware_path}, "
                f"port={port}, baud={baud}"
            )
            subprocess.run(command, check=True)
            time.sleep(0.5)
            return True

        except subprocess.CalledProcessError as e:
            self.logger.error(f"FEB flashing failed: {e}")
            return False

        except Exception as e:
            self.logger.error(f"Unexpected error during FEB flashing: {e}")
            return False

    def _submit_rc_command(
        self,
        command: str,
        payload: dict,
        timeout_s: float = 30.0,
    ) -> bool:
        response = self.runtime.rc_service._submit_command(
            command=command,
            payload=payload,
            sender="feb_service",
            timeout_s=timeout_s,
        )

        if response.status != MessageStatus.OK:
            self.logger.error(
                f"RC command {command} failed: {response.error}"
            )
            return False

        return True

    def _submit_hv_command(
        self,
        command: str,
        payload: dict,
        timeout_s: float = 60.0,
    ) -> bool:
        if self.runtime.hv_service is None:
            self.logger.error("HVService unavailable")
            return False

        response = self.runtime.hv_service._submit_command(
            command=command,
            payload=payload,
            sender="feb_service",
            timeout_s=timeout_s,
        )

        if response.status != MessageStatus.OK:
            self.logger.error(
                f"HV command {command} failed: {response.error}"
            )
            return False

        return True
    
    def _restore_after_programming(self, successful_channels: list[int]) -> tuple[bool, bool]:
        """
        Restore RC/HV state after FEB programming according to acquisition mode.

        Returns
        -------
        data_mode_ok, hv_restore_ok
        """

        if not successful_channels:
            return False, False

        data_mode_ok = self._submit_rc_command(
            command="rc_acq_start",
            payload={
                "channels": ",".join(str(ch) for ch in successful_channels),
            },
            timeout_s=30.0,
        )

        if not data_mode_ok:
            return False, False

        mode = self.runtime.acq_mode
        successful_hv_channels = [ch + 1 for ch in successful_channels]

        if mode == "test":
            self.logger.info(
                "Test mode: FEB programmed channels returned to data mode, "
                "but HV channels will remain OFF."
            )
            return data_mode_ok, True

        if mode == "calibration":
            self.logger.info(
                "Calibration mode: restoring common calibration HV settings "
                "before switching channels ON."
            )

            if not self._submit_hv_command(
                command="set_common_voltage",
                payload={
                    "channels": successful_hv_channels,
                    "common_voltage": 1200,
                },
                timeout_s=35.0,
            ):
                return data_mode_ok, False

            if not self._submit_hv_command(
                command="set_common_threshold",
                payload={
                    "channels": successful_hv_channels,
                    "common_threshold": 400,
                },
                timeout_s=35.0,
            ):
                return data_mode_ok, False

            hv_restore_ok = self._submit_hv_command(
                command="hv_on_and_wait",
                payload={"channels": successful_hv_channels},
                timeout_s=300.0,
            )

            return data_mode_ok, hv_restore_ok

        if mode == "multipmt":
            self.logger.info(
                "multiPMT mode: restoring acquisition configuration from handshake "
                "before switching channels ON."
            )

            if not self.runtime.acq_info:
                self.logger.error(
                    "Cannot restore multiPMT HV state: missing acquisition configuration"
                )
                return data_mode_ok, False

            if not self._submit_hv_command(
                command="set_acquisition_configuration",
                payload={
                    "channels": successful_hv_channels,
                    "acquisition_configuration": self.runtime.acq_info,
                },
                timeout_s=300.0,
            ):
                return data_mode_ok, False

            hv_restore_ok = self._submit_hv_command(
                command="hv_on_and_wait",
                payload={"channels": successful_hv_channels},
                timeout_s=300.0,
            )

            return data_mode_ok, hv_restore_ok

        self.logger.warning(
            f"Unknown acquisition mode after FEB programming: {mode}. "
            "Leaving HV channels OFF."
        )

        return data_mode_ok, True

    def program(
        self,
        channels,
        baud: int,
        firmware: str | None = None,
        port: str = "/dev/ttyPS1",
        standard_addr: int | None = None,
    ) -> dict:

        self.logger.info(
            f"Starting FEB programming: channels={channels}, baud={baud}, "
            f"firmware={firmware}, port={port}, standard_addr={standard_addr}"
        )

        channel_list = channels_definition(
            channels=channels,
            n_channels=7,
        )

        if not channel_list:
            return {
                "success": False,
                "successful_channels": [],
                "failed_channels": [],
                "skipped_bad_channels": [],
                "error": "No valid RC channels selected",
            }

        if self.runtime.hv_service is None:
            return {
                "success": False,
                "successful_channels": [],
                "failed_channels": channel_list,
                "skipped_bad_channels": [],
                "error": (
                    "HVService unavailable. Apply acquisition mode before "
                    "programming FEB."
                ),
            }

        hv = self.runtime.hv_service.hv

        successful_channels = []
        failed_channels = []
        skipped_bad_channels = []

        ok_on_hv_channels = sorted(
            set(hv.getOkChannels()) & set(hv.getOnChannels())
        )

        if ok_on_hv_channels:
            self.logger.info(
                f"Switching OFF all known OK+ON HV channels before FEB programming: "
                f"{ok_on_hv_channels}"
            )

            if not self._submit_hv_command(
                command="hv_off_and_wait",
                payload={
                    "channels": ok_on_hv_channels,
                    "timeout_s": 120.0,
                    "poll_s": 2.0,
                },
                timeout_s=150.0,
            ):
                return {
                    "success": False,
                    "successful_channels": [],
                    "failed_channels": channel_list,
                    "skipped_bad_channels": [],
                    "data_mode_ok": False,
                    "hv_restore_ok": False,
                    "acq_mode": self.runtime.acq_mode,
                    "error": (
                        "Failed to switch OFF known OK+ON HV channels "
                        "before FEB programming"
                    ),
                }

        for ch in channel_list:
            hv_ch = ch + 1

            self.logger.info(
                f"Programming FEB on RC channel {ch}; "
                f"expected final HV address {hv_ch}"
            )

            if not self._submit_rc_command(
                command="rc_reset",
                payload={"channels": "all"},
                timeout_s=30.0,
            ):
                self.logger.error(
                    f"Failed to reset RC before boot mode for FEB channel {ch}"
                )
                failed_channels.append(ch)
                hv.moveToBad(hv_ch)
                continue

            time.sleep(0.1)

            if not self._submit_rc_command(
                command="rc_boot",
                payload={"channels": str(ch)},
                timeout_s=30.0,
            ):
                self.logger.error(f"Failed to put FEB channel {ch} in boot mode")
                failed_channels.append(ch)
                hv.moveToBad(hv_ch)
                continue

            time.sleep(0.1)

            if not self._flash(
                baud=baud,
                firmware=firmware,
                port=port,
            ):
                self.logger.error(f"Firmware flash failed for FEB channel {ch}")
                failed_channels.append(ch)
                hv.moveToBad(hv_ch)
                continue

            time.sleep(1.0)

            if not self._submit_rc_command(
                command="rc_feb_reset_after_flash",
                payload={},
                timeout_s=30.0,
            ):
                self.logger.error(f"Post-flash RC reset failed for FEB channel {ch}")
                failed_channels.append(ch)
                hv.moveToBad(hv_ch)
                continue

            time.sleep(0.1)

            if not self._submit_rc_command(
                command="rc_feb_select_address_change",
                payload={"channels": str(ch)},
                timeout_s=30.0,
            ):
                self.logger.error(
                    f"Failed to select FEB channel {ch} for address change"
                )
                failed_channels.append(ch)
                hv.moveToBad(hv_ch)
                continue

            time.sleep(0.5)

            if not self._submit_hv_command(
                command="feb_change_address",
                payload={
                    "channel_index": ch,
                    "standard_addr": standard_addr,
                },
                timeout_s=90.0,
            ):
                self.logger.error(
                    f"Address change/verification failed for FEB channel {ch}"
                )
                failed_channels.append(ch)
                hv.moveToBad(hv_ch)
                continue

            successful_channels.append(ch)

            hv.moveToOk(hv_ch)
            hv.moveToOff(hv_ch)

            self.logger.info(
                f"FEB channel {ch} programmed successfully; "
                f"expected HV address is now {hv_ch}"
            )

        for ch in failed_channels:
            hv.moveToBad(ch + 1)

        data_mode_ok, hv_restore_ok = self._restore_after_programming(
            successful_channels=successful_channels,
        )

        return {
            "success": bool(successful_channels) and data_mode_ok and hv_restore_ok,
            "successful_channels": successful_channels,
            "failed_channels": failed_channels,
            "skipped_bad_channels": skipped_bad_channels,
            "data_mode_ok": data_mode_ok,
            "hv_restore_ok": hv_restore_ok,
            "acq_mode": self.runtime.acq_mode,
            "firmware": str(self._resolve_firmware_path(firmware)),
            "baud": baud,
            "port": port,
            "standard_addr": standard_addr,
        }