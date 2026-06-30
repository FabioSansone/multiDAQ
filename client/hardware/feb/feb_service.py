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

        channel_list: List[int] = channels_definition(
            channels=channels,
            n_channels=7,
        )

        if not channel_list:
            return {
                "success": False,
                "successful_channels": [],
                "failed_channels": [],
                "error": "No valid channels selected",
            }

        if not self.runtime.ensure_hv_service():
            return {
                "success": False,
                "successful_channels": [],
                "failed_channels": channel_list,
                "error": "HVService unavailable",
            }

        self.runtime.hv_service.set_policy("full_control")
        self.runtime.hv_service.start()

        successful_channels = []
        failed_channels = []

        for ch in channel_list:
            self.logger.info(f"Programming FEB channel {ch}")

            # 1. Reset RC: old reset() logic, but using existing rc_reset
            if not self._submit_rc_command(
                command="rc_reset",
                payload={"channels": "all"},
                timeout_s=30.0,
            ):
                failed_channels.append(ch)
                continue

            time.sleep(0.1)

            # 2. Boot mode: use existing rc_boot
            if not self._submit_rc_command(
                command="rc_boot",
                payload={"channels": str(ch)},
                timeout_s=30.0,
            ):
                failed_channels.append(ch)
                continue

            time.sleep(0.1)

            # 3. Flash FEB
            if not self._flash(
                baud=baud,
                firmware=firmware,
                port=port,
            ):
                failed_channels.append(ch)
                continue

            time.sleep(1.0)

            # 4. Select FEB for Modbus address change:
            #    reset all + write only register 1
            if not self._submit_rc_command(
                command="rc_feb_select_address_change",
                payload={"channels": str(ch)},
                timeout_s=30.0,
            ):
                failed_channels.append(ch)
                continue

            time.sleep(0.5)

            # 5. Change/verify address using HV Modbus
            if not self._submit_hv_command(
                command="feb_change_address",
                payload={
                    "channel_index": ch,
                    "standard_addr": standard_addr,
                },
                timeout_s=90.0,
            ):
                failed_channels.append(ch)
                continue

            successful_channels.append(ch)
            self.logger.info(f"FEB channel {ch} programmed successfully")

        if successful_channels:
            # Old init_acq starts with reg17 = 0.
            reg17_ok = self._submit_rc_command(
                command="rc_write_register",
                payload={
                    "address": 17,
                    "value": 0,
                },
                timeout_s=30.0,
            )

            data_mode_ok = reg17_ok and self._submit_rc_command(
                command="rc_acq_start",
                payload={
                    "channels": ",".join(str(ch) for ch in successful_channels),
                },
                timeout_s=30.0,
            )
        else:
            data_mode_ok = False

        return {
            "success": bool(successful_channels) and data_mode_ok,
            "successful_channels": successful_channels,
            "failed_channels": failed_channels,
            "data_mode_ok": data_mode_ok,
            "firmware": str(self._resolve_firmware_path(firmware)),
            "baud": baud,
            "port": port,
            "standard_addr": standard_addr,
        }