import subprocess
import time
from pathlib import Path
from typing import List

from client.utils.logger import get_logger
from client.utils.channels import channels_definition
from client.hardware.rc.rc_messages import MessageStatus


ADDR_CHANNELS_ENCODING = {
    0: 1,
    1: 2,
    2: 4,
    3: 8,
    4: 16,
    5: 32,
    6: 64,
}


class FEBService:
    def __init__(self, runtime):
        self.runtime = runtime
        self.logger = get_logger("feb_service")

    def _flash(self, baud: int, firmware: str, port: str) -> bool:
        firmware_path = Path(firmware)

        if not firmware_path.exists():
            self.logger.error(f"Firmware file not found: {firmware}")
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
                f"Flashing FEB with firmware={firmware_path}, port={port}, baud={baud}"
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

    def _change_address(self, channel_index: int, standard_addr: int | None) -> bool:
        if self.runtime.hv_service is None:
            self.logger.error("Cannot change FEB address: HVService unavailable")
            return False

        response = self.runtime.hv_service._submit_command(
            command="feb_change_address",
            payload={
                "channel_index": channel_index,
                "standard_addr": standard_addr,
            },
            sender="feb_service",
            timeout_s=60.0,
        )

        if response.status != MessageStatus.OK:
            self.logger.error(f"FEB address change failed: {response.error}")
            return False

        return True

    def program(
        self,
        channels,
        baud: int,
        firmware: str,
        port: str,
        standard_addr: int | None = None,
    ) -> dict:
        self.logger.info(
            f"Starting FEB programming: channels={channels}, baud={baud}, "
            f"firmware={firmware}, port={port}, standard_addr={standard_addr}"
        )

        if not self.runtime.ensure_hv_service():
            return {
                "success": False,
                "successful_channels": [],
                "failed_channels": [],
                "error": "HVService unavailable",
            }

        self.runtime.hv_service.set_policy("full_control")
        self.runtime.hv_service.start()

        channel_list: List[int] = channels_definition(
            channels=channels,
            n_channels=7,
        )

        successful_channels = []
        failed_channels = []

        for ch in channel_list:
            self.logger.info(f"Programming FEB channel {ch}")

            reset_response = self.runtime.rc_service._submit_command(
                command="rc_reset",
                payload={"channels": "all"},
                sender="feb_service",
                timeout_s=30.0,
            )

            if reset_response.status != MessageStatus.OK:
                self.logger.error(f"RC reset failed before programming channel {ch}")
                failed_channels.append(ch)
                continue

            time.sleep(0.1)

            boot_response = self.runtime.rc_service._submit_command(
                command="rc_boot",
                payload={"channels": str(ch)},
                sender="feb_service",
                timeout_s=30.0,
            )

            if boot_response.status != MessageStatus.OK:
                self.logger.error(f"RC boot mode failed for channel {ch}")
                failed_channels.append(ch)
                continue

            time.sleep(0.1)

            if not self._flash(
                baud=baud,
                firmware=firmware,
                port=port,
            ):
                failed_channels.append(ch)
                continue

            time.sleep(1.0)

            if not self._change_address(
                channel_index=ch,
                standard_addr=standard_addr,
            ):
                failed_channels.append(ch)
                continue

            successful_channels.append(ch)
            self.logger.info(f"FEB channel {ch} programmed successfully")

        if successful_channels:
            data_response = self.runtime.rc_service._submit_command(
                command="rc_acq_start",
                payload={
                    "channels": ",".join(str(ch) for ch in successful_channels)
                },
                sender="feb_service",
                timeout_s=30.0,
            )

            data_mode_ok = data_response.status == MessageStatus.OK
        else:
            data_mode_ok = False

        return {
            "success": bool(successful_channels) and data_mode_ok,
            "successful_channels": successful_channels,
            "failed_channels": failed_channels,
            "data_mode_ok": data_mode_ok,
            "firmware": firmware,
            "baud": baud,
            "port": port,
            "standard_addr": standard_addr,
        }