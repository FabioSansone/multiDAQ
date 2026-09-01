from pathlib import Path

from client.utils.logger import get_logger

IIO_DEVICES_PATH = Path("/sys/bus/iio/devices")


class XADC:


    def __init__(self):
        self.logger = get_logger("xadc")

        self.device_path = self._find_device()

        if self.device_path is None:
            self.logger.error("XADC IIO device not found")
        else:
            self.logger.info(f"XADC detected at {self.device_path}")

    def _find_device(self) -> Path | None:

        if not IIO_DEVICES_PATH.exists():
            return None

        for device in IIO_DEVICES_PATH.glob("iio:device*"):

            name_path = device / "name"

            try:
                name = (name_path.read_text().strip().lower())
            except Exception:
                continue

            if name == "xadc":
                return device

        return None
    
    @property
    def available(self) -> bool:
        return self.device_path is not None


    @staticmethod
    def _read_float(path: Path) -> float:
        return float(path.read_text().strip())

    def get_temperature(self,) -> float | None:
        if self.device_path is None:
            return None

        try:
            raw = self._read_float(self.device_path / "in_temp0_raw")
            offset = self._read_float(self.device_path / "in_temp0_offset")
            scale = self._read_float(self.device_path / "in_temp0_scale")

            temperature_c = ((raw + offset) * scale) / 1000.0

            return round(temperature_c, 2)

        except Exception as e:
            self.logger.error(f"Failed to read FPGA temperature from XADC: {e}")
            return None

    def close(self) -> None:
        pass


            

        