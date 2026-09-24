from pathlib import Path
import json


from server.utils.logger import get_logger
from server.hardware.optical.device_discovery import resolve_serial_device
from server.hardware.optical.filter_wheel import FilterWheel
from server.hardware.optical.polarizer import Polarizer


class OpticalInstrumentService:

    POSSIBLE_OPTICAL_CONF_FILE_PATH = [
                Path(__file__).parent.parent / "multipmt_config_files" / "optical_devices.json",
                Path.home() / "multiPMT" / "multipmt_config_files" / "optical_devices.json",
                Path("swgo/multiPMT/multipmt_config_files/optical_devices.json"),
            ]

    def __init__(self):


        self.logger = get_logger("optical_service")
        self.logger.debug("Optical Instrument Service Initialized")

        self.optical_path = self._find_optical_path()

        self.near_wheel = None
        self.far_wheel = None
        self.polarizer = None

        self.device_config = {}

        


    def _find_optical_path(self) -> Path | None:
        path = None
        for path_try in self.POSSIBLE_OPTICAL_CONF_FILE_PATH:
            if path_try.exists():
                path = path_try
                break
                
        if path is None:
            self.logger.warning(
                "No optical device configuration file found "
                "among configured paths"
            )
            return None

        return path

    def _load_config(self) -> bool:
        if self.optical_path is None:
            self.logger.error(
                "Optical device configuration file not found: "
                f"{self.optical_path}"
            )
            return False
        try:
            with self.optical_path.open("r", encoding="utf-8") as config_file:
                config = json.load(config_file)
        except Exception as e:
            self.logger.error(
                "Failed to load optical device configuration: "
                f"{e}"
            )

            return False

        if not isinstance(config, dict):
            self.logger.error(
                "Invalid optical device configuration: "
                "top-level JSON object must be a dictionary"
            )

            return False

        self.device_config = config
        self.logger.info(
            "Optical device configuration loaded"
        )
        return True


    def _resolve_device_paths(self) -> dict[str, Path] | None:

        required_devices = ("near_wheel", "far_wheel", "polarizer")

        resolved: dict[str, Path] = {}

        for role in required_devices:
            device_config = self.device_config.get(role)

            if not isinstance(device_config, dict):
                self.logger.error(
                    f"Missing optical device configuration "
                    f"for role {role!r}"
                )
                return None

            serial = device_config.get("serial")

            if not isinstance(serial, str) or not serial.strip():
                self.logger.error(
                    f"Missing serial for optical device "
                    f"{role!r}"
                )

                return None

            serial = serial.strip()
            path = resolve_serial_device(serial=serial)

            if path is None:
                self.logger.error(
                    f"Cannot resolve optical device "
                    f"{role!r} with serial {serial!r}"
                )

                return None

            resolved[role] = path

        return resolved


    def resolve_configured_devices(
        self,
    ) -> dict[str, Path] | None:

        if not self._load_config():
            return None

        resolved = self._resolve_device_paths()

        if resolved is None:
            return None

        return resolved


    def initialize(self) -> bool:

        if self.far_wheel is not None or self.near_wheel is not None or self.polarizer is not None:
            self.logger.warning(
                "Optical instruments already initialized"
            )
            return True

        if not self._load_config():
            return False

        device_paths = self._resolve_device_paths()
        if device_paths is None:
            return False

        self.near_wheel = FilterWheel(
            device_paths["near_wheel"]
        )

        self.far_wheel = FilterWheel(
            device_paths["far_wheel"]
        )

        self.polarizer = Polarizer(
            device_paths["polarizer"]
        )

        if not self.near_wheel.open():
            self.logger.error(
                "Failed to initialize near filter wheel"
            )
            self.close()
            return False

        if not self.far_wheel.open():
            self.logger.error(
                "Failed to initialize far filter wheel"
            )
            self.close()
            return False

        if not self.polarizer.open():
            self.logger.error(
                "Failed to initialize polarizer"
            )
            self.close()
            return False

        self.logger.info(
            "Optical instruments initialized successfully"
        )

        return True


    def close(self) -> bool:
        success = True

        if self.near_wheel is not None:

            if not self.near_wheel.close():
                success = False

            self.near_wheel = None

        if self.far_wheel is not None:

            if not self.far_wheel.close():
                success = False

            self.far_wheel = None

        if self.polarizer is not None:

            if not self.polarizer.close():
                success = False

            self.polarizer = None

        if success:
            self.logger.info(
                "Optical instruments closed successfully"
            )
        else:
            self.logger.warning(
                "One or more optical instruments "
                "failed to close cleanly"
            )

        return success


    def move_wheel(
        self,
        position: int,
        which_wheel: str = "near",
    ) -> bool:

        if not isinstance(position, int):
            self.logger.error(
                f"Invalid filter wheel position: {position!r}"
            )
            return False

        if which_wheel == "near":
            wheel = self.near_wheel

        elif which_wheel == "far":
            wheel = self.far_wheel

        else:
            self.logger.error(
                f"Invalid filter wheel selection: {which_wheel!r}"
            )
            return False

        if wheel is None:
            self.logger.error(
                f"Cannot move {which_wheel} filter wheel: "
                "optical instruments are not initialized"
            )
            return False

        if not wheel.move_to(position=position):
            self.logger.error(
                f"Failed to move {which_wheel} filter wheel "
                f"to position {position}"
            )
            return False

        self.logger.info(
            f"{which_wheel.capitalize()} filter wheel moved "
            f"to position {position}"
        )

        return True


    def move_polarizer(
        self,
        position: float,
    ) -> bool:

        if self.polarizer is None:
            self.logger.error(
                "Cannot move polarizer: "
                "optical instruments are not initialized"
            )
            return False

        if not self.polarizer.move_to(position=position):
            self.logger.error(
                f"Failed to move polarizer "
                f"to position {position}"
            )
            return False

        self.logger.info(
            f"Polarizer moved to position {position}"
        )

        return True


    def get_status(self) -> dict:

        status = {
            "initialized": False,
            "near_wheel": None,
            "far_wheel": None,
            "polarizer": None,
        }

        if (
            self.near_wheel is None
            or self.far_wheel is None
            or self.polarizer is None
        ):
            return status

        status["near_wheel"] = (
            self.near_wheel.get_info()
        )

        status["far_wheel"] = (
            self.far_wheel.get_info()
        )

        status["polarizer"] = (
            self.polarizer.get_info()
        )

        status["initialized"] = (
            status["near_wheel"] is not None
            and status["far_wheel"] is not None
            and status["polarizer"] is not None
        )

        return status

    
            

        