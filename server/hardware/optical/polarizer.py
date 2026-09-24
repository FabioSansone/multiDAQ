from pathlib import Path

from pylablib.devices import Thorlabs

from server.utils.logger import get_logger


logger = get_logger("polarizer")

POSITION_TOLERANCE = 1e-3


class Polarizer:

    def __init__(
        self,
        device_path: str | Path,
    ) -> None:

        self.device_path = Path(device_path)
        self._motor = None

    def open(self) -> bool:

        if self._motor is not None:
            logger.warning(
                f"Polarizer already open: {self.device_path}"
            )
            return True

        try:
            serial_port = self.device_path.resolve()

            self._motor = Thorlabs.KinesisMotor(
                str(serial_port),
                scale="stage",
            )

        except Exception as exc:
            logger.exception(
                "Failed to initialize polarizer "
                f"on {self.device_path}: {exc}"
            )

            self._motor = None
            return False

        logger.info(
            "Polarizer initialized: "
            f"persistent_path={self.device_path}, "
            f"serial_port={serial_port}"
        )

        return True

    def is_open(self) -> bool:
        return self._motor is not None

    def get_position(self) -> float | None:

        if self._motor is None:
            logger.error(
                "Cannot read polarizer position: "
                "device is not open"
            )
            return None

        try:
            return float(
                self._motor.get_position(
                    scale=True
                )
            )

        except Exception as exc:
            logger.error(
                "Failed to read polarizer position "
                f"from {self.device_path}: {exc}"
            )
            return None

    def get_stage_info(self):

        if self._motor is None:
            logger.error(
                "Cannot read polarizer stage info: "
                "device is not open"
            )
            return None

        try:
            return self._motor.get_stage()

        except Exception as exc:
            logger.error(
                "Failed to read polarizer stage info "
                f"from {self.device_path}: {exc}"
            )
            return None

    def get_homing_parameters(self):

        if self._motor is None:
            logger.error(
                "Cannot read polarizer homing parameters: "
                "device is not open"
            )
            return None

        try:
            return self._motor.get_homing_parameters(
                scale=True
            )

        except Exception as exc:
            logger.error(
                "Failed to read polarizer homing parameters "
                f"from {self.device_path}: {exc}"
            )
            return None

    def get_info(self) -> dict | None:

        if self._motor is None:
            logger.error(
                "Cannot read polarizer info: "
                "device is not open"
            )
            return None

        stage = self.get_stage_info()
        position = self.get_position()
        homing_parameters = self.get_homing_parameters()

        if position is None:
            return None

        return {
            "stage": stage,
            "current_position": position,
            "homing_parameters": homing_parameters,
            "device_path": str(self.device_path),
        }

    def move_to(
        self,
        position: float,
    ) -> bool:

        if self._motor is None:
            logger.error(
                "Cannot move polarizer: "
                "device is not open"
            )
            return False

        try:
            target_position = float(position)

        except (TypeError, ValueError):
            logger.error(
                f"Invalid polarizer position: {position!r}"
            )
            return False

        current_position = self.get_position()

        if current_position is None:
            return False

        if (
            abs(current_position - target_position)
            <= POSITION_TOLERANCE
        ):
            logger.debug(
                "Polarizer already at requested position: "
                f"requested={target_position}, "
                f"readback={current_position}"
            )
            return True

        try:
            self._motor.move_to(
                target_position,
                scale=True,
            )

            self._motor.wait_move()

        except Exception as exc:
            logger.error(
                "Failed to move polarizer "
                f"{self.device_path} "
                f"to position {target_position}: {exc}"
            )
            return False

        final_position = self.get_position()

        if final_position is None:
            return False

        position_error = abs(
            final_position - target_position
        )

        if position_error > POSITION_TOLERANCE:
            logger.error(
                "Polarizer movement verification failed: "
                f"requested={target_position}, "
                f"readback={final_position}, "
                f"error={position_error}"
            )
            return False

        logger.info(
            "Polarizer moved successfully: "
            f"device={self.device_path}, "
            f"requested={target_position}, "
            f"readback={final_position}"
        )

        return True

    def home(self) -> bool:

        if self._motor is None:
            logger.error(
                "Cannot home polarizer: "
                "device is not open"
            )
            return False

        try:
            self._motor.home(
                sync=True,
            )

        except Exception as exc:
            logger.error(
                "Failed to home polarizer "
                f"{self.device_path}: {exc}"
            )
            return False

        logger.info(
            f"Polarizer homed successfully: "
            f"{self.device_path}"
        )

        return True

    def close(self) -> bool:

        if self._motor is None:
            return True

        try:
            self._motor.close()

        except Exception as exc:
            logger.warning(
                "Failed to close polarizer "
                f"{self.device_path}: {exc}"
            )
            return False

        self._motor = None

        logger.info(
            f"Polarizer closed: {self.device_path}"
        )

        return True