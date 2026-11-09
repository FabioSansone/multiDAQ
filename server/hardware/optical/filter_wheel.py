from pathlib import Path

from pylablib.devices import Thorlabs

from server.utils.logger import get_logger


logger = get_logger("filter_wheel")


class FilterWheel:

    def __init__(
        self,
        device_path: str | Path,
    ) -> None:

        self.device_path = Path(device_path)

        self._wheel = None

    def open(self) -> bool:

        if self._wheel is not None:
            logger.warning(
                f"Filter wheel already open: "
                f"{self.device_path}"
            )
            return True

        try:

            self._wheel = Thorlabs.FW(
                str(self.device_path)
            )

        except Exception as exc:

            logger.error(
                "Failed to initialize filter wheel "
                f"on {self.device_path}: {exc}"
            )

            self._wheel = None

            return False

        logger.info(
            f"Filter wheel initialized on "
            f"{self.device_path}"
        )

        return True

    def is_open(self) -> bool:

        return self._wheel is not None

    def get_id(self):

        if self._wheel is None:

            logger.error(
                "Cannot read filter wheel ID: "
                "device is not open"
            )

            return None

        try:

            return self._wheel.get_id()

        except Exception as exc:

            logger.error(
                "Failed to read filter wheel ID "
                f"from {self.device_path}: {exc}"
            )

            return None

    def get_position_count(self) -> int | None:

        if self._wheel is None:

            logger.error(
                "Cannot read filter wheel position count: "
                "device is not open"
            )

            return None

        try:

            return int(
                self._wheel.get_pcount()
            )

        except Exception as exc:

            logger.error(
                "Failed to read filter wheel "
                f"position count from "
                f"{self.device_path}: {exc}"
            )

            return None

    def get_position(self) -> int | None:

        if self._wheel is None:

            logger.error(
                "Cannot read filter wheel position: "
                "device is not open"
            )

            return None

        try:

            return int(
                self._wheel.get_position()
            )

        except Exception as exc:

            logger.error(
                "Failed to read filter wheel "
                f"position from "
                f"{self.device_path}: {exc}"
            )

            return None

    def get_info(self) -> dict | None:

        if self._wheel is None:

            logger.error(
                "Cannot read filter wheel info: "
                "device is not open"
            )

            return None

        device_id = self.get_id()
        position_count = (
            self.get_position_count()
        )
        current_position = (
            self.get_position()
        )

        if (
            device_id is None
            or position_count is None
            or current_position is None
        ):

            return None

        return {
            "device_id": device_id,
            "position_count": position_count,
            "current_position": current_position,
            "device_path": str(
                self.device_path
            ),
        }

    def move_to(
        self,
        position: int,
    ) -> bool:

        if self._wheel is None:

            logger.error(
                "Cannot move filter wheel: "
                "device is not open"
            )

            return False

        if not isinstance(position, int):

            logger.error(
                f"Invalid filter wheel position: "
                f"{position!r}"
            )

            return False

        position_count = (
            self.get_position_count()
        )

        if position_count is None:
            return False

        if (
            position < 1
            or position > position_count
        ):

            logger.error(
                f"Filter wheel position "
                f"{position} outside valid range "
                f"1..{position_count}"
            )

            return False

        current_position = (
            self.get_position()
        )

        if current_position is None:
            return False

        if position == current_position:

            logger.debug(
                "Filter wheel already at "
                f"position {position}: "
                f"{self.device_path}"
            )

            return True

        try:

            self._wheel.set_position(
                position
            )

            self._wheel.wait_sync()

        except Exception as exc:

            logger.error(
                "Failed to move filter wheel "
                f"{self.device_path} "
                f"to position {position}: {exc}"
            )

            return False

        final_position = (
            self.get_position()
        )

        if final_position is None:
            return False

        if final_position != position:

            logger.error(
                "Filter wheel movement verification "
                f"failed on {self.device_path}: "
                f"requested={position}, "
                f"readback={final_position}"
            )

            return False

        logger.info(
            "Filter wheel moved successfully: "
            f"device={self.device_path}, "
            f"position={position}"
        )

        return True

    def close(self) -> bool:

        if self._wheel is None:
            return True

        try:

            self._wheel.close()

        except Exception as exc:

            logger.warning(
                "Failed to close filter wheel "
                f"{self.device_path}: {exc}"
            )

            return False

        self._wheel = None

        logger.info(
            f"Filter wheel closed: "
            f"{self.device_path}"
        )

        return True