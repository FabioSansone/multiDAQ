from pathlib import Path
from server.utils.logger import get_logger

logger = get_logger("optical_discovery")

SERIAL_BY_ID_PATH = Path("/dev/serial/by-id")

def discover_serial_devices() -> dict[str, Path]:
    if not SERIAL_BY_ID_PATH.exists():
        logger.error(
            f"Serial device directory not found: {SERIAL_BY_ID_PATH}"
        )
        return {}

    devices: dict[str, Path] = {}

    for device in SERIAL_BY_ID_PATH.iterdir():
        if not device.is_symlink():
            continue

        devices[device.name] = device

    logger.info(
        f"Discovered {len(devices)} serial device(s)"
    )

    return devices

def resolve_serial_device(serial: str) -> Path | None:
    if not serial or not serial.strip():
        logger.error("Cannot resolve serial device: empty serial")
        return None
    
    devices = discover_serial_devices()

    matches = [path for name, path in devices.items() if serial in name]

    if not matches:
        logger.error(
            f"No serial device found for serial {serial!r}"
        )
        return None

    if len(matches) > 1:
        logger.error(
            f"Multiple serial devices found for serial {serial!r}: "
            f"{matches}"
        )
        return None

    return matches[0]



