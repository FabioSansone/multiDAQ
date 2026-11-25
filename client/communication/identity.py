import json
import socket
from pathlib import Path
from typing import Optional

from client.utils.logger import get_logger
from common.version import __version__

# Local state of this board (not its identity): today only the I2C bus of the Main Board
# sensors, a cache that saves the scan of the buses at startup. Losing it is harmless.
STATE_PATHS = [
    Path("/var/lib/multidaq/client_state.json"),
    Path.home() / ".local/state/multidaq/client_state.json",
]

# Identity files of the previous versions: read once, only for the I2C bus cache.
LEGACY_IDENTITY_PATHS = [
    Path("/etc/multipmt/client_identity.json"),
    Path.home() / ".config/multipmt/client_identity.json",
]


class ClientIdentity:
    """Identity of the client.

    The board knows only its MAC address (of IFACE, the interface the DHCP server knows)
    and its hostname. batch_id and multipmt_id are assigned by the server at the control
    handshake, from its multiPMT registry keyed by MAC: nothing is configured on the board.
    """

    def __init__(self, iface: str = "eth0"):
        self.logger = get_logger('identity')

        self.iface = iface
        self.hostname = socket.gethostname()
        self.mac = self._read_mac_address(iface)

        # Assigned by the server at the end of the control handshake
        self.batch_id: Optional[str] = None
        self.multipmt_id: Optional[str] = None

        self.i2cbus_sensors: int | None = None

        self.logger.info(f"Client MAC address ({iface}): {self.mac}, hostname: {self.hostname}")

    @staticmethod
    def _read_mac_address(iface: str) -> str:
        """MAC of a fixed interface: uuid.getnode() picks any interface, or a random number."""
        path = Path("/sys/class/net") / iface / "address"
        try:
            mac = path.read_text().strip().lower()
        except OSError as e:
            raise RuntimeError(
                f"Cannot read the MAC address of {iface} ({path}): {e}. "
                "Use --iface to choose the interface known to the server."
            ) from e
        if len(mac.split(":")) != 6 or mac == "00:00:00:00:00:00":
            raise RuntimeError(f"Invalid MAC address of {iface}: {mac!r}")
        return mac

    def _is_configured(self) -> bool:
        """True once the server has assigned batch_id and multipmt_id."""
        return self.batch_id is not None and self.multipmt_id is not None

    def to_dict_identity(self) -> dict:
        """What the client sends at the handshake: the server looks the MAC up."""
        return {
            "mac_address": self.mac,
            "hostname": self.hostname,
            "client_version": __version__,
        }

    def set_from_server(self, identity: dict) -> None:
        """batch_id and multipmt_id received at the end of the control handshake."""
        multipmt_id = identity.get("multipmt_id")
        batch_id = identity.get("batch_id")
        if not multipmt_id or not batch_id:
            raise ValueError(f"Incomplete identity from the server: {identity}")

        if self._is_configured() and (self.multipmt_id, self.batch_id) != (multipmt_id, batch_id):
            self.logger.warning(
                f"Identity changed by the server: {self.multipmt_id}/{self.batch_id} -> "
                f"{multipmt_id}/{batch_id}"
            )

        self.multipmt_id = str(multipmt_id)
        self.batch_id = str(batch_id)
        self.logger.info(f"Identity assigned by the server: batch={self.batch_id}, multipmt={self.multipmt_id}")

    # ------------------------------------------------------------------ local state (I2C cache)

    def _read_state(self) -> dict:
        for path in STATE_PATHS:
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        return data
                    self.logger.warning(f"Ignoring {path}: not a JSON object")
                except Exception as e:
                    self.logger.warning(f"Cannot read {path}: {e}")
        return {}

    def get_i2cbus(self) -> int | None:
        value = self._read_state().get("i2cbus_sensors")

        if value is None:
            for path in LEGACY_IDENTITY_PATHS:
                if path.exists():
                    try:
                        with open(path) as f:
                            value = json.load(f).get("i2cbus_sensors")
                    except Exception:
                        value = None
                    if value is not None:
                        self.logger.info(f"I2C bus cache taken from the old identity file {path}")
                        break

        if value is None:
            return None
        try:
            self.i2cbus_sensors = int(value)
        except (TypeError, ValueError):
            self.logger.warning(f"Invalid cached I2C bus: {value!r}")
            return None
        return self.i2cbus_sensors

    def set_i2cbus(self, i2cbus: int) -> None:
        """Persist the I2C bus used by the Main Board sensors (first writable state path)."""
        i2cbus = int(i2cbus)
        data = self._read_state()
        data["i2cbus_sensors"] = i2cbus

        for path in STATE_PATHS:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_name(path.name + ".tmp")
                with open(tmp, "w") as f:
                    json.dump(data, f, indent=2)
                tmp.replace(path)
                self.i2cbus_sensors = i2cbus
                self.logger.info(f"Cached sensor I2C bus: i2c-{i2cbus} ({path})")
                return
            except OSError as e:
                self.logger.debug(f"Cannot write {path}: {e}")

        self.logger.warning("Cannot save the I2C bus cache: no writable state path")
