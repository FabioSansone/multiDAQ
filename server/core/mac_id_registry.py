from server.utils.logger import get_logger
from pathlib import Path
import json
import re
import threading

MAC_PATTERN = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")


def normalize_mac(mac) -> str | None:
    """Lower-case aa:bb:cc:dd:ee:ff, or None if mac is not a valid MAC address."""
    if not isinstance(mac, str):
        return None
    mac = mac.strip().lower().replace("-", ":")
    return mac if MAC_PATTERN.match(mac) else None


class MacIdentityRegistry:
    """Identity of every multiPMT, keyed by the MAC address of its Main Board (eth0).

    multipmt_registry.json, written by hand:
        {
          "fc:0f:e7:e5:f8:be": {"multipmt_id": "cile247", "batch_id": "batch_1", "numeric_id": 0},
          ...
        }
    numeric_id is the source id that evproducer puts in the data stream: it must be unique
    and must never change for a board, or the data already taken become ambiguous.

    The server never writes the file. A MAC that is not in the file is rejected at the
    handshake. The file is read again when it changes on disk, so a board can be added
    while the server is running (the client retries the handshake by itself).
    """

    CONFIG_FILES_POSSIBLE_PATHS = [
        Path(__file__).parent.parent / "multipmt_config_files" / "multipmt_registry.json",
        Path.home() / "multiPMT" / "multipmt_config_files" / "multipmt_registry.json",
    ]

    REQUIRED_FIELDS = ("multipmt_id", "batch_id", "numeric_id")

    def __init__(self, path: Path | str | None = None):

        self.logger = get_logger("mac_identity_register")

        self._lock = threading.Lock()
        self._fixed_path = Path(path) if path is not None else None
        self.path_mac_file: Path | None = None
        self._loaded_mtime: float | None = None

        self.identity_by_mac: dict[str, dict] = {}
        self.mac_by_numeric_id: dict[int, str] = {}

        self._reload_if_changed()
        self.logger.debug("MAC identity registry initialized")

    def _find_path(self) -> Path | None:
        if self._fixed_path is not None:
            return self._fixed_path
        for path_try in self.CONFIG_FILES_POSSIBLE_PATHS:
            if path_try.exists():
                return path_try
        return None

    def _parse(self, data) -> tuple[dict[str, dict], dict[int, str]]:
        """Validate the whole file: any error rejects it (no partial registry)."""
        if not isinstance(data, dict):
            raise ValueError("the file must contain a JSON object {mac: {...}}")

        identity_by_mac: dict[str, dict] = {}
        mac_by_numeric_id: dict[int, str] = {}
        mac_by_multipmt_id: dict[str, str] = {}

        for raw_mac, entry in data.items():
            mac = normalize_mac(raw_mac)
            if mac is None:
                raise ValueError(f"invalid MAC address {raw_mac!r}")
            if mac in identity_by_mac:
                raise ValueError(f"MAC {mac} listed twice")
            if not isinstance(entry, dict):
                raise ValueError(f"{mac}: the entry must be an object")

            missing = [field for field in self.REQUIRED_FIELDS if entry.get(field) in (None, "")]
            if missing:
                raise ValueError(f"{mac}: missing {', '.join(missing)}")

            numeric_id = entry["numeric_id"]
            if isinstance(numeric_id, bool) or not isinstance(numeric_id, int) or numeric_id < 0:
                raise ValueError(f"{mac}: numeric_id must be an integer >= 0, got {numeric_id!r}")
            if numeric_id in mac_by_numeric_id:
                raise ValueError(
                    f"numeric_id {numeric_id} used by both {mac_by_numeric_id[numeric_id]} and {mac}"
                )

            multipmt_id = str(entry["multipmt_id"])
            if multipmt_id in mac_by_multipmt_id:
                raise ValueError(
                    f"multipmt_id {multipmt_id!r} used by both {mac_by_multipmt_id[multipmt_id]} and {mac}"
                )

            identity_by_mac[mac] = {
                "multipmt_id": multipmt_id,
                "batch_id": str(entry["batch_id"]),
                "numeric_id": numeric_id,
            }
            mac_by_numeric_id[numeric_id] = mac
            mac_by_multipmt_id[multipmt_id] = mac

        return identity_by_mac, mac_by_numeric_id

    def _reload_if_changed(self) -> None:
        with self._lock:
            path = self._find_path()

            if path is None or not path.exists():
                if self.path_mac_file is not None or self._loaded_mtime is None:
                    self.logger.error(
                        "multiPMT registry not found (multipmt_registry.json in "
                        f"{', '.join(str(p.parent) for p in self.CONFIG_FILES_POSSIBLE_PATHS)}): "
                        "every client will be rejected at the handshake"
                    )
                self.identity_by_mac, self.mac_by_numeric_id = {}, {}
                self.path_mac_file, self._loaded_mtime = None, 0.0
                return

            try:
                mtime = path.stat().st_mtime
            except OSError as e:
                self.logger.error(f"Cannot read {path}: {e}")
                return

            if path == self.path_mac_file and mtime == self._loaded_mtime:
                return

            # Remember this version even if it is invalid: the error is logged once, not at every lookup
            self.path_mac_file, self._loaded_mtime = path, mtime

            try:
                with open(path) as f:
                    identity_by_mac, mac_by_numeric_id = self._parse(json.load(f))
            except Exception as e:
                self.logger.error(
                    f"Invalid multiPMT registry {path}: {e}. "
                    + ("Keeping the previous registry." if self.identity_by_mac else "No client can be admitted.")
                )
                return

            self.identity_by_mac, self.mac_by_numeric_id = identity_by_mac, mac_by_numeric_id
            self.logger.info(f"Loaded multiPMT registry from {path}: {len(identity_by_mac)} boards")

    def lookup(self, client_mac) -> dict | None:
        """{"multipmt_id", "batch_id", "numeric_id"} of this MAC, or None if it is not registered."""
        mac = normalize_mac(client_mac)
        if mac is None:
            return None
        self._reload_if_changed()
        with self._lock:
            entry = self.identity_by_mac.get(mac)
            return dict(entry) if entry is not None else None

    def get_id_from_mac(self, client_mac) -> int | None:
        entry = self.lookup(client_mac)
        return entry["numeric_id"] if entry is not None else None

    def get_mac_from_id(self, client_numeric_id: int | str) -> str | None:
        try:
            numeric_id = int(client_numeric_id)
        except (TypeError, ValueError):
            return None
        self._reload_if_changed()
        with self._lock:
            return self.mac_by_numeric_id.get(numeric_id)

    def describe_path(self) -> str:
        return str(self.path_mac_file or self.CONFIG_FILES_POSSIBLE_PATHS[0])
