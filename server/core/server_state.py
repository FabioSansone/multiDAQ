import threading
from typing import List, Optional
from server.utils.logger import get_logger


class ServerState:

    def __init__(self, initial_mode: str = "test"):
        self._lock = threading.Lock()

        self.acq_mode = initial_mode

        self.connected_clients: List[bytes] = []
        self.identity_by_client_id: dict[bytes, dict] = {}
        self.client_id_by_multipmt_id: dict[str, bytes] = {}

        self.logger = get_logger("server_state")
        self.logger.debug("Server State initialized")

    def set_mode(self, mode: str) -> None:
        with self._lock:
            self.acq_mode = mode

    def get_mode(self) -> str:
        with self._lock:
            return self.acq_mode

    def add_client(self, client_id: bytes, identity: Optional[dict] = None) -> None:
        with self._lock:
            if client_id not in self.connected_clients:
                self.connected_clients.append(client_id)
                self.logger.info(
                    f"Client {client_id.decode(errors='ignore')} connected. "
                    f"Total clients: {len(self.connected_clients)}"
                )

            if identity is not None:
                self.identity_by_client_id[client_id] = identity

                multipmt_id = identity.get("multipmt_id")
                if multipmt_id:
                    self.client_id_by_multipmt_id[multipmt_id] = client_id

    def remove_client(self, client_id: bytes) -> None:
        with self._lock:
            if client_id in self.connected_clients:
                self.connected_clients.remove(client_id)
                self.logger.info(
                    f"Client {client_id.decode(errors='ignore')} disconnected. "
                    f"Total clients: {len(self.connected_clients)}"
                )

            identity = self.identity_by_client_id.pop(client_id, None)

            if identity:
                multipmt_id = identity.get("multipmt_id")
                if multipmt_id in self.client_id_by_multipmt_id:
                    del self.client_id_by_multipmt_id[multipmt_id]

    def list_connected_clients(self) -> List[bytes]:
        with self._lock:
            return list(self.connected_clients)

    def get_identity(self, client_id: bytes) -> Optional[dict]:
        with self._lock:
            return self.identity_by_client_id.get(client_id)

    def get_client_id_by_multipmt_id(self, multipmt_id: str) -> Optional[bytes]:
        with self._lock:
            return self.client_id_by_multipmt_id.get(multipmt_id)