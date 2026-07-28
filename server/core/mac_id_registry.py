from server.utils.logger import get_logger
from pathlib import Path
import json

class MacIdentityRegistry:
    
    CONFIG_FILES_POSSIBLE_PATHS = [
        Path("multiDAQ/server/multipmt_config_files/mac_registry.json"),
        Path.home() / "multiPMT" / "multipmt_config_files" / "mac_registry.json",
        Path("swgo/multiPMT/multipmt_config_files/mac_registry.json")
        ]


    def __init__(self):

        self.logger = get_logger("mac_identity_register")
        self.logger.debug("ZMQ MAC ID Registry initialized")
        
        self.path_mac_file: str | None = None
        self.present_client_mac_id: dict | None = None
        
        
    def _load_from_file(self):
        path = None
        if self.path_mac_file is not None: 
            path = self.path_mac_file 
        else:
            for path_try in self.CONFIG_FILES_POSSIBLE_PATHS:
                if path_try.exists():
                    path = path_try
                    break
                
        if path is None:
            self.logger.warning(
                "No existing mac identity registry file found among configured paths; "
                "a new one will be created on first client registration."
            )
            return False
        
       
        
        try:
            with open(path) as f:
                self.present_client_mac_id = json.load(f)
                self.path_mac_file = path
                self.logger.info(f"Loaded mac identity registry from {path}")
                return True
        except Exception as e:
            self.logger.error(f"Error loading {path}: {e}")
            return False
        

    def add_client_mac(self, client_mac: str):
        if self.present_client_mac_id is None:
            self._load_from_file()

        if self.present_client_mac_id is None:
            self.present_client_mac_id = {}

        if client_mac in self.present_client_mac_id:
            self.logger.debug(
                f"Client MAC {client_mac} already registered with id "
                f"{self.present_client_mac_id[client_mac]}"
            )
            return self.present_client_mac_id[client_mac]

        actual_idx = self.present_client_mac_id.get("index", -1)
        new_idx = actual_idx + 1

        self.present_client_mac_id[client_mac] = new_idx
        self.present_client_mac_id["index"] = new_idx

        path = self.path_mac_file or self.CONFIG_FILES_POSSIBLE_PATHS[0]

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(self.present_client_mac_id, f, indent=2)
            self.path_mac_file = path
            self.logger.info(
                f"Registered new client MAC {client_mac} with numeric id {new_idx} "
                f"(saved to {path})"
            )
            return new_idx
        except Exception as e:
            self.logger.error(f"Error saving mac identity registry to {path}: {e}")
            return None
    
    def get_id_from_mac(self, client_mac: str):
        if self.present_client_mac_id is None:
            self._load_from_file()

        if self.present_client_mac_id is not None and client_mac in self.present_client_mac_id:
            return self.present_client_mac_id[client_mac]

        return self.add_client_mac(client_mac=client_mac)