import json
from client.utils.logger import get_logger
from pathlib import Path
from typing import Optional
import socket
import re, uuid
from datetime import datetime

CONFIG_PATHS = [
        Path("/etc/multipmt/client_identity.json"),
        Path.home() / ".config/multipmt/client_identity.json",
        Path(__file__).parent.parent / "config" / "client_identity.json",
    ]


class ClientIdentity:
    """Handles the identity of the client"""
    
    def __init__(self):
        self.logger = get_logger('identity')
        
        self.batch_id: Optional[str] = None
        self.multipmt_id: Optional[str] = None
        self.hostname = socket.gethostname()
        self.mac = self._get_mac_address()
        self.fixed_bad_channels : Optional[list] = None
        
        self._load_from_file()
        
        if not self._is_configured():
            self._interactive_setup()
        
        self.logger.debug("ClientIdentity initialized")
        
    
    def _get_mac_address(self) -> str:
        """Try to get the MAC address of the client"""
        try:
            mac = ':'.join(re.findall('..', '%012x' % uuid.getnode()))
            self.logger.info(f"Client MAC Address: {mac}")
            return mac
        except:
            self.logger.warning("It was not possible to retrieve the MAC Address of the client. Check you hardware. Setted to \"unknown\"")
            return "unknown"
        
    def _load_from_file(self) -> bool:
        """Try to find existing client configuration file"""
        for path in CONFIG_PATHS:
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)
                        self.multipmt_id = data.get('multipmt_id')
                        self.batch_id = data.get('batch_id')
                        self.fixed_bad_channels = data.get('fixed_bad_channels')
                        self.logger.info(f"Loaded identity from {path}: batch={self.batch_id}, multipmt={self.multipmt_id}, bad channels={self.fixed_bad_channels}")
        
                        return True
                except Exception as e:
                    self.logger.error(f"Error loading {path}: {e}")
                    
        self.logger.warning("Failed to retrieve client identity from config file. Interactive identification will be activated.")
        return False
    
    def _is_configured(self) -> bool:
        """Check if the client is identified correctly"""
        return self.batch_id is not None and self.multipmt_id is not None
    
    def _save_to_file(self):
        """Save the identity configuration to the first writable path"""
        for path in CONFIG_PATHS:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, 'w') as f:
                    json.dump({
                        'batch_id': self.batch_id,
                        'multipmt_id': self.multipmt_id,
                        'hostname': self.hostname,
                        'mac_address': self.mac,
                        'configured_at': str(__import__('datetime').datetime.now()),
                        'fixed_bad_channels': self.fixed_bad_channels
                    }, f, indent=2)
                    self.logger.info(f"Identity saved to {path}")
                    return
            except Exception as e:
                self.logger.warning(f"Cannot write to {path}: {e}")
                continue
        self.logger.error("Could not save identity to any location")
    
    def _interactive_setup(self):
        """Ask the user to insert manually the identification of the client"""
        print("\n" + "="*50)
        print("CLIENT MULTIPMT - FIRST CONFIGURATION")
        print("="*50)
        print("This client is not yet configured.")
        print("Please give the necessary info.\n")
        
        self.batch_id = input("Batch ID (es. batch_4): ").strip()
        while not self.batch_id:
            self.batch_id = input("Batch ID (mandatory): ").strip()
        
        self.multipmt_id = input("multiPMT ID (es. cile1, milano, [Enter for generic]): ").strip()
        if not self.multipmt_id:
            self.multipmt_id = "generic"
            
        self._save_to_file()
        print(f"\n Configuration saved. Please, restart the client.\n")
        
    def to_dict_identity(self) -> dict:
        if not self._is_configured():
            raise RuntimeError(
                "ClientIdentity is not configured: missing batch_id or multipmt_id"
            )

        return {
            "batch_id": self.batch_id,
            "multipmt_id": self.multipmt_id,
            "hostname": self.hostname,
            "mac_address": self.mac,
            "fixed_bad_channels": self.fixed_bad_channels
        }
    
    def update_fixed_bad_channels(self, bad_channel: int) -> None:
        for path in CONFIG_PATHS:
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)

                    current_bad_channels = data.get("fixed_bad_channels") or []

                    if bad_channel not in current_bad_channels:
                        current_bad_channels.append(bad_channel)
                        data["fixed_bad_channels"] = current_bad_channels
                        self.fixed_bad_channels = current_bad_channels

                        with open(path, 'w') as f:
                            json.dump(data, f, indent=2)
                    else:
                        self.logger.warning("Specified channel was already marked as bad")
                    return

                except Exception as e:
                    self.logger.error(f"Error updating fixed_bad_channels in {path}: {e}")
                    return

        self.logger.warning("Failed to retrieve client identity from config file.")


    def remove_fixed_bad_channels(self, bad_channel: int) -> None:
        for path in CONFIG_PATHS:
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)

                    current_bad_channels = data.get("fixed_bad_channels") or []

                    if bad_channel in current_bad_channels:
                        current_bad_channels.remove(bad_channel)
                        data["fixed_bad_channels"] = current_bad_channels
                        self.fixed_bad_channels = current_bad_channels

                        with open(path, 'w') as f:
                            json.dump(data, f, indent=2)
                    else:
                        self.logger.warning("Specified channel was not marked as bad")
                    return

                except Exception as e:
                    self.logger.error(f"Error updating fixed_bad_channels in {path}: {e}")
                    return

        self.logger.warning("Failed to retrieve client identity from config file.")


    def get_fixed_bad_channels(self) -> list:
        for path in CONFIG_PATHS:
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)
                    return data.get("fixed_bad_channels") or []
                except Exception as e:
                    self.logger.error(f"Error loading {path}: {e}")
                    return []

        self.logger.warning("Failed to retrieve client identity from config file.")
        return []
    
    def set_fixed_bad_channels(self, channels: list[int]) -> None:
        """Overwrite the entire fixed_bad_channels list and persist it to file."""
        for path in CONFIG_PATHS:
            if path.exists():
                try:
                    with open(path) as f:
                        data = json.load(f)

                    data["fixed_bad_channels"] = list(channels)
                    self.fixed_bad_channels = list(channels)

                    with open(path, 'w') as f:
                        json.dump(data, f, indent=2)

                except Exception as e:
                    self.logger.error(f"Error updating fixed_bad_channels in {path}: {e}")
                return

        self.logger.warning("Failed to retrieve client identity from config file.")