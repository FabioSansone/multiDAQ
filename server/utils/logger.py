import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
import sys
from typing import Dict

LOGGER_NAMES = {

    #Server Core
    'server_state': 'server.core.state',
    
    #Communication
    'control_manager': 'server.comm.control',
    'message_handler': 'server.comm.msg',
    'handshake': 'server.handshake',

    #Acquisition
    'acquisition_manager': 'server.comm.acq',

    #Receiver
    'data_receiver': 'server.acquisition.receiver',
    
    #Utils
    'json_parser': 'server.utils.json',

    #Commands
    'generic_commands': 'server.commands.generic',
    'hv_commands': 'server.commands.hv',
    'rc_commands': 'server.commands.rc',
    'acq_commands': 'server.commands.acq',

    #Server Services
    'client_command_service': 'server.services.client_command',
    'channel_selection_service': 'server.services.channel_selection',
    'acquisition_orchestrator': 'server.services.acquisition_orchestrator',
    'calibration_orchestrator': 'server.services.calibration_orchestrator',
    'shutdown_service': 'server.services.shutdown',
    'acquisition_service': 'server.services.acquisition',

    #Main Application
    'app': 'server.app'
    
    
    
}


LOGGER_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class LoggerManager:
    
    
    _initialised = False
    _log_instances : Dict[str, logging.Logger] = {}
    
    @classmethod
    def initialize(cls, log_level: str = "DEBUG", log_to_console: bool = False):
        """Initialise base logger configuration at the start (ONE TIME)"""
        
        if cls._initialised:
            return
        
        possible_paths = [
            Path("/var/log/multipmt"),           
            Path.home() / "multipmt_logs",       
            Path.cwd() / "logs",   
        ]

        for path in possible_paths:
            try:
                path.mkdir(parents=True, exist_ok=True)
                test_file = path / "write_test.tmp"
                test_file.touch()
                test_file.unlink()
                cls._log_dir = path
                print(f"Using log directory: {path}")
                break
            except (PermissionError, OSError):
                continue
        else:
            cls._log_dir = Path.cwd()  
            print(f"Warning: Using current directory for logs: {cls._log_dir}")

        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)-8s - [%(name)s] - %(filename)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            datefmt='%H:%M:%S'
        )
        
        log_file = cls._log_dir / f"server_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5)
        file_handler.setFormatter(detailed_formatter)
        file_handler.setLevel(logging.DEBUG)
        
        
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        root_logger.setLevel(logging.DEBUG)
        
        
        
        
        if log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(LOGGER_LEVELS.get(log_level, logging.INFO))
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)
        
        
        
        cls._initialised = True
        
        
    @classmethod
    def get_logger(cls, module_name:str) -> logging.Logger:
        """
        Get a logger for a specified module.
        
        Args:
            module_name: Key for LOGGER_NAMES dictionaries (es. 'zmq_manager')
        
        Returns:
            Hierarchical logger
        """
        
        if not cls._initialised:
            cls.initialize()
        
        if module_name not in LOGGER_NAMES.keys():
            logger_name = f"server.unknown.{module_name}"
        else:
            logger_name = LOGGER_NAMES[module_name]
        
        if logger_name not in cls._log_instances:
            logger = logging.getLogger(logger_name)
            logger.propagate = True
            cls._log_instances[logger_name] = logger
        
        return cls._log_instances[logger_name]


def get_logger(module_name: str) -> logging.Logger:
    """Get a logger for the specified module"""
    return LoggerManager.get_logger(module_name)