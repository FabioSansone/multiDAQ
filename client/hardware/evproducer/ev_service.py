import subprocess
from pathlib import Path
from client.utils.logger import get_logger


class EVService:

    EV_POSSIBLE_PATHS = [
        Path("/opt/mpmt-readout"),
        Path("/opt/mpmt-readout/build"),
    ]

    EV_EXECUTABLE_NAME = "evproducer"

    def __init__(self):
        self.logger = get_logger("ev_service")
        self.logger.debug("EV Service initialized")

        self.ev_path = self._find_ev_path()
        self.process: subprocess.Popen | None = None

    def _find_ev_path(self) -> Path | None:
        for path in self.EV_POSSIBLE_PATHS:
            evproducer_path = path / self.EV_EXECUTABLE_NAME

            if evproducer_path.exists() and evproducer_path.is_file():
                self.logger.info(f"Found evproducer at: {evproducer_path}")
                return evproducer_path

            self.logger.warning(
                f"evproducer not found at: {evproducer_path}. Searching another directory..."
            )

        self.logger.error("evproducer not found in possible paths")
        return None

    def start(self, server_ip: str) -> bool:
        if self.ev_path is None:
            self.logger.error("Cannot start evproducer: executable path not found")
            return False

        if self.is_running():
            self.logger.warning(
                f"evproducer already running with PID {self.process.pid}"
            )
            return True

        try:
            self.process = subprocess.Popen(
                [str(self.ev_path), "--host", server_ip],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )

            self.logger.info(
                f"Started evproducer with PID {self.process.pid} and server IP {server_ip}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to start evproducer: {e}")
            self.process = None
            return False

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def stop(self) -> bool:
        if not self.is_running():
            self.logger.warning("evproducer is not running")
            self.process = None
            return True

        try:
            self.logger.info(f"Stopping evproducer with PID {self.process.pid}")
            self.process.terminate()
            self.process.wait(timeout=5)

            self.logger.info("evproducer stopped")
            self.process = None
            return True

        except subprocess.TimeoutExpired:
            self.logger.warning("evproducer did not terminate, killing it")
            self.process.kill()
            self.process.wait()
            self.process = None
            return True

        except Exception as e:
            self.logger.error(f"Failed to stop evproducer: {e}")
            return False

    def status(self) -> dict:
        return {
            "found": self.ev_path is not None,
            "path": str(self.ev_path) if self.ev_path else None,
            "running": self.is_running(),
            "pid": self.process.pid if self.is_running() else None,
        }