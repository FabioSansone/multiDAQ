import subprocess
from datetime import datetime
from pathlib import Path
import time
from client.utils.logger import LoggerManager, get_logger


class EVService:

    EV_POSSIBLE_PATHS = [
        Path("/opt/mpmt-readout"),
        Path("/opt/mpmt-readout/build"),
    ]

    EV_EXECUTABLE_NAME = "evproducer"
    DMA_MODULE_NAME = "dma-proxy"

    def __init__(self):
        self.logger = get_logger("ev_service")
        self.logger.debug("EV Service initialized")

        self.ev_path = self._find_ev_path()
        self.process: subprocess.Popen | None = None
        self.output_path: Path | None = None

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
    
    def _load_dma_proxy_module(self) -> bool:
        try:
            result = subprocess.run(
                ["modprobe", self.DMA_MODULE_NAME],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5,
            )

            if result.returncode == 0:
                self.logger.info("dma-proxy module loaded successfully")
                return True

            self.logger.error(
                f"modprobe dma-proxy failed: {result.stderr.strip()}"
            )
            return False

        except Exception as e:
            self.logger.error(f"Exception during modprobe dma-proxy: {e}")
            return False
    
    def _spawn_evproducer(self, server_ip: str, client_id: int | None = None) -> subprocess.Popen:
        # The output goes to a file, not to a pipe: nobody reads a pipe after the startup check,
        # and a full pipe would block the evproducer on its next write.
        log_dir = getattr(LoggerManager, "_log_dir", Path.cwd())
        self.output_path = log_dir / f"evproducer_{datetime.now().strftime('%Y%m%d')}.log"

        with open(self.output_path, "a") as output:
            output.write(f"\n===== {datetime.now().isoformat(timespec='seconds')} start, client id {client_id} =====\n")
            output.flush()

            return subprocess.Popen(
                [str(self.ev_path), "--host", server_ip, "--id", str(client_id)],
                stdout=output,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )

    def _output_tail(self, max_bytes: int = 2000) -> str:
        if self.output_path is None:
            return ""

        try:
            with open(self.output_path, "rb") as output:
                output.seek(0, 2)
                output.seek(max(0, output.tell() - max_bytes))
                return output.read().decode(errors="replace").strip()
        except OSError:
            return ""

    def _check_process_alive_after_startup(self, process: subprocess.Popen)-> bool:
        startup_check_time_s = 1.0
        check_interval_s = 0.2
        
        elapsed = 0.0
        
        while elapsed < startup_check_time_s:
            return_code = process.poll()
            
            if return_code is not None:
                self.logger.error(
                    f"evproducer exited during startup with return code {return_code}"
                )

                tail = self._output_tail()
                if tail:
                    self.logger.error(f"evproducer output (from {self.output_path}): {tail}")

                return False
            
            time.sleep(check_interval_s)
            elapsed += check_interval_s
            
        return True

    def start(self, server_ip: str, client_id: int | None = None) -> bool:

        if client_id is None:
            self.logger.error(
                "Cannot start evproducer: no numeric client id assigned yet "
                "(handshake not completed or mac registry unavailable)."
            )
            return False
    
        if self.ev_path is None:
            self.logger.error("Cannot start evproducer: executable path not found")
            return False

        if self.is_running():
            self.logger.warning(
                f"evproducer already running with PID {self.process.pid}"
            )
            return True

        for attempt in range(2):
            self.logger.info(f"Starting evproducer, attempt {attempt + 1}/2")
            try:
                process = self._spawn_evproducer(server_ip, client_id)
                
                if self._check_process_alive_after_startup(process):
                    self.process = process
                    self.logger.info(
                        f"Started evproducer with PID {self.process.pid} and server IP {server_ip}"
                    )
                    return True

                self.process = None
                
                if attempt == 0:
                    self.logger.warning(
                        "evproducer failed during startup. "
                        "Trying to load dma-proxy module and retry..."
                    )

                    if not self._load_dma_proxy_module():
                        self.logger.error("Cannot recover: dma-proxy loading failed")
                        return False

            except Exception as e:
                self.logger.error(f"Failed to start evproducer: {e}")
                self.process = None
                
                if attempt == 0:
                    if not self._load_dma_proxy_module():
                        return False

        self.logger.error("Failed to start evproducer after retry")
        return False
    
    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def stop(self) -> bool:
        if self.process is None:
            return True

        if self.process.poll() is not None:
            self.process.wait()
            self.process = None
            return True

        try:
            self.logger.info(f"Stopping evproducer PID {self.process.pid}")
            self.process.terminate()
            self.process.wait(timeout=5)
            self.process = None
            return True

        except subprocess.TimeoutExpired:
            self.logger.warning("evproducer did not terminate, killing it")
            self.process.kill()
            self.process.wait()
            self.process = None
            return True

    def kill_stale_processes(self) -> bool:
        """Kill every evproducer on the board, also the ones this service did not start.

        The evproducer is spawned in its own session, so it survives the client when the
        client dies abruptly (kill -9, crash). Meant to be used at startup, before this
        service has spawned its own process. Returns True if none is left."""

        name = self.EV_EXECUTABLE_NAME

        def is_alive() -> bool:
            return subprocess.run(["pgrep", "-x", name], capture_output=True).returncode == 0

        try:
            if not is_alive():
                self.logger.info("No stale evproducer found")
                return True

            self.logger.warning("Stale evproducer found: terminating it")
            subprocess.run(["killall", name], capture_output=True)

            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and is_alive():
                time.sleep(0.2)

            if is_alive():
                self.logger.warning("Stale evproducer did not terminate, killing it")
                subprocess.run(["killall", "-KILL", name], capture_output=True)
                time.sleep(0.5)

            if is_alive():
                self.logger.error("Stale evproducer still running")
                return False

            self.logger.info("Stale evproducer removed")
            return True

        except Exception:
            self.logger.exception("Cannot remove the stale evproducer")
            return False

    def status(self) -> dict:
        return {
            "found": self.ev_path is not None,
            "path": str(self.ev_path) if self.ev_path else None,
            "running": self.is_running(),
            "pid": self.process.pid if self.is_running() else None,
        }