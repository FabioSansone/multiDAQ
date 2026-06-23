from pathlib import Path
from datetime import datetime
import subprocess

from server.utils.logger import get_logger


ACQUISITION_FOLDERS = {
    "polarizer": "polarizer_calibration",
    "pedestal": "pedestal_characterisation",
    "spe": "single_photoelectron",
    "gain": "gain_curve",
    "wheels_char": "wheels_characterisation",
    "fiber_char": "fiber_characterisation",
    "threshold": "threshold_calibration",
    "threshold_dark": "threshold_calibration_dark",
    "threshold_scan": "threshold_scan",
    "spe_equal": "spe_equal_gains",
    "test": "test",
}


class DataReceiverService:

    def __init__(self):
        self.logger = get_logger("data_receiver")

        self.process: subprocess.Popen | None = None
        self.current_file: Path | None = None
        self.current_folder: Path | None = None

        self.receiver_dir = Path(__file__).parent
        self.evr_exe = self.receiver_dir / "evreceiver"
        self.evr_src = self.receiver_dir / "evreceiver.c"

        self.receiver_ready = self.compile_evreceiver()

        if self.receiver_ready:
            self.logger.info("DataReceiverService initialized")
        else:
            self.logger.error("DataReceiverService initialized, but evreceiver is unavailable")

    @staticmethod
    def generate_timestamp() -> str:
        return datetime.now().strftime("%Y_%m_%d_%H_%M")

    @staticmethod
    def generate_date_folder() -> str:
        return datetime.now().strftime("%Y_%m_%d")

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _get_base_path(self) -> Path:
        if Path("/swgo").exists():
            return Path("/swgo")
        return Path.home()

    def _get_run_folder(self, acq_type: str, batch_id: str | int | None = None, run_id: str | int | None = None) -> Path:
        batch_name = str(batch_id)

        if not batch_name.startswith("batch_"):
            batch_name = f"batch_{batch_name}"

        folder_name = ACQUISITION_FOLDERS.get(acq_type, "unknown")

        base_folder = (
            self._get_base_path()
            / "multiPMT"
            / "acquisition"
            / batch_name
            / folder_name
            / self.generate_date_folder()
        )

        if run_id is not None:
            run_folder = base_folder / f"run_{run_id}"
        else:
            i = 1
            run_folder = base_folder / f"acq_{i}"

            while run_folder.exists():
                i += 1
                run_folder = base_folder / f"acq_{i}"

        run_folder.mkdir(parents=True, exist_ok=True)
        return run_folder

    def _build_file_path(self, run_folder: Path, suffix: str = "") -> Path:
        timestamp = self.generate_timestamp()

        if suffix:
            filename = f"daq_{timestamp}_{suffix}.csv"
        else:
            filename = f"daq_{timestamp}.csv"

        filepath = run_folder / filename

        if not filepath.exists():
            return filepath

        base = filepath.with_suffix("")
        ext = filepath.suffix

        i = 1
        while True:
            candidate = Path(f"{base}_{i}{ext}")
            if not candidate.exists():
                return candidate
            i += 1

    def compile_evreceiver(self, force_compile: bool = False) -> bool:
        if not self.evr_src.exists():
            self.logger.error(f"evreceiver source not found: {self.evr_src}")
            return False

        if self.evr_exe.exists() and not force_compile:
            self.logger.info(f"evreceiver executable found: {self.evr_exe}")
            return True

        self.logger.info("Compiling evreceiver executable...")

        compile_cmd = [
            "gcc",
            str(self.evr_src),
            "-o",
            str(self.evr_exe),
            "-lzmq",
            "-lpthread",
            "-O2",
        ]

        result = subprocess.run(
            compile_cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            self.logger.error(
                f"evreceiver compilation failed:\n{result.stderr}"
            )
            return False

        self.logger.info("evreceiver compilation completed successfully")
        return True

    def start(self, duration: int | float | None = None, suffix: str = "", acq_type: str = "test", run_id: str | int | None = None, batch_id: str | int | None = None, force_compile: bool = False) -> dict | None:
        if self.is_running():
            self.logger.error(
                f"Cannot start data receiver: already running with PID {self.process.pid}"
            )
            return None

        if force_compile:
            self.receiver_ready = self.compile_evreceiver(force_compile=True)

        if not self.receiver_ready:
            self.logger.error("Cannot start data receiver: evreceiver unavailable")
            return None

        if batch_id is None:
            self.logger.error("Cannot start data receiver: missing batch_id")
            return None

        run_folder = self._get_run_folder(
            acq_type=acq_type,
            batch_id=batch_id,
            run_id=run_id,
        )

        filepath = self._build_file_path(
            run_folder=run_folder,
            suffix=suffix,
        )

        duration_arg = str(duration if duration is not None else -1)

        self.logger.info(
            f"Starting evreceiver: file={filepath}, duration={duration_arg}"
        )

        try:
            self.process = subprocess.Popen(
                [
                    str(self.evr_exe),
                    str(filepath),
                    duration_arg,
                    "0"
                ]
            )

        except Exception as e:
            self.logger.error(f"Failed to start evreceiver: {e}")
            self.process = None
            return None

        self.current_file = filepath
        self.current_folder = run_folder

        return {
            "pid": self.process.pid,
            "file": str(filepath),
            "folder": str(run_folder),
            "duration": duration,
            "acq_type": acq_type,
            "batch_id": batch_id,
            "run_id": run_id,
        }

    def stop(self) -> bool:
        if not self.is_running():
            self.logger.warning("Data receiver is not running")
            return True

        self.logger.info(f"Stopping evreceiver PID {self.process.pid}")

        try:
            self.process.terminate()
            self.process.wait(timeout=5.0)

        except subprocess.TimeoutExpired:
            self.logger.warning(
                f"evreceiver PID {self.process.pid} did not stop, killing it"
            )
            self.process.kill()
            self.process.wait(timeout=5.0)

        except Exception as e:
            self.logger.error(f"Error while stopping evreceiver: {e}")
            return False

        finally:
            self.process = None

        self.logger.info("evreceiver stopped")
        return True

    def status(self) -> dict:
        return {
            "running": self.is_running(),
            "pid": self.process.pid if self.is_running() else None,
            "file": str(self.current_file) if self.current_file else None,
            "folder": str(self.current_folder) if self.current_folder else None,
        }
    

    def start_flush(
        self,
        duration: int | float | None = None,
    ) -> dict | None:

        if self.is_running():
            self.logger.error(
                f"Cannot start flush service: already running with PID {self.process.pid}"
            )
            return None

        if not self.receiver_ready:
            self.logger.error(
                "Cannot start flush service: evreceiver unavailable"
            )
            return None

        if self.current_file is None:
            self.logger.error(
                "Cannot start flush service: no acquisition file available"
            )
            return None

        duration_arg = str(duration if duration is not None else -1)

        self.logger.info(
            f"Starting flushing of last data: "
            f"file={self.current_file}, duration={duration_arg}"
        )

        try:
            self.process = subprocess.Popen(
                [
                    str(self.evr_exe),
                    str(self.current_file),
                    duration_arg,
                    "1"
                ]
            )

        except Exception as e:
            self.logger.error(f"Failed to start flushing: {e}")
            self.process = None
            return None

        return {
            "pid": self.process.pid,
            "file": str(self.current_file),
            "folder": str(self.current_folder),
            "duration": duration,
        }