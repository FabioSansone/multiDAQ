from pathlib import Path
from datetime import datetime
import subprocess
import zmq
from typing import Optional
import time
import threading

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
    "ttp": "time_to_peak",
    "test": "test",
}

CONTROL_LISTENER_PORT = "5556"
CONTROL_COMMAND_TIMEOUT_MS = 10000

METRICS_LISTENER_PORT = "5557"
METRICS_POLL_TIMEOUT_MS = 500


class DataReceiverService:

    def __init__(self, context: zmq.Context):
        self.logger = get_logger("data_receiver")

        self.context = context
        self.socket: Optional[zmq.Socket] = None

        self.process: subprocess.Popen | None = None  # il processo PERSISTENTE, uno solo per tutta la sessione

        self.receiver_dir = Path(__file__).parent
        self.evr_exe = self.receiver_dir / "evreceiver"
        self.evr_src = self.receiver_dir / "evreceiver.c"

        self.receiver_ready = self.compile_evreceiver(force_compile=True) #mettere a False se non vogliamo ricompilare ad ogni avvio del server

        self.metrics_callback = None

        self._metrics_stop_event = threading.Event()
        self._metrics_ready_event = threading.Event()

        self._metrics_thread = None


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
    
    @staticmethod
    def _serialize_metadata(metadata: dict) -> str:
        fields = []

        for key, value in metadata.items():
            if isinstance(value, bytes):
                value = value.decode(errors="ignore")

            value = str(value)

            if ";" in str(key) or "=" in str(key):
                raise ValueError(f"Invalid metadata key: {key!r}")

            if ";" in value:
                raise ValueError(
                    f"Metadata value for {key!r} contains ';'"
                )
            
            if "=" in value:
                raise ValueError(f"Metadata value for {key!r} contains '='")

            fields.append(f"{key}={value}")

        return ";".join(fields)

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def get_exit_code(self) -> int | None:
        if self.process is None:
            return None
        return self.process.poll()

    def _get_base_path(self) -> Path:
        if Path("/swgo").exists():
            return Path("/swgo")
        return Path.home()

    def get_run_folder(self, acq_type: str, client_identity: str, run_id: str | int | None = None) -> Path:
        folder_name = ACQUISITION_FOLDERS.get(acq_type, "unknown")

        base_folder = (
            self._get_base_path()
            / "multiPMT"
            / "acquisition"
            / client_identity
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

    def _build_file_path(self, run_folder: Path, suffix: str = "", file_format: str = "csv") -> Path:
        timestamp = self.generate_timestamp()

        if suffix:
            filename = f"daq_{timestamp}_{suffix}_chunk000.{file_format}"
        else:
            filename = f"daq_{timestamp}_chunk000.{file_format}"

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
            str(self.receiver_dir / "dispatch" / "worker_dispatcher.c"),
            "-o",
            str(self.evr_exe),
            "-lzmq",
            "-lpthread",
            "-O2",
        ]

        result = subprocess.run(compile_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            self.logger.error(f"evreceiver compilation failed:\n{result.stderr}")
            return False

        self.logger.info("evreceiver compilation completed successfully")
        return True

    def _bind_control_socket(self) -> bool:
        if self.socket is not None:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None

        try:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.RCVTIMEO, CONTROL_COMMAND_TIMEOUT_MS)
            self.socket.bind(f"tcp://*:{CONTROL_LISTENER_PORT}")
            self.logger.debug(f"Control channel bound on port {CONTROL_LISTENER_PORT}")
            return True
        except zmq.ZMQError as e:
            self.socket = None
            self.logger.error(f"ZMQ error binding control socket on port {CONTROL_LISTENER_PORT}: {e}")
            return False
        except Exception as e:
            self.socket = None
            self.logger.error(f"Unexpected error binding control socket on port {CONTROL_LISTENER_PORT}: {e}")
            return False

    def _send_command(self, frames: list[str]) -> tuple[bool, str]:
        if self.socket is None:
            return False, "control socket not initialized"

        try:
            self.socket.send_multipart(
                [frame.encode("utf-8") for frame in frames]
            )
            reply = self.socket.recv_string()
        except zmq.Again:
            self.logger.error(f"Timeout waiting for reply to command '{frames}'")
            return False, "timeout waiting for reply"
        except zmq.ZMQError as e:
            self.logger.error(f"ZMQ error sending command '{frames}': {e}")
            return False, str(e)

        if reply == "OK":
            return True, reply

        self.logger.error(f"Command '{frames}' failed: {reply}")
        return False, reply

    def start_persistent_receiver(self) -> bool:
        if self.is_running():
            self.logger.warning(f"evreceiver already running with PID {self.process.pid}")
            return True

        if not self.receiver_ready:
            self.logger.error("Cannot start evreceiver: executable unavailable")
            return False

        if not self._bind_control_socket():
            return False

        self.logger.info("Starting persistent evreceiver process...")

        if not self.start_metrics_listener():
            self.logger.warning(
                "Acquisition metrics listener "
                "could not be started. "
                "evreceiver will run without "
                "runtime metrics."
            )

        try:
            self.process = subprocess.Popen([str(self.evr_exe)], start_new_session=True,)
        except Exception as e:
            self.logger.error(f"Failed to start evreceiver: {e}")
            self.process = None
            return False

        time.sleep(0.5)  

        if not self.is_running():
            self.logger.error(f"evreceiver exited immediately after startup (exit code {self.get_exit_code()})")
            return False

        self.logger.info(f"evreceiver started, PID {self.process.pid}")
        return True

    def stop_persistent_receiver(self) -> bool:
        if not self.is_running():
            self.logger.warning("evreceiver is not running")
            self.process = None
            self.stop_metrics_listener()
            return True

        self.logger.info(f"Stopping evreceiver PID {self.process.pid}")

        try:
            self.process.terminate()
            self.process.wait(timeout=5.0)
            self.stop_metrics_listener()
        except subprocess.TimeoutExpired:
            self.logger.warning(f"evreceiver PID {self.process.pid} did not stop, killing it")
            self.process.kill()
            self.process.wait(timeout=5.0)
        except Exception as e:
            self.logger.error(f"Error while stopping evreceiver: {e}")
            return False
        finally:
            self.process = None
            if self.socket is not None:
                self.socket.setsockopt(zmq.LINGER, 0)
                self.socket.close()
                self.socket = None

        self.logger.info("evreceiver stopped")
        return True

    def open_file(
        self,
        client_id: str | int,          
        client_identity: str, 
        metadata: dict, 
        file_format: str = "csv", #"csv" or "bin"        
        acq_type: str = "test",
        suffix: str = "",
        run_id: str | int | None = None,
        run_folder: Path | None = None,
    ) -> dict | None:
        if not self.is_running():
            self.logger.error(f"Cannot open file for client {client_id}: evreceiver is not running")
            return None

        if run_folder is None:
            run_folder = self.get_run_folder(acq_type=acq_type, client_identity=client_identity, run_id=run_id)
        else:
            run_folder.mkdir(parents=True, exist_ok=True)

        filepath = self._build_file_path(run_folder=run_folder, suffix=suffix, file_format=file_format)
        client_id_str = str(client_id)

        metadata_wire = self._serialize_metadata(metadata)

        success, reply = self._send_command([
            "OPEN",
            client_id_str,
            str(filepath),
            file_format,
            metadata_wire
        ])

        if not success:
            self.logger.error(f"OPEN failed for client {client_id_str}: {reply}")
            return None

        self.logger.info(f"Opened file for client {client_id_str} ({client_identity}): {filepath}")

        return {
            "client_id": client_id_str,
            "client_identity": client_identity,
            "file": str(filepath),
            "folder": str(run_folder),
            "acq_type": acq_type,
            "run_id": run_id,
        }

    def close_file(self, client_id: str | int) -> bool:
        client_id_str = str(client_id)

        if not self.is_running():
            self.logger.warning(f"Cannot close file for client {client_id_str}: evreceiver is not running")
            return False

        success, reply = self._send_command([
            "CLOSE",
            client_id_str,
        ])

        if not success:
            self.logger.error(f"CLOSE failed for client {client_id_str}: {reply}")
            return False

        self.logger.info(f"Closed file for client {client_id_str}")
        return True

    def status(self) -> dict:
        return {
            "running": self.is_running(),
            "pid": self.process.pid if self.is_running() else None,
        }

    def _metrics_listener_loop(self, ) -> None:

        metrics_socket = None
        try:
            metrics_socket = self.context.socket(zmq.PULL)
            metrics_socket.setsockopt(zmq.LINGER, 0)
            metrics_socket.bind(f"tcp://127.0.0.1:{METRICS_LISTENER_PORT}")

            poller = zmq.Poller()
            poller.register(metrics_socket, zmq.POLLIN)

            self._metrics_ready_event.set()

            self.logger.info(
                "Acquisition metrics listener "
                f"started on port "
                f"{METRICS_LISTENER_PORT}"
            )

            while not self._metrics_stop_event.is_set():

                events = dict(poller.poll(METRICS_POLL_TIMEOUT_MS))

                if metrics_socket not in events:
                    continue

                try:
                    raw_message = metrics_socket.recv_string()
                except zmq.ZMQError as e:
                    if not self._metrics_stop_event.is_set():
                        self.logger.error(
                            "Acquisition metrics receive "
                            f"error: {e}"
                        )
                    continue

                metric = self._parse_metrics_message(raw_message)
                if metric is None:
                    continue

                callback = self.metrics_callback

                try:
                    callback(metric)
                except Exception as e:
                    self.logger.exception(
                        "Acquisition metrics callback "
                        f"failed: {e}"
                    )

        except Exception as e:
            self.logger.exception(
                "Acquisition metrics listener "
                f"failed: {e}"
            )

            self._metrics_ready_event.set()

        finally:
            if metrics_socket is None:
                try:
                    metrics_socket.close(linger=0)
                except Exception:
                    pass
            self.logger.info(
                "Acquisition metrics listener stopped"
            )


    def _parse_metrics_message(
        self,
        raw_message: str,
    ) -> dict | None:

        if not raw_message:

            return None


        fields = (
            raw_message.split("|")
        )

        kind = (
            fields[0]
        )


        if kind == "SOURCE":

            if len(fields) != 6:

                self.logger.warning(
                    "Invalid SOURCE acquisition "
                    f"metric: {raw_message!r}"
                )

                return None

            try:

                return {
                    "type": "source",
                    "source_id": fields[1],
                    "events_received": int(
                        fields[2]
                    ),
                    "events_written": int(
                        fields[3]
                    ),
                    "bytes_received": int(
                        fields[4]
                    ),
                    "timestamp_monotonic_ns": int(fields[5])
                }

            except ValueError:

                self.logger.warning(
                    "Invalid numeric SOURCE "
                    "acquisition metric: "
                    f"{raw_message!r}"
                )

                return None


        if kind == "WORKER":

            if len(fields) != 4:

                self.logger.warning(
                    "Invalid WORKER acquisition "
                    f"metric: {raw_message!r}"
                )

                return None

            try:

                return {
                    "type": "worker",
                    "worker": int(
                        fields[1]
                    ),
                    "queue_size": int(
                        fields[2]
                    ),
                    "queue_capacity": int(
                        fields[3]
                    ),
                }

            except ValueError:

                self.logger.warning(
                    "Invalid numeric WORKER "
                    "acquisition metric: "
                    f"{raw_message!r}"
                )

                return None
            
        if kind == "SOURCE_STOP":
            if len(fields) != 2:
                return None
            return {
                "type": "source_stop",
                "source_id": fields[1]
            }


        self.logger.warning(
            "Unknown acquisition metric "
            f"type: {raw_message!r}"
        )

        return None


    def start_metrics_listener(
        self,
        timeout_s: float = 2.0,
    ) -> bool:

        if (
            self._metrics_thread is not None
            and self._metrics_thread.is_alive()
        ):
            return True


        self._metrics_stop_event.clear()
        self._metrics_ready_event.clear()


        self._metrics_thread = (
            threading.Thread(
                target=(
                    self._metrics_listener_loop
                ),
                daemon=True,
                name="acquisition-metrics",
            )
        )

        self._metrics_thread.start()


        if not self._metrics_ready_event.wait(
            timeout=timeout_s
        ):

            self.logger.error(
                "Timeout starting acquisition "
                "metrics listener"
            )

            return False


        return (
            self._metrics_thread.is_alive()
        )


    def stop_metrics_listener(
        self,
    ) -> None:

        self._metrics_stop_event.set()

        thread = (
            self._metrics_thread
        )

        if (
            thread is not None
            and thread.is_alive()
        ):

            thread.join(
                timeout=2.0
            )

        self._metrics_thread = None