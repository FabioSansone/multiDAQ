import threading
import queue
import time
import hashlib

from client.utils.logger import get_logger
from common.message_handler import Channel, MessageStatus

from client.hardware.rc.rc_messages import RCMessagePriority
from client.hardware.hv.hv_messages import HVMessagePriority
from client.hardware.main.main_messages import MainMessagePriority

class MonitorSampleService:
    
    SUPPORTED_CHANNELS = {
        Channel.RC,
        Channel.HV,
        Channel.MAIN,
    }
    
    def __init__(self, main_service, rc_service, hv_service, client_identity: str,):
        
        self.main_service = main_service
        self.rc_service = rc_service
        self.hv_service = hv_service
        
        self.client_identity = client_identity
        
        self.config = {
            Channel.RC: {
                "enabled": False,
                "interval_s": None,
            },
            
            Channel.HV: {
                "enabled": False,
                "interval_s": None,
            },
            
            Channel.MAIN: {
                "enabled": False,
                "interval_s": None,
            },
        }
        
        self.sample_queues = {
            Channel.RC: queue.Queue(maxsize=1),
            Channel.HV: queue.Queue(maxsize=1),
            Channel.MAIN: queue.Queue(maxsize=1)
        }
        
        self.sequence = {
            Channel.RC: 0,
            Channel.HV: 0,
            Channel.MAIN: 0,
        }
        
        self.stop_event = threading.Event()

        self.section_threads = {
            Channel.RC: None,
            Channel.HV: None,
            Channel.MAIN: None,
        }
        
        self.section_wakeup = {
            Channel.RC: threading.Event(),
            Channel.HV: threading.Event(),
            Channel.MAIN: threading.Event(),
        }
        
        self.config_lock = threading.Lock()
        
        self.logger = get_logger("monitor_service")
        self.logger.info("MonitorSampleService initialized")
        
    def set_hv_service(self, hv_service) -> None:
        self.hv_service = hv_service
        
    def _store_sample(self, channel: Channel, sample: dict) -> None:
        
        sample_queue = self.sample_queues[channel]
        
        try:
            sample_queue.put_nowait(sample)
            return
        except queue.Full:
            pass
        
        try:
            sample_queue.get_nowait()
        except queue.Empty:
            pass
        
        try:
            sample_queue.put_nowait(sample)
        except queue.Full:
            self.logger.debug(
                f"Sample queue still full for {channel.value}; "
                "dropping sample"
            )
    
    def _initial_offset(self, channel: Channel, interval_s: float) -> float:
        
        key = (f"{self.client_identity}:{channel.value}")
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        raw_value = int.from_bytes(digest[:8], byteorder="big",signed=False)
        
        fraction = raw_value / float(2**64)
        return fraction * interval_s
        
    
    def get_rc_sample(self) -> dict | None:

            response = self.rc_service._submit_command(
                command="rc_all_rate_monitoring",
                payload={
                    "channels": "all",
                },
                sender="monitor_sample_service",
                priority=RCMessagePriority.MONITORING,
                timeout_s=35.0,
            )

            if response.status != MessageStatus.OK:
                self.logger.warning(
                    f"Failed to acquire RC monitoring sample: "
                    f"{response.error}"
                )
                return None

            return response.result
    
    
    def get_main_sample(self) -> dict | None:

        response = self.main_service._submit_command(
            command="main_read_snapshot",
            payload={},
            sender="monitor_sample_service",
            priority=MainMessagePriority.MONITORING,
            timeout_s=35.0,
        )

        if response.status != MessageStatus.OK:
            self.logger.warning(
                f"Failed to acquire MAIN monitoring sample: "
                f"{response.error}"
            )
            return None

        result = response.result or {}

        return {
            "env": result.get("env", {}),
            "power": result.get("power", {}),
            "fpga": result.get("fpga", {}),
        }
    
    
    def get_hv_sample(self) -> dict | None:

        hv_service = self.hv_service

        if hv_service is None:
            self.logger.warning(
                "Cannot acquire HV monitoring sample: "
                "HVService unavailable"
            )
            return None

        response = hv_service._submit_command(
            command="hv_monitor_snapshot",
            payload={
                "channels": "all",
            },
            sender="monitor_sample_service",
            priority=HVMessagePriority.MONITORING,
            timeout_s=60.0,
        )

        if response.status != MessageStatus.OK:
            self.logger.warning(
                f"HV monitoring sample completed with errors: "
                f"{response.error}"
            )

        return response.result or {}

    
    def _produce_sample(self, channel: Channel) -> None:
        
        sample_monotonic_ns = time.monotonic_ns()
        
        if channel == Channel.RC:
            data = self.get_rc_sample()
        elif channel == Channel.HV:
            data = self.get_hv_sample()
        elif channel == Channel.MAIN:
            data = self.get_main_sample()
        else:
            self.logger.error(f"Unsupported sample channel: {channel}")
            return
        if data is None:
            return
        
        sequence = self.sequence[channel]
        self.sequence[channel] += 1
        
        sample = {
            "sequence": sequence,
            "sample_monotonic_ns": sample_monotonic_ns,
            "data": data,
        }
        
        self._store_sample(
            channel=channel,
            sample=sample,
        )
    
    def _section_loop(
        self,
        channel: Channel,
    ) -> None:

        next_run = None

        while not self.stop_event.is_set():

            with self.config_lock:
                config = dict(
                    self.config[channel]
                )

            if not config["enabled"]:
                next_run = None

                self.section_wakeup[channel].wait(
                    timeout=0.5
                )

                self.section_wakeup[channel].clear()
                continue

            interval_s = config["interval_s"]

            if interval_s is None:
                self.logger.error(
                    f"{channel.value} sample stream enabled "
                    "without interval"
                )

                self.section_wakeup[channel].wait(
                    timeout=0.5
                )

                self.section_wakeup[channel].clear()
                continue

            if next_run is None:
                offset_s = self._initial_offset(
                    channel=channel,
                    interval_s=interval_s,
                )

                next_run = (
                    time.monotonic()
                    + offset_s
                )

            now = time.monotonic()

            if now < next_run:
                wait_s = next_run - now

                woke_up = self.section_wakeup[channel].wait(
                    timeout=wait_s
                )

                self.section_wakeup[channel].clear()

                if woke_up:
                    next_run = None

                continue

            self._produce_sample(channel)

            next_run += interval_s

            now = time.monotonic()

            while next_run <= now:
                next_run += interval_s
        
            
    
    def start_section(
        self,
        channel: Channel,
        interval_s: float,
    ) -> bool:

        if channel not in self.SUPPORTED_CHANNELS:
            self.logger.error(
                f"Unsupported monitoring sample channel: "
                f"{channel}"
            )
            return False

        try:
            interval_s = float(interval_s)
        except (TypeError, ValueError):
            self.logger.error(
                f"Invalid interval for {channel.value}: "
                f"{interval_s}"
            )
            return False

        if interval_s <= 0:
            self.logger.error(
                f"Interval must be > 0 for "
                f"{channel.value}"
            )
            return False

        with self.config_lock:
            self.config[channel]["enabled"] = True
            self.config[channel]["interval_s"] = interval_s

        self.section_wakeup[channel].set()

        self.logger.info(
            f"{channel.value.upper()} monitoring samples enabled: "
            f"interval={interval_s}s"
        )

        return True
        
    
    def stop_section(
        self,
        channel: Channel,
    ) -> bool:

        if channel not in self.SUPPORTED_CHANNELS:
            self.logger.error(
                f"Unsupported monitoring sample channel: "
                f"{channel}"
            )
            return False

        with self.config_lock:
            self.config[channel]["enabled"] = False
            self.config[channel]["interval_s"] = None

        self.section_wakeup[channel].set()

        sample_queue = self.sample_queues[channel]

        while True:
            try:
                sample_queue.get_nowait()
            except queue.Empty:
                break

        self.logger.info(
            f"{channel.value.upper()} monitoring samples disabled"
        )

        return True
    
    
    def start(self) -> None:

        self.stop_event.clear()

        for channel in self.SUPPORTED_CHANNELS:

            thread = self.section_threads[channel]

            if thread is not None and thread.is_alive():
                continue

            thread = threading.Thread(
                target=self._section_loop,
                args=(channel,),
                daemon=True,
                name=f"monitor-sample-{channel.value}",
            )

            self.section_threads[channel] = thread
            thread.start()

        self.logger.info(
            "MonitorSampleService workers started"
        )
    
    
    def stop(self) -> None:

        self.stop_event.set()

        for wakeup in self.section_wakeup.values():
            wakeup.set()

        for channel, thread in self.section_threads.items():

            if thread is not None and thread.is_alive():
                thread.join(timeout=2.0)

        self.logger.info(
            "MonitorSampleService workers stopped"
        )
    
