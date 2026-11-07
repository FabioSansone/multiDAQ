from typing import Optional
from client.utils.logger import get_logger
from client.communication.identity import ClientIdentity
from client.hardware.hv.hv_service import HVService
from client.hardware.hv.hv_messages import HVMessagePriority
from client.hardware.rc.rc_service import RCService
from client.hardware.main.main_service import MainService
from client.hardware.evproducer.ev_service import EVService
from client.acquisition.acquisition_service import AcquisitionService
from client.core.monitor_sample_service import MonitorSampleService
from client.hardware.feb.feb_service import FEBService
from common.message_handler import MessageStatus, Channel
from client.utils.channels import channels_definition
from client.hardware.rc.rc_interface import RC_HARDWARE_SNAPSHOT
from common.timeouts import client_timeout



import queue
import threading
import time


SHUTDOWN_ZERO_REGISTERS = [19, 15, 1, 0, 39, 16, 18]
# RC1 and RC0 keep the FEBs in data mode, and the HV boards answer on the Modbus only through the FEBs:
# the safe state zeroes them after the HV off, not before
SAFE_STATE_FIRST_REGISTERS = [19, 15]
SAFE_STATE_LAST_REGISTERS = [1, 0, 39, 16, 18]
RC39_CHANNEL_MASK = 0x7F
RC39_EXTERNAL_RATE_BIT = 1 << 7
RC39_AUTO_RATE_BIT = 1 << 8
RC39_TRIGGER_RATE_MASK = (RC39_EXTERNAL_RATE_BIT | RC39_AUTO_RATE_BIT)


class ClientRunTime:
    def __init__(
        self,
        identity: ClientIdentity,
        server_ip: str,
        hv_port: str,
    ) -> None:
        self.logger = get_logger("client_runtime")

        self.identity = identity
        self.server_ip = server_ip
        self.hv_port = hv_port
        self.cached_i2c_bus = self.identity.get_i2cbus()
        
        self.monitor_event_queue = queue.Queue()

        self.hv_service: Optional[HVService] = None
        self.rc_service: RCService = RCService(configuration_change_callback=self._on_rc_configuration_change)
        self.rc_service.start()
        self.evproducer = EVService()
        self.feb_service = FEBService(self)
        self.main_service: MainService = MainService(cached_i2c_bus=self.cached_i2c_bus)
        self.sync_sensor_i2c_bus_cache()
        self.main_service.start()
        
        self.monitor_sample_service = MonitorSampleService(main_service=self.main_service, rc_service=self.rc_service, hv_service=self.hv_service, client_identity=self.zmq_identity(),)
        self.monitor_sample_service.start()
        

        self.acq_mode: Optional[str] = None
        self.acq_info: Optional[dict] = None
        self.start_thr: Optional[int | float] = None

        self.acquisition_service = AcquisitionService(self)
        
        self.mac_to_id: int | None = None
        
        self.channel_hv_parameters: dict[int, dict] = {}
        
        
        
        self._closed = False
        self._safe_state_lock = threading.Lock()
        self._acq_safe_lock = threading.Lock()
        # Two planes can ask for the HVService together: only one must open the serial line
        self._hv_service_lock = threading.Lock()
        
        self.boot_id = self._read_boot_id()

        self.logger.info("ClientRuntime initialized")

    def _sanitize_identity_part(self, value: str) -> str:
        return (
            str(value)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
        )
    
    @staticmethod
    def _read_boot_id() -> str:
        with open(
            "/proc/sys/kernel/random/boot_id",
            "r",
            encoding="utf-8",
        ) as f:
            return f.read().strip()

    def zmq_identity(self) -> str:
        multipmt_id = self._sanitize_identity_part(
            self.identity.multipmt_id or "unknown"
        )
        batch_id = self._sanitize_identity_part(
            self.identity.batch_id or "unknown"
        )
        mac_suffix = self.identity.mac.replace(":", "")[-6:]

        return f"{multipmt_id}-{batch_id}-{mac_suffix}"

    def ensure_hv_service(self, fixed_bad_channels: list[int] | None = None, hv_policy: str | None = None) -> bool:
        """hv_policy is used only if the service has to be created: an existing one keeps its own."""
        with self._hv_service_lock:
            if self.hv_service is not None:
                return True

            # The HVService checks the address of every channel when it is created, and the HV boards
            # answer only if the FEBs are in data mode: the safe state leaves them out of it
            if not self._open_hv_link():
                self.logger.warning("HV link not opened: the channels may not answer to the address check")

            try:
                policy_kwargs = {"hv_policy": hv_policy} if hv_policy is not None else {}
                self.hv_service = HVService(hv_port=self.hv_port,fixed_bad_channels=fixed_bad_channels or [], state_change_callback=self.sync_rc_register_39_with_hv, hv_parameters_callback=self.update_hv_parameters_after_command, **policy_kwargs)
                self.monitor_sample_service.set_hv_service(self.hv_service)
                self.logger.info("HVService initialized")
                return True

            except Exception as e:
                self.logger.error(f"Cannot initialize HVService: {e}")
                self.hv_service = None
                return False

    def stop_hv_service(self) -> None:
        if self.hv_service is None:
            return

        try:
            self.monitor_sample_service.set_hv_service(None)
            self.hv_service.stop()
            self.logger.info("HVService stopped")
        except Exception as e:
            self.logger.error(f"Error while stopping HVService: {e}")
        finally:
            self.hv_service = None


    def stop_rc_service(self) -> None:
        if self.rc_service is None:
            return

        try:
            self.rc_service.stop()
            self.logger.info("RCService stopped")
        except Exception as e:
            self.logger.error(f"Error while stopping RCService: {e}")
        finally:
            self.rc_service = None
            
    def stop_main_service(self) -> None:
        if self.main_service is None:
            return

        try:
            self.main_service.stop()
            self.logger.info("MainService stopped")
        except Exception as e:
            self.logger.error(f"Error while stopping MainService: {e}")
        finally:
            self.main_service = None
    
    def sync_rc_register_39_with_hv(self) -> bool:
        if self.hv_service is None:
            self.logger.warning("Cannot sync RC register 39: HVService unavailable")
            return False

        ok_channels = set(self.hv_service.hv.getOkChannels())
        on_channels = set(self.hv_service.hv.getOnChannels())

        if self.acq_mode == "test":
            hv_enabled_channels = sorted(ok_channels)
        else:
            hv_enabled_channels = sorted(ok_channels & on_channels)

        rc_channels = [ch - 1 for ch in hv_enabled_channels]

        mask = 0
        for ch in rc_channels:
            if ch < 0 or ch >= 7:
                self.logger.error(f"Invalid RC channel derived from HV: {ch}")
                return False

            mask |= 1 << ch


        read_response = self.rc_service._submit_command(
            command="rc_read_register",
            payload={"address": 39},
            sender="client_runtime_rc39_sync",
        )

        if read_response.status != MessageStatus.OK:
            self.logger.error(f"Failed to read RC register 39 before HV sync: {read_response.error}")
            return False

        current_value = read_response.result.get("value", 0)
        new_value = mask | RC39_TRIGGER_RATE_MASK
        if current_value == new_value:
            return True


        response = self.rc_service._submit_command(
            command="rc_write_register",
            payload={
                "address": 39,
                "value": new_value,
            },
            sender="client_runtime_rc39_sync",
        )

        if response.status != MessageStatus.OK:
            self.logger.error(
                f"Failed to sync RC register 39 with HV state: {response.error}"
            )
            return False

        self.logger.debug(
            "RC register 39 synchronized: "
            f"mode={self.acq_mode}, "
            f"hv_channels={hv_enabled_channels}, "
            f"rc_channels={rc_channels}, "
            f"channel_mask={mask}, "
            f"reg39={new_value}"
        )

        return True

    def set_acquisition_mode(
        self,
        acq_mode: str,
        acq_info: dict | None = None,
        start_thr: int | float | None = None,
    ) -> None:
        previous_mode = self.acq_mode
        self.acq_mode = acq_mode
        self.acq_info = acq_info
        self.start_thr = start_thr
        
        if previous_mode == "multipmt" and acq_mode != "multipmt" and self.hv_service is not None:
            self.hv_service.stop_acq_check()

        self.logger.info(
            f"Client runtime acquisition mode set to {acq_mode}"
        )

    def clear_acquisition_configuration(self) -> None:
        self.acq_info = None
        self.start_thr = None
        self.logger.debug("Acquisition configuration cleared")

    def zero_rc_registers_on_shutdown(self, registers: list[int] | None = None) -> bool:
        overall_success = True

        for address in registers or SHUTDOWN_ZERO_REGISTERS:
            response = self.rc_service._submit_command(
                command="rc_write_register",
                payload={
                    "address": address,
                    "value": 0,
                },
                sender="client_runtime_shutdown",
            )

            if response.status != MessageStatus.OK:
                overall_success = False
                self.logger.error(
                    f"Failed to reset RC register {address} on shutdown: "
                    f"{response.error}"
                )
                continue

            self.logger.info(
                f"RC register {address} reset to 0 on shutdown"
            )

        return overall_success

    def close(self) -> None:
        
        if self._closed:
            self.logger.debug(
                "ClientRuntime already closed"
            )
            return

        self._closed = True
        
        self.monitor_sample_service.stop()
        self.stop_main_service()
        self.stop_hv_service()

        try:
            self.evproducer.stop()
        except Exception as e:
            self.logger.error(f"Error while stopping evproducer: {e}")

        rc_zero_ok = self.zero_rc_registers_on_shutdown()
        if not rc_zero_ok:
            self.logger.warning("ClientRuntime closed with RC shutdown reset errors")

        self.stop_rc_service()

        self.logger.info("ClientRuntime closed")
    
    def set_mac_to_id(self, mac_to_id:int):
        if self.mac_to_id is not None:
            self.logger.warning("Id associated to client mac already exists. Overwriting...")
            self.mac_to_id = mac_to_id
        else:
            self.mac_to_id = mac_to_id
    
    def get_mac_to_id(self):
        if self.mac_to_id is not None:
            return self.mac_to_id
        else:
            self.logger.warning(
                "get_mac_to_id called before a numeric client id was assigned "
                "during handshake; returning None."
            )
            return
    
    
    def store_multipmt_hv_parameters(self, acq_info: dict) -> None:

        for key, ch_config in acq_info.items():
            try:
                external_ch = int(key)
            except (TypeError, ValueError):
                continue

            hv_channel = external_ch + 1
            entry = self.channel_hv_parameters.setdefault(hv_channel, {})

            if "voltage" in ch_config:
                entry["voltage"] = ch_config["voltage"]
            if "threshold" in ch_config:
                entry["threshold"] = ch_config["threshold"]

        self.logger.info(
            f"Stored multiPMT HV parameters for channels: {sorted(self.channel_hv_parameters.keys())}"
        )
        
    
    def update_hv_parameters_after_command(self, command:str, hv_request, hv_response):
        
        if self.acq_mode != "multipmt":
            return
        
        if hv_response.status != MessageStatus.OK:
            return
        
        successful = set(hv_response.result.get("successful_channels", []))
        if not successful:
            return
        
        if command == "set_common_voltage":
            voltage = hv_request.payload.get("common_voltage")
            for ch in successful:
                self.channel_hv_parameters.setdefault(ch, {})["voltage"] = voltage
                
        elif command == "set_common_threshold":
            threshold = hv_request.payload.get("common_threshold")
            for ch in successful:
                self.channel_hv_parameters.setdefault(ch, {})["threshold"] = threshold
        
        elif command == "set_acquisition_configuration":
            acq_configuration = hv_request.payload.get("acquisition_configuration", {})
            for ch in successful:
                external_ch = ch - 1
                ch_config = acq_configuration.get(external_ch) or acq_configuration.get(str(external_ch))
                if not ch_config:
                    continue
                entry = self.channel_hv_parameters.setdefault(ch, {})
                if "voltage" in ch_config:
                    entry["voltage"] = ch_config["voltage"]
                if "threshold" in ch_config:
                    entry["threshold"] = ch_config["threshold"]
                    
    
    def sync_sensor_i2c_bus_cache(self) -> None:
        detected_i2c_bus = self.main_service.get_i2c_bus()

        if detected_i2c_bus is None:
            self.logger.warning(
                "Cannot update Main Board I2C bus cache: "
                "no valid sensor bus detected"
            )
            return

        if detected_i2c_bus == self.cached_i2c_bus:
            self.logger.debug(
                f"Main Board sensor I2C bus cache already valid: "
                f"{detected_i2c_bus}"
            )
            return

        self.identity.set_i2cbus(
            detected_i2c_bus
        )

        self.logger.info(
            "Main Board sensor I2C bus cache updated: "
            f"{self.cached_i2c_bus} -> {detected_i2c_bus}"
        )

        self.cached_i2c_bus = detected_i2c_bus   
        
        
    def emit_monitoring_event(
        self,
        *,
        channel: Channel,
        event: str,
        severity: str = "info",
        details: dict | None = None,
        source_request_id: str | None = None,
        error=None,
    ) -> None:

        payload = {
            "event": event,
            "severity": severity,
            "event_monotonic_ns": (
                time.monotonic_ns()
            ),
            "source_request_id": (
                source_request_id
            ),
            "details": details or {},
            "error": error,
        }

        self.monitor_event_queue.put(
            {
                "channel": channel,
                "payload": payload,
            }
        )    
        
    def _on_rc_configuration_change(
        self,
        *,
        register: int,
        old_value,
        new_value,
        reason: str,
        source_request_id: str | None = None,
    ) -> None:

        if register not in {
            31,
            39,
        }:
            return

        if old_value == new_value:
            return

        self.emit_monitoring_event(
            channel=Channel.RC,
            event="rc_configuration_changed",
            severity="info",
            source_request_id=(
                source_request_id
            ),
            details={
                "register": register,
                "old_value": old_value,
                "new_value": new_value,
                "reason": reason,
            },
        )


    def get_multipmt_configuration(self, channels: list[int] | str = "all", *, rc_flag: bool = False, hv_flag: bool = False, power_flag: bool = False, serial_flag: bool = False) -> dict:
        """Hardware values the file header needs, read in one place.

        channels: user numbering (0..6) or "all". The result is keyed by user channel.
        - rc_flag: the RC acquisition registers;
        - hv_flag: voltage and threshold set on each channel;
        - power_flag: state of each channel (ok/bad/...) and whether it is on or off.
          Both come from the channel lists the HV service keeps, no wait on the hardware;
        - serial_flag: the PMT serial of each channel ("0" if missing or unreadable).

        A section that cannot be read is reported in "errors" and left empty or None:
        it does not raise, and one section failing does not stop the others.
        "success" is True only if every requested section was read for every channel."""

        hv_channels = channels_definition(channels=channels, hv_channels=True)

        snapshot = {
            "success": True,
            "registers": {},
            "hv_parameters": {},
            "channel_state": {},
            "power_state": {},
            "serials": {},
            "errors": {},
        }

        if rc_flag:
            rc_error = None

            rc_response = self.rc_service._submit_command(
                command="rc_read_acq_registers",
                payload={"rc_acq_registers": RC_HARDWARE_SNAPSHOT},
                sender="client_runtime_multipmt_configuration",
            )

            if rc_response.status != MessageStatus.OK:
                rc_error = rc_response.error
            else:
                values = (rc_response.result or {}).get("value", {})
                snapshot["registers"] = {int(address): value for address, value in values.items()}

                unread_registers = sorted(address for address, value in snapshot["registers"].items() if value is None)
                if unread_registers:
                    rc_error = f"registers not read: {unread_registers}"

            if rc_error is not None:
                snapshot["success"] = False
                snapshot["errors"]["rc"] = str(rc_error)
                self.logger.error(f"Failed to read RC registers for the run snapshot: {rc_error}")

        if hv_flag:
            hv_error = None

            if self.hv_service is None:
                hv_error = "HVService unavailable"
            else:
                hv_response = self.hv_service._submit_command(
                    command="get_volt_thr_channels",
                    payload={"channels": hv_channels},
                    sender="client_runtime_multipmt_configuration",
                )

                # One failing channel makes the response an error, but the others were read.
                configuration = (hv_response.result or {}).get("volt_thr_configuration", {})

                for hv_channel in hv_channels:
                    values = configuration.get(hv_channel) or {}
                    snapshot["hv_parameters"][hv_channel - 1] = {
                        "voltage": values.get("voltage"),
                        "threshold": values.get("threshold"),
                    }

                unread_channels = [
                    channel for channel, values in snapshot["hv_parameters"].items()
                    if values["voltage"] is None or values["threshold"] is None
                ]

                if hv_response.status != MessageStatus.OK:
                    hv_error = hv_response.error
                elif unread_channels:
                    hv_error = f"channels not read: {unread_channels}"

            if hv_error is not None:
                snapshot["success"] = False
                snapshot["errors"]["hv"] = str(hv_error)
                self.logger.error(f"Failed to read HV voltage/threshold for the run snapshot: {hv_error}")

        if power_flag:
            power_error = None

            if self.hv_service is None:
                power_error = "HVService unavailable"
            else:
                lists_response = self.hv_service._submit_command(
                    command="hv_channel_lists",
                    payload={},
                    sender="client_runtime_multipmt_configuration",
                )

                if lists_response.status != MessageStatus.OK:
                    power_error = lists_response.error
                else:
                    lists = lists_response.result or {}
                    ok_channels = set(lists.get("ok_channels", []))
                    on_channels = set(lists.get("on_channels", []))
                    off_channels = set(lists.get("off_channels", []))
                    bad_channels = set(lists.get("bad_channels", []))
                    fixed_bad_channels = set(lists.get("fixed_bad_channels", []))
                    missing_serial_channels = set(lists.get("missing_serial_channels", []))

                    for hv_channel in hv_channels:
                        if hv_channel in fixed_bad_channels:
                            channel_state = "fixed_bad"
                        elif hv_channel in missing_serial_channels:
                            channel_state = "missing_serial"
                        elif hv_channel in bad_channels:
                            channel_state = "bad"
                        elif hv_channel in ok_channels:
                            channel_state = "ok"
                        else:
                            channel_state = "unknown"

                        if channel_state == "ok" and hv_channel in on_channels:
                            power_state = "on"
                        elif channel_state == "ok" and hv_channel in off_channels:
                            power_state = "off"
                        else:
                            power_state = "unknown"

                        snapshot["channel_state"][hv_channel - 1] = channel_state
                        snapshot["power_state"][hv_channel - 1] = power_state

            if power_error is not None:
                snapshot["success"] = False
                snapshot["errors"]["power"] = str(power_error)
                self.logger.error(f"Failed to read HV channel states for the run snapshot: {power_error}")

        if serial_flag:
            serial_error = None

            if self.hv_service is None:
                serial_error = "HVService unavailable"
            else:
                serial_response = self.hv_service._submit_command(
                    command="get_serial_map",
                    payload={"channels": hv_channels},
                    sender="client_runtime_multipmt_configuration",
                )

                if serial_response.status != MessageStatus.OK:
                    serial_error = serial_response.error
                else:
                    serial_map = (serial_response.result or {}).get("serial_map", {})

                    for hv_channel in hv_channels:
                        snapshot["serials"][hv_channel - 1] = serial_map.get(hv_channel, "0")

            if serial_error is not None:
                snapshot["success"] = False
                snapshot["errors"]["serial"] = str(serial_error)
                self.logger.error(f"Failed to read PMT serials for the run snapshot: {serial_error}")

        return snapshot


    def _read_rc_register(self, address: int) -> int | None:
        response = self.rc_service._submit_command(
            command="rc_read_register",
            payload={"address": address},
            sender="client_runtime",
        )

        if response.status != MessageStatus.OK:
            self.logger.error(f"Cannot read RC register {address}: {response.error}")
            return None

        return (response.result or {}).get("value")

    def _write_rc_register(self, address: int, value: int) -> bool:
        response = self.rc_service._submit_command(
            command="rc_write_register",
            payload={"address": address, "value": value},
            sender="client_runtime",
        )

        if response.status != MessageStatus.OK:
            self.logger.error(f"Cannot write RC register {address}={value}: {response.error}")
            return False

        return True

    def _open_hv_link(self) -> bool:
        """Put the FEBs in data mode (rc_acq_start: RC1, RC0 and RC39), the only way to reach the HV
        boards on the Modbus. RC19 is zeroed first, so that nothing can be acquired while they are on."""
        if self.rc_service is None:
            self.logger.error("Safe state: RCService unavailable, the HV boards cannot be reached")
            return False

        for command, payload in (
            ("rc_write_register", {"address": 19, "value": 0}),
            ("rc_acq_start", {"channels": "all"}),
        ):
            response = self.rc_service._submit_command(
                command=command,
                payload=payload,
                sender="client_runtime_safe_state",
            )

            if response.status != MessageStatus.OK:
                self.logger.error(f"Safe state: the HV boards cannot be reached, {command} failed: {response.error}")
                return False

        return True

    def startup_safe_state(self) -> bool:
        """Bring the board to a known safe state when the client starts: the previous client
        may have died without running enter_safe_state (kill -9, crash, power loss), leaving
        the channels enabled, an orphan evproducer and the HV on.

        The HVService does not exist yet at this point (the server asks for it later), so it
        is created for the occasion and dropped afterwards: the server's own request, with
        the fixed-bad channels, must find it as before. Can last minutes (HV ramp down)."""

        self.logger.warning("Startup: bringing the board to the safe state")

        success = self.evproducer.kill_stale_processes()

        hv_created = False
        if self.hv_service is None:
            # The HVService checks the address of every channel when it starts: the FEBs must already answer
            if not self._open_hv_link():
                success = False

            hv_created = self.ensure_hv_service()
            if not hv_created:
                self.logger.error("Startup: HVService unavailable, HV not switched off")
                success = False
            else:
                # The thread that executes the HV commands: the AcquisitionService starts it, but it does not run yet
                self.hv_service.start()

        try:
            success = self.enter_safe_state() and success
        finally:
            if hv_created:
                self.stop_hv_service()

        if success:
            self.logger.info("Startup safe state reached")
        else:
            self.logger.error("Startup safe state reached with errors")

        return success

    def enter_safe_state(self) -> bool:
        """Switch everything off without closing the services, so that the client can go
        back to the handshake: channels disabled, producer stopped, HV off, test mode.

        Every step runs even if a previous one failed. It can last minutes (HV ramp down),
        so it must not run in a thread that has to keep serving the sockets. A second call
        while one is running does nothing. Returns True only if every step succeeded.
        """
        if not self._safe_state_lock.acquire(blocking=False):
            self.logger.warning("Safe state already in progress, nothing to do")
            return True

        try:
            self.logger.warning("Entering safe state")
            overall_success = True

            # The recovery worker would switch the channels on again
            if self.hv_service is not None:
                try:
                    self.hv_service.stop_acq_check()
                except Exception:
                    self.logger.exception("Safe state: cannot stop the acquisition recovery worker")
                    overall_success = False

            # RC19 first: the channels are disabled before anything else is stopped
            if self.rc_service is not None:
                try:
                    if not self.zero_rc_registers_on_shutdown(SAFE_STATE_FIRST_REGISTERS):
                        self.logger.error("Safe state: some RC registers were not reset to 0")
                        overall_success = False
                except Exception:
                    self.logger.exception("Safe state: cannot reset the RC registers")
                    overall_success = False
            else:
                self.logger.error("Safe state: RCService unavailable, RC registers not reset")
                overall_success = False

            try:
                if not self.evproducer.stop():
                    self.logger.error("Safe state: evproducer did not stop")
                    overall_success = False
            except Exception:
                self.logger.exception("Safe state: cannot stop the evproducer")
                overall_success = False

            # No hardware reset pulse here: without the software RESET of the producer before and after it
            # it leaves the DMA stuck. The server does software-hardware-software before every acquisition.

            if self.hv_service is not None:
                # RC1 and RC0 are still on only if nothing zeroed them since the last mode: do not count on it
                if not self._open_hv_link():
                    overall_success = False

                try:
                    response = self.hv_service._submit_command(
                        command="hv_off_and_wait",
                        payload={"channels": "all"},
                        sender="client_runtime",
                        priority=HVMessagePriority.EMERGENCY,
                        timeout_s=client_timeout("hv_off_and_wait"),
                    )

                    if response.status != MessageStatus.OK:
                        self.logger.error(f"Safe state: HV off failed: {response.error}")
                        overall_success = False
                    else:
                        skipped = (response.result or {}).get("skipped_channels", [])
                        if skipped:
                            # off_and_wait leaves the channels that are not OK alone: switch them off anyway
                            self.logger.warning(f"Safe state: forcing OFF the HV channels that are not OK: {skipped}")
                            force_response = self.hv_service._submit_command(
                                command="hv_force_off",
                                payload={"channels": skipped},
                                sender="client_runtime",
                                priority=HVMessagePriority.EMERGENCY,
                                timeout_s=client_timeout("hv_force_off"),
                            )

                            if force_response.status != MessageStatus.OK:
                                self.logger.error(f"Safe state: forced HV off failed: {force_response.error}")
                                overall_success = False
                            else:
                                failed = (force_response.result or {}).get("failed_channels", [])
                                if failed:
                                    self.logger.error(f"Safe state: HV channels that could not be forced OFF: {failed}")
                                    overall_success = False
                except Exception:
                    self.logger.exception("Safe state: cannot switch the HV off")
                    overall_success = False
            else:
                self.logger.warning("Safe state: HVService unavailable, HV not switched off")

            # After the HV off: the FEBs are not needed any more
            if self.rc_service is not None:
                try:
                    if not self.zero_rc_registers_on_shutdown(SAFE_STATE_LAST_REGISTERS):
                        self.logger.error("Safe state: some RC registers were not reset to 0")
                        overall_success = False
                except Exception:
                    self.logger.exception("Safe state: cannot reset the RC registers")
                    overall_success = False

            self.set_acquisition_mode("test")

            if overall_success:
                self.logger.info("Safe state reached")
            else:
                self.logger.error("Safe state reached with errors")

            return overall_success

        finally:
            self._safe_state_lock.release()


    def enter_acquisition_safe_state(self) -> bool:
        if not self._acq_safe_lock.acquire(blocking=False):
            self.logger.warning("Acquisition safe state already in progress, nothing to do")
            return True

        try:
            self.logger.warning("Entering acquisition safe state")
            overall_success = True

            # Only RC19: the other registers (RC39 follows the HV, which stay as they are) are left alone
            if self.rc_service is not None:
                try:
                    if not self._write_rc_register(19, 0):
                        self.logger.error("Acquisition safe state: RC19 not reset to 0")
                        overall_success = False
                except Exception:
                    self.logger.exception("Acquisition safe state: cannot reset RC19")
                    overall_success = False
            else:
                self.logger.error("Acquisition safe state: RCService unavailable, RC19 not reset")
                overall_success = False

            try:
                if not self.evproducer.stop():
                    self.logger.error("Safe state: evproducer did not stop")
                    overall_success = False
            except Exception:
                self.logger.exception("Safe state: cannot stop the evproducer")
                overall_success = False

            # No hardware reset pulse here: without the software RESET of the producer before and after it
            # it leaves the DMA stuck. The server does software-hardware-software before every acquisition.

            if overall_success:
                self.logger.info("Acquisition safe state reached")
            else:
                self.logger.error("Acquisition safe state reached with errors")

            return overall_success

        finally:
            self._acq_safe_lock.release()
