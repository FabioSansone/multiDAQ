from client.hardware.hv.hvmodbus import HVModBus
from client.utils.logger import get_logger
import threading
from typing import List
import time
import numpy as np



class HV:
    
    def __init__(self, hv_port, fixed_bad_channels):
        self.logger = get_logger('hv')
        self.hv = HVModBus(hv_port)
        
        self.channels_lock = threading.Lock()
        self.on_ch = []
        self.off_ch = []
        self.fixed_bad = []
        self.missing_serial = []

        scan_channels = [ch for ch in range(1, 8) if ch not in (fixed_bad_channels or [])]
        self.ok_ch, self.bad_ch = self.checkChannel(channels=scan_channels)

        for ch in (fixed_bad_channels or []):
            self.moveToFixedBad(ch)
        
        self.sync_power_state(channels=self.ok_ch)
        
        

    def getOkChannels(self):
        with self.channels_lock:
            return list(self.ok_ch)

    def getBadChannels(self):
        with self.channels_lock:
            return list(self.bad_ch)

    def getOnChannels(self):
        with self.channels_lock:
            return list(self.on_ch)

    def getOffChannels(self):
        with self.channels_lock:
            return list(self.off_ch)
        
    def getOkOffChannels(self):
        with self.channels_lock:
            return sorted(set(self.ok_ch) & set(self.off_ch))
    
    def getFixedBad(self):
        with self.channels_lock:
            return list(self.fixed_bad)
    
    def getMissingSerial(self):
            with self.channels_lock:
                return list(self.missing_serial)

    def moveToOk(self, channel: int) -> None:
        with self.channels_lock:
            if channel in self.bad_ch:
                self.bad_ch.remove(channel)

            if channel not in self.ok_ch:
                self.ok_ch.append(channel)

            if channel not in self.off_ch and channel not in self.on_ch:
                self.off_ch.append(channel)

            self.ok_ch = sorted(self.ok_ch)
            self.bad_ch = sorted(self.bad_ch)
            self.off_ch = sorted(self.off_ch)

    def moveToBad(self, channel: int) -> None:
        with self.channels_lock:
            if channel in self.ok_ch:
                self.ok_ch.remove(channel)

            if channel not in self.bad_ch:
                self.bad_ch.append(channel)

            if channel in self.on_ch:
                self.on_ch.remove(channel)

            if channel not in self.off_ch:
                self.off_ch.append(channel)

            self.ok_ch = sorted(self.ok_ch)
            self.bad_ch = sorted(self.bad_ch)
            self.on_ch = sorted(self.on_ch)
            self.off_ch = sorted(self.off_ch)

    def moveToOn(self, channel: int) -> None:
        with self.channels_lock:
            if channel not in self.ok_ch:
                return

            if channel in self.off_ch:
                self.off_ch.remove(channel)

            if channel not in self.on_ch:
                self.on_ch.append(channel)

            self.on_ch = sorted(self.on_ch)
            self.off_ch = sorted(self.off_ch)

    def moveToOff(self, channel: int) -> None:
        with self.channels_lock:
            if channel in self.on_ch:
                self.on_ch.remove(channel)

            if channel not in self.off_ch:
                self.off_ch.append(channel)

            self.on_ch = sorted(self.on_ch)
            self.off_ch = sorted(self.off_ch)
    
    def moveToFixedBad(self, channel: int) -> None:
        with self.channels_lock:
            if channel in self.on_ch:
                self.on_ch.remove(channel)

            elif channel in self.off_ch:
                self.off_ch.remove(channel)
            
            elif channel in self.ok_ch:
                self.ok_ch.remove(channel)
            
            elif channel in self.bad_ch:
                self.bad_ch.remove(channel)
            
            if channel not in self.fixed_bad:
                self.fixed_bad.append(channel)

            self.on_ch = sorted(self.on_ch)
            self.off_ch = sorted(self.off_ch)
            self.ok_ch = sorted(self.ok_ch)
            self.bad_ch = sorted(self.bad_ch)
    
    def moveToMissingSerial(self, channel: int) -> None:
            with self.channels_lock:
                if channel in self.on_ch:
                    self.on_ch.remove(channel)
    
                elif channel in self.off_ch:
                    self.off_ch.remove(channel)
                
                elif channel in self.ok_ch:
                    self.ok_ch.remove(channel)
                
                elif channel in self.bad_ch:
                    self.bad_ch.remove(channel)
                
                if channel not in self.missing_serial:
                    self.missing_serial.append(channel)
    
                self.on_ch = sorted(self.on_ch)
                self.off_ch = sorted(self.off_ch)
                self.ok_ch = sorted(self.ok_ch)
                self.bad_ch = sorted(self.bad_ch)
    
    def removeFromFixedBad(self, channel:int) -> None:
        with self.channels_lock:
            if channel in self.fixed_bad:
                self.fixed_bad.remove(channel)
        self.moveToBad(channel)
    
    def removeFromMissingSerial(self, channel:int) -> None:
            with self.channels_lock:
                if channel in self.missing_serial:
                    self.missing_serial.remove(channel)
            self.moveToBad(channel)
    
    def sync_power_state(self, channels):
        for ch in channels:
            try:
                status = self.hv.getStatus(slave=ch)
                alarm = self.hv.getAlarm(slave=ch)

                if status in {"ON", "UP"}:
                    self.moveToOn(ch)
                elif status in {"OFF", "DOWN"}:
                    self.moveToOff(ch)
                else:
                    self.moveToOff(ch)

            except Exception as e:
                self.logger.error(f"Problem syncing power state on channel {ch}: {e}")
                self.moveToBad(ch)
    
    def hv_channels_definition(self, channels, n_channels: int = 7):
        if isinstance(channels, int):
            ch_list = [channels]
        elif isinstance(channels, list):
            ch_list = channels
        elif isinstance(channels, str):
            if channels.lower() == "all":
                ch_list = list(range(1, n_channels + 1))
            else:
                ch_list = [int(c) for c in channels.split(",")]
        else:
            raise TypeError(f"Invalid type for channels: {type(channels)}")

        for ch in ch_list:
            if not isinstance(ch, int):
                raise TypeError(
                    f"Invalid HV channel {ch!r}: expected int"
                )
            if ch < 1 or ch > n_channels:
                raise ValueError(f"Invalid HV channel: {ch}")

        return ch_list
   
    def _normalize_channels(self, channels):
        channel_list = self.hv_channels_definition(
            channels=channels,
        )

        ok = []
        bad = []

        for ch in channel_list:
            try:
                if not self.hv.checkAddressBoundary(ch):
                    self.logger.error(f"Channel {ch} out of boundary")
                    bad.append(ch)
                    continue

                if not self.hv.checkAddress(ch):
                    self.logger.error(f"Channel {ch} not responding")
                    bad.append(ch)
                    continue

                ok.append(ch)

            except Exception as e:
                self.logger.error(f"Channel {ch} not responding during startup scan: {e}")
                bad.append(ch)

        return ok, bad
        
    def checkChannel(self, channels):
        ok_channels, bad_channels = self._normalize_channels(channels)
        return ok_channels, bad_channels
    

    def close(self) -> None:
        try:
            self.hv.close()
        except Exception as e:
            self.logger.error(f"Error while closing ModBus client: {e}")
    
    

    def set_common_voltage(self, channels: List[int] | str | int, common_voltage: int):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []

        for ch in channels_good_selected:
            try:
                self.hv.setVoltageSet(
                    value=common_voltage,
                    slave=ch,
                )

                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem setting common voltage on channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
            "common_voltage": common_voltage,
        }
    
    def set_common_threshold(self, channels: List[int] | str | int, common_threshold: int):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []

        for ch in channels_good_selected:
            try:
                self.hv.setThreshold(
                    value=common_threshold,
                    slave=ch,
                )

                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem setting common voltage on channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
            "common_threshold": common_threshold,
        }


    def get_voltage(self, channels: List[int] | str | int):
    
        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []
        volt_conf = {}

        for ch in channels_good_selected:
            try:
                volt = self.hv.getVoltageSet(
                    slave=ch,
                )

                if ch not in volt_conf:
                    volt_conf[ch] = volt

                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem reading voltage setpoint on channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
            "voltage_configuration": volt_conf
        }
    
    def get_threshold(self, channels: List[int] | str | int):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []
        thr_conf = {}

        for ch in channels_good_selected:
            try:
                thr = self.hv.getThreshold(
                    slave=ch,
                )

                if ch not in thr_conf:
                    thr_conf[ch] = thr

                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem reading threshold on channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
            "threshold_configuration": thr_conf
        }


    def get_volt_thr_configuration(self, channels: List[int] | str | int):
    
        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []
        configuration = {}

        for ch in channels_good_selected:
            try:
                
                volt = self.hv.getVoltageSet(slave=ch)
                thr = self.hv.getThreshold(
                    slave=ch,
                )

                if ch not in configuration:
                    configuration[ch] = {"voltage": volt, "threshold": thr}

                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem reading voltage/threshold configuration on channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
            "volt_thr_configuration": configuration
        }


    def get_power_state(
        self,
        channels: List[int] | str | int,
    ):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        successful = []
        failed = []
        power_state = {}

        for ch in list_channels_selected:

            try:
                status = self.hv.getStatus(slave=ch)

                if status in {"UP", "ON"}:
                    state = "on"
                    self.moveToOn(ch)

                elif status in {"DOWN", "OFF"}:
                    state = "off"
                    self.moveToOff(ch)

                elif status == "TRIP":
                    self.logger.error(
                        f"HV channel {ch} is in TRIP state"
                    )
                    failed.append(ch)
                    self.moveToBad(ch)
                    continue

                else:
                    self.logger.error(
                        f"Cannot determine stable power state for "
                        f"HV channel {ch}: status={status}"
                    )
                    failed.append(ch)
                    continue

                power_state[ch] = state
                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem reading power state on HV channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "successful_channels": successful,
            "failed_channels": failed,
            "power_state": power_state,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }
        
    def set_acquisition_configuration(self, channels, acq_configuration: dict):
        list_channels_selected = self.hv_channels_definition(channels=channels)

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []

        for ch in channels_good_selected:
            try:
                external_ch = ch - 1

                ch_config = (
                    acq_configuration.get(external_ch)
                    or acq_configuration.get(str(external_ch))
                )

                if ch_config is None:
                    self.logger.error(
                        f"Missing acquisition configuration for external channel {external_ch} / HV channel {ch}"
                    )
                    failed.append(ch)
                    continue

                voltage = ch_config.get("voltage")
                threshold = ch_config.get("threshold")

                if voltage is None or threshold is None:
                    self.logger.error(
                        f"Incomplete acquisition configuration for external channel {external_ch}: {ch_config}"
                    )
                    failed.append(ch)
                    continue

                self.hv.setVoltageSet(
                    value=int(round(voltage)),
                    slave=ch,
                )

                self.hv.setThreshold(
                    value=int(round(threshold)),
                    slave=ch,
                )

                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem setting acquisition configuration on HV channel {ch}: {e}"
                )
                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }


    def restore_configuration(
        self,
        configuration: dict[int, dict],
    ):

        requested_channels = sorted(
            int(ch)
            for ch in configuration
        )

        successful = []
        failed = []
        channel_results = {}

        #
        # First force channels originally OFF to OFF.
        #
        channels_to_off = [
            ch
            for ch, parameters in configuration.items()
            if parameters.get("power_state") == "off"
        ]

        if channels_to_off:
            off_result = self.off_and_wait(
                channels=channels_to_off,
            )

            failed.extend(
                off_result.get("failed_channels", [])
            )
            failed.extend(off_result.get("skipped_channels", []))

        #
        # Restore voltage and threshold.
        #
        for channel, parameters in configuration.items():

            channel = int(channel)

            channel_ok = True
            channel_result = {}

            try:

                if "voltage" in parameters:
                    self.hv.setVoltageSet(
                        value=int(round(parameters["voltage"])),
                        slave=channel,
                    )

                    channel_result["voltage"] = True

                if "threshold" in parameters:
                    self.hv.setThreshold(
                        value=int(round(parameters["threshold"])),
                        slave=channel,
                    )

                    channel_result["threshold"] = True

            except Exception as e:

                self.logger.error(
                    f"Problem restoring HV configuration "
                    f"on channel {channel}: {e}"
                )

                channel_ok = False
                channel_result["error"] = str(e)

                self.moveToBad(channel)

            channel_results[channel] = channel_result

            if not channel_ok:
                failed.append(channel)

        #
        # Restore channels originally ON.
        #
        channels_to_on = [
            ch
            for ch, parameters in configuration.items()
            if parameters.get("power_state") == "on"
            and ch not in failed
        ]

        if channels_to_on:

            on_result = self.on_and_wait(
                channels=channels_to_on,
            )

            failed.extend(
                on_result.get("failed_channels", [])
            )
            failed.extend(off_result.get("skipped_channels", []))

        failed = sorted(set(failed))

        successful = [
            ch
            for ch in requested_channels
            if ch not in failed
        ]

        return {
            "requested_channels": requested_channels,
            "successful_channels": successful,
            "failed_channels": failed,
            "channel_results": channel_results,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }

    def get_ch_status(self, channels: List[int] | str | int):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())
        bad_ch_set = set(self.getBadChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []

        status = {}

        for ch in channels_good_selected:
            try:

                ch_status = self.hv.getStatus(slave=ch)

                status[ch] = ch_status

                successful.append(ch)

                ok_ch_set.add(ch)
                bad_ch_set.discard(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem reading status from channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)


        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "status": status,
        }


    def get_ch_alarm(self, channels: List[int] | str | int):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []
        alarm = {}

        for ch in channels_good_selected:
            try:
                ch_alarm = self.hv.getAlarm(slave=ch)
                alarm[ch] = ch_alarm
                successful.append(ch)

            except Exception as e:
                self.logger.error(
                    f"Problem reading alarm from channel {ch}: {e}"
                )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "alarm": alarm,
        }
    
    def _build_channel_state_lookup(self, channels=None):

        channel_list = channels if channels is not None else self.hv_channels_definition(channels="all")
 
        fixed_bad_set = set(self.getFixedBad())
        missing_serial_set = set(self.getMissingSerial())
        bad_set = set(self.getBadChannels())
        on_set = set(self.getOnChannels())
        off_set = set(self.getOffChannels())
 
        lookup = {}
 
        for ch in channel_list:
            if ch in fixed_bad_set:
                lookup[ch] = {"channel_state": "fixed_bad", "power_state": None}
            elif ch in missing_serial_set:
                lookup[ch] = {"channel_state": "missing_serial", "power_state": None}
            elif ch in bad_set:
                lookup[ch] = {"channel_state": "bad", "power_state": None}
            elif ch in on_set:
                lookup[ch] = {"channel_state": "ok", "power_state": "on"}
            elif ch in off_set:
                lookup[ch] = {"channel_state": "ok", "power_state": "off"}
            else:
                lookup[ch] = {"channel_state": "unknown", "power_state": None}
 
        return lookup
 
    def get_channel_lists(self):

        return {
            "ok_channels": self.getOkChannels(),
            "bad_channels": self.getBadChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
            "missing_serial_channels": self.getMissingSerial(),
            "fixed_bad_channels": self.getFixedBad(),
        }
 
    def get_ch_electrical(self, channels):
        
        channel_list = self.hv_channels_definition(channels=channels)
        state_lookup = self._build_channel_state_lookup(channel_list)
 
        result = {
            "requested_channels": channel_list,
            "failed_channels": [],
            "channels": {},
        }
 
        for ch in channel_list:
            channel_state = state_lookup.get(ch, {"channel_state": "unknown", "power_state": None})
 
            try:
                voltage = self.hv.getVoltage(slave=ch)
                current = self.hv.getCurrent(slave=ch)
                temperature = self.hv.convertTemperature(self.hv.getTemperature(slave=ch))
 
                result["channels"][ch] = {
                    "voltage": voltage,
                    "current": current,
                    "temperature": temperature,
                    **channel_state,
                }
 
            except Exception as e:
                self.logger.error(f"Problem reading V/I/T from channel {ch}: {e}")
                result["failed_channels"].append(ch)
                self.moveToBad(ch)
 
                result["channels"][ch] = {
                    "voltage": None,
                    "current": None,
                    "temperature": None,
                    **channel_state,
                }
 
        return result
 
    def get_ch_status_and_alarm(self, channels):

        channel_list = self.hv_channels_definition(channels=channels)
        state_lookup = self._build_channel_state_lookup(channel_list)
 
        result = {
            "requested_channels": channel_list,
            "failed_channels": [],
            "channels": {},
        }
 
        for ch in channel_list:
            channel_state = state_lookup.get(ch, {"channel_state": "unknown", "power_state": None})
 
            try:
                hw_status = self.hv.getStatus(slave=ch)
                hw_alarm = self.hv.getAlarm(slave=ch)
 
                result["channels"][ch] = {
                    "hw_status": hw_status,
                    "hw_alarm": hw_alarm,
                    **channel_state,
                }
 
            except Exception as e:
                self.logger.error(f"Problem reading status/alarm from channel {ch}: {e}")
                result["failed_channels"].append(ch)
                self.moveToBad(ch)
 
                result["channels"][ch] = {
                    "hw_status": None,
                    "hw_alarm": None,
                    **channel_state,
                }
 
        return result
 
    def monitor_snapshot(self, channels="all"):

        requested_channels = self.hv_channels_definition(channels=channels)

        fixed_bad_set = set(self.getFixedBad())

        monitored_channels = [ch for ch in requested_channels if ch not in fixed_bad_set]
        skipped_channels = [ch for ch in requested_channels if ch in fixed_bad_set]

        state_lookup = self._build_channel_state_lookup(requested_channels)
        electrical = self.get_ch_electrical(monitored_channels)
        status_alarm = self.get_ch_status_and_alarm(monitored_channels)

        for ch in skipped_channels:
            electrical["channels"][ch] = {
                "voltage": None,
                "current": None,
                "temperature": None,
                **state_lookup[ch],
            }

            status_alarm["channels"][ch] = {
                "hw_status": None,
                "hw_alarm": None,
                **state_lookup[ch],
            }

        electrical["requested_channels"] = requested_channels
        electrical["used_channels"] = monitored_channels
        electrical["skipped_channels"] = skipped_channels

        status_alarm["requested_channels"] = requested_channels
        status_alarm["used_channels"] = monitored_channels
        status_alarm["skipped_channels"] = skipped_channels

        return {
            "type": "data",
            "data_type": "hv_mon",
            "electrical": electrical,
            "status_alarm": status_alarm,
            "channel_lists": self.get_channel_lists(),
        }



    def on(self, channels: List[int] | str | int):
        list_channels_selected = self.hv_channels_definition(channels=channels)

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []

        for ch in channels_good_selected:
            try:
                self.hv.powerOn(slave=ch)

                successful.append(ch)
                self.moveToOn(ch)

            except Exception as e:
                self.logger.error(f"Problem powering on channel {ch}: {e}")

                try:
                    self.hv.reset(slave=ch)
                    self.hv.powerOff(slave=ch)
                except Exception as shutdown_error:
                    self.logger.error(
                        f"Problem forcing channel {ch} off after power-on failure: {shutdown_error}"
                    )

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }

    def off(self, channels: List[int] | str | int):
        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []

        for ch in channels_good_selected:
            try:
                self.hv.powerOff(slave=ch)

                successful.append(ch)
                self.moveToOff(ch)

            except Exception as e:
                self.logger.error(f"Problem powering off channel {ch}: {e}")

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }
    
    def force_off(self, channels: List[int] | str | int):
        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )


        successful = []
        failed = []

        for ch in list_channels_selected:
            try:
                self.hv.powerOff(slave=ch)

                successful.append(ch)
                self.moveToOff(ch)

            except Exception as e:
                self.logger.error(f"Problem powering off channel {ch}: {e}")

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }

    def reset(self, channels: List[int] | str | int):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        successful = []
        failed = []

        for ch in channels_good_selected:
            try:
                self.hv.reset(slave=ch)

                successful.append(ch)

            except Exception as e:
                self.logger.error(f"Problem resetting channel {ch}: {e}")

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }
    
    def force_reset(self, channels: List[int] | str | int):

        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )


        successful = []
        failed = []

        for ch in list_channels_selected:
            try:
                self.hv.reset(slave=ch)

                successful.append(ch)

            except Exception as e:
                self.logger.error(f"Problem resetting channel {ch}: {e}")

                failed.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "successful_channels": successful,
            "failed_channels": failed,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }

    def recover_bad_channels(self):
        bad_channels = self.getBadChannels()

        recovered = []
        still_bad = []
        recovered_on = []
        recovered_off = []

        for ch in bad_channels:
            try:
                if not self.hv.checkAddressBoundary(ch):
                    still_bad.append(ch)
                    continue

                if not self.hv.checkAddress(ch):
                    still_bad.append(ch)
                    continue

                status = self.hv.getStatus(slave=ch)

                self.moveToOk(ch)

                if status == "UP":
                    self.moveToOn(ch)
                    recovered_on.append(ch)
                elif status == "DOWN":
                    self.moveToOff(ch)
                    recovered_off.append(ch)
                else:
                    self.moveToOff(ch)
                    recovered_off.append(ch)

                recovered.append(ch)

            except Exception as e:
                self.logger.error(f"Problem recovering bad channel {ch}: {e}")
                still_bad.append(ch)

        return {
            "checked_channels": bad_channels,
            "recovered_channels": recovered,
            "recovered_on_channels": recovered_on,
            "recovered_off_channels": recovered_off,
            "still_bad_channels": still_bad,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }
        
    def recover_ok_off_acquisition(self, acq_info: dict[int, dict] | None):

        ok_off_channels = sorted(self.getOkOffChannels())

        if not ok_off_channels:
            return {
                "checked_channels": [],
                "recovered_channels": [],
                "still_bad_channels": [],
                "bad_channels": self.getBadChannels(),
                "ok_channels": self.getOkChannels(),
                "on_channels": self.getOnChannels(),
                "off_channels": self.getOffChannels(),
            }
            
        if not acq_info:
            self.logger.warning("Acquisition recovery requested but no stored HV parameters available")
            return {
                "checked_channels": ok_off_channels,
                "recovered_channels": [],
                "still_bad_channels": ok_off_channels,
                "bad_channels": self.getBadChannels(),
                "ok_channels": self.getOkChannels(),
                "on_channels": self.getOnChannels(),
                "off_channels": self.getOffChannels(),
            }

        safe_channels = []
        failed_recovery = []

        for channel in ok_off_channels:
            try:
                if not self.hv.checkAddressBoundary(channel):
                    failed_recovery.append(channel)
                    continue

                if not self.hv.checkAddress(channel):
                    failed_recovery.append(channel)
                    continue

                status = self.hv.getStatus(slave=channel)
                if status in {"TRIP"}:
                    failed_recovery.append(channel)
                    continue

                alarm = self.hv.getAlarm(slave=channel)
                if alarm != "none":
                    failed_recovery.append(channel)
                    continue

                safe_channels.append(channel)

            except Exception as e:
                self.logger.error(f"Problem checking channel {channel} before acquisition recovery: {e}")
                failed_recovery.append(channel)

        configured_channels = []
        
        for channel in safe_channels:
            params = acq_info.get(channel)
            
            if not params or params.get("voltage") is None or params.get("threshold") is None:
                self.logger.warning(
                    f"No stored voltage/threshold for channel {channel}, skipping recovery for this channel"
                )
                failed_recovery.append(channel)
                continue
                
            try:
                self.hv.setVoltageSet(value=int(round(params["voltage"])), slave=channel)
                self.hv.setThreshold(value=int(round(params["threshold"])), slave=channel)
                configured_channels.append(channel)
            except Exception as e:
                self.logger.error(f"Problem setting voltage/threshold on channel {channel} before recovery: {e}")
                failed_recovery.append(channel)
                self.moveToBad(channel)
        
        
        acquisition_recovered = []

        if safe_channels:
            result_power = self.on_and_wait(channels=safe_channels)

            successful = set(result_power.get("successful_channels", []))
            acquisition_recovered = [ch for ch in safe_channels if ch in successful]
            failed_recovery.extend(ch for ch in safe_channels if ch not in successful)

        return {
            "checked_channels": ok_off_channels,
            "recovered_channels": acquisition_recovered,
            "still_bad_channels": failed_recovery,
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }
    

    def on_and_wait(self, channels: List[int] | str | int, timeout_s: float = 240.0, poll_s: float = 2.0):
        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        power_on_successful = []
        failed_channels = []
        up_channels = []

        for ch in channels_good_selected:
            try:
                self.hv.powerOn(slave=ch)
                power_on_successful.append(ch)

            except Exception as e:
                self.logger.error(f"Problem powering on channel {ch}: {e}")

                try:
                    self.hv.reset(slave=ch)
                    self.hv.powerOff(slave=ch)
                except Exception as shutdown_error:
                    self.logger.error(
                        f"Problem forcing channel {ch} off after power-on failure: "
                        f"{shutdown_error}"
                    )

                failed_channels.append(ch)
                self.moveToBad(ch)

        pending_channels = [
            ch for ch in power_on_successful
            if ch not in failed_channels
        ]

        deadline = time.time() + timeout_s

        while pending_channels and time.time() < deadline:
            for ch in list(pending_channels):
                try:
                    status = self.hv.getStatus(slave=ch)
                    alarm = self.hv.getAlarm(slave=ch)

                    if status == "UP":
                        self.moveToOn(ch)
                        up_channels.append(ch)
                        pending_channels.remove(ch)

                    elif status == "TRIP" or alarm != "none":
                        self.logger.error(
                            f"Channel {ch} unsafe while waiting for UP: "
                            f"status={status}, alarm={alarm}"
                        )

                        try:
                            self.hv.reset(slave=ch)
                            self.hv.powerOff(slave=ch)
                        except Exception as shutdown_error:
                            self.logger.error(
                                f"Problem resetting/off channel {ch} after unsafe state: "
                                f"{shutdown_error}"
                            )

                        failed_channels.append(ch)
                        self.moveToBad(ch)
                        pending_channels.remove(ch)

                except Exception as e:
                    self.logger.error(
                        f"Problem checking UP state for channel {ch}: {e}"
                    )

                    try:
                        self.hv.reset(slave=ch)
                        self.hv.powerOff(slave=ch)
                    except Exception as shutdown_error:
                        self.logger.error(
                            f"Problem resetting/off channel {ch} after read failure: "
                            f"{shutdown_error}"
                        )

                    failed_channels.append(ch)
                    self.moveToBad(ch)
                    pending_channels.remove(ch)

            if pending_channels:
                time.sleep(poll_s)

        if pending_channels:
            for ch in pending_channels:
                self.logger.error(
                    f"Timeout waiting for channel {ch} to reach UP state"
                )

                try:
                    self.hv.reset(slave=ch)
                    self.hv.powerOff(slave=ch)
                except Exception as shutdown_error:
                    self.logger.error(
                        f"Problem resetting/off channel {ch} after timeout: "
                        f"{shutdown_error}"
                    )

                failed_channels.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": sorted(up_channels),
            "up_channels": sorted(up_channels),
            "failed_channels": sorted(set(failed_channels)),
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }
        
        
    
    def off_and_wait(
        self,
        channels: List[int] | str | int,
        timeout_s: float = 120.0,
        poll_s: float = 2.0,
    ):
        list_channels_selected = self.hv_channels_definition(
            channels=channels,
        )

        ok_ch_set = set(self.getOkChannels())

        channels_good_selected = [
            ch for ch in list_channels_selected if ch in ok_ch_set
        ]

        channels_skipped = [
            ch for ch in list_channels_selected if ch not in ok_ch_set
        ]

        power_off_successful = []
        failed_channels = []
        down_channels = []

        for ch in channels_good_selected:
            try:
                self.hv.powerOff(slave=ch)
                power_off_successful.append(ch)

            except Exception as e:
                self.logger.error(f"Problem powering off channel {ch}: {e}")
                failed_channels.append(ch)
                self.moveToBad(ch)

        pending_channels = [
            ch for ch in power_off_successful
            if ch not in failed_channels
        ]

        deadline = time.time() + timeout_s

        while pending_channels and time.time() < deadline:
            for ch in list(pending_channels):
                try:
                    status = self.hv.getStatus(slave=ch)

                    if status in {"DOWN", "OFF"}:
                        self.moveToOff(ch)
                        down_channels.append(ch)
                        pending_channels.remove(ch)

                    elif status == "TRIP":
                        self.logger.error(
                            f"Channel {ch} went TRIP while waiting for DOWN"
                        )

                        try:
                            self.hv.reset(slave=ch)
                            self.hv.powerOff(slave=ch)
                        except Exception as shutdown_error:
                            self.logger.error(
                                f"Problem resetting/off channel {ch} after TRIP: "
                                f"{shutdown_error}"
                            )

                        failed_channels.append(ch)
                        self.moveToBad(ch)
                        pending_channels.remove(ch)

                except Exception as e:
                    self.logger.error(
                        f"Problem checking DOWN state for channel {ch}: {e}"
                    )

                    failed_channels.append(ch)
                    self.moveToBad(ch)
                    pending_channels.remove(ch)

            if pending_channels:
                time.sleep(poll_s)

        if pending_channels:
            for ch in pending_channels:
                self.logger.error(
                    f"Timeout waiting for channel {ch} to reach DOWN state"
                )

                failed_channels.append(ch)
                self.moveToBad(ch)

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,
            "successful_channels": sorted(down_channels),
            "down_channels": sorted(down_channels),
            "failed_channels": sorted(set(failed_channels)),
            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }
        
        
    def change_feb_address(
        self,
        new_address: int,
        standard_addr: int | None = None,
    ) -> dict:
        try:
            self.hv.reset_connection()
            time.sleep(2.0)

            old_address = self.hv.find_feb_address(
                preferred_address=standard_addr or new_address,
            )

            if old_address is None:
                return {
                    "success": False,
                    "old_address": None,
                    "new_address": new_address,
                    "error": "No FEB found in Modbus scan",
                }

            if old_address == new_address:
                self.moveToOk(new_address)
                return {
                    "success": True,
                    "old_address": old_address,
                    "new_address": new_address,
                    "message": "FEB already at requested address",
                }

            try:
                self.hv.setModbusAddress(
                    ch_addr=new_address,
                    slave=old_address,
                )
            except Exception as e:
                self.logger.warning(
                    f"Expected communication interruption during address change: {e}"
                )

            self.hv.reset_connection()
            time.sleep(2.0)

            if self.hv.checkAddress(new_address):
                self.moveToOk(new_address)
                return {
                    "success": True,
                    "old_address": old_address,
                    "new_address": new_address,
                    "message": "FEB address changed successfully",
                }

            return {
                "success": False,
                "old_address": old_address,
                "new_address": new_address,
                "error": f"Failed to verify new address {new_address}",
            }

        except Exception as e:
            return {
                "success": False,
                "old_address": None,
                "new_address": new_address,
                "error": str(e),
            }
    
    def set_pmt_serial(self, channel: int, serial: str) -> None:
        if channel < 1 or channel > self.hv.num_channels:
            raise ValueError(f"Invalid HV channel: {channel}")
        self.hv.setPMTSerialNumber(serial, slave=channel)
    
    def check_missing_serial(self, channels: list[int]) -> dict:
        missing = []
        
        for ch in channels:
            try:
                    _, pmt_serial, _, _, _ = self.hv.getInfo(slave=ch)
            except Exception as e:
                self.logger.error(f"Failed to read PMT info for channel {ch}: {e}")
                missing.append(ch)
                self.moveToMissingSerial(ch)
                continue
            
            if not pmt_serial.strip("\x00").strip():
                missing.append(ch)
                self.moveToMissingSerial(ch)
        
        return {"missing_serial_channels": missing, "checked_channels": list(channels)}
    
    def get_serial_map(self, channels: list[int]) -> dict:
        
        serial_ch_map = {}
        
        for ch in channels:
            if ch not in serial_ch_map:
                try:
                    _, pmt_serial, _, _, _ = self.hv.getInfo(slave=ch)
                    serial_ch_map[ch] = pmt_serial
                except Exception as e:
                    self.logger.error(f"Failed to read PMT info for channel {ch}: {e}")
                    serial_ch_map[ch] = "0"
                    continue
                
                if not pmt_serial.strip("\x00").strip():
                    serial_ch_map[ch] = "0"
        
        return serial_ch_map  



    def _apply_calib_conf(self, ch:int, calib_conf: dict[int, tuple[int, int]]) -> bool:

        if ch not in calib_conf:
            self.logger.error("It was not possible to apply calibration configuration")
            return False

        m, q = calib_conf.get(ch)
        try:
            self.hv.writeCalibSlope(slope=m, slave=ch)
            self.hv.writeCalibOffset(offset=q, slave=ch)
            return True
        except Exception:
            self.logger.exception(f"It was not possible to apply calibration configuration")
            return False

    def calibrate(self, channels: list[int]) -> dict:

        try:
            list_channels_selected = self.hv_channels_definition(channels=channels)
        except Exception:
            self.logger.exception("Invalid HV calibration channel selection")
            return {
                "requested_channels": [], "used_channels": [], "skipped_channels": [],
                "successful_channels": [], "failed_channels": [],
                "restored_channels": [], "failed_restore_channels": [],
                "channel_results": {}, "success": False,
                "bad_channels": self.getBadChannels(), "ok_channels": self.getOkChannels(),
                "on_channels": self.getOnChannels(), "off_channels": self.getOffChannels(),
            }
        
        ok_ch_set = set(self.getOkChannels())
        channels_good_selected = [ch for ch in list_channels_selected if ch in ok_ch_set]
        channels_skipped = [ch for ch in list_channels_selected if ch not in ok_ch_set]

        successful = []
        failed = []
        restored = []
        failed_restore = []
        channel_results = {ch: {"calibration_success": False, "restore_success": None,
                                "error": None, "slope": None, "offset": None}
                           for ch in list_channels_selected}
        old_configuration = {}
        channels_to_be_done = []
        modified_channels = []
        new_calibration = {}
        Vexpect = [25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400]

        self.logger.warning("HV voltage calibration started; operating configuration will be restored")

        for ch in channels_good_selected:
            try:
                m, q, _ = self.hv.readCalibRegisters(slave=ch)
                rate_up, rate_down = self.hv.getRate(fmt=tuple, slave=ch)
                voltage = self.hv.getVoltageSet(slave=ch)
                threshold = self.hv.getThreshold(slave=ch)
                status = self.hv.getStatus(slave=ch)
                if status not in {"UP", "DOWN"}:
                    self.logger.error(f"Channel {ch}: cannot snapshot non-stable power status {status}")
                    failed.append(ch)
                    channel_results[ch]["error"] = f"unstable initial status: {status}"
                    continue
                values = (m, q, rate_up, rate_down, voltage, threshold)
                if not all(np.isfinite(value) for value in values):
                    self.logger.error(f"Channel {ch}: invalid initial configuration")
                    failed.append(ch)
                    channel_results[ch]["error"] = "invalid initial configuration"
                    continue
                old_configuration[ch] = {
                    "calibration": (m, q), "rates": (rate_up, rate_down),
                    "voltage": voltage, "threshold": threshold, "status": status,
                }
                channels_to_be_done.append(ch)
            except Exception:
                self.logger.exception(f"Failed to snapshot channel {ch}; leaving it untouched")
                failed.append(ch)
                channel_results[ch]["error"] = "hardware snapshot failed"

        try:
            for ch in list(channels_to_be_done):
                modified_channels.append(ch)
                try:
                    self.hv.powerOff(slave=ch)
                    self.hv.setVoltageSet(value=10, slave=ch)

                except Exception:
                    self.logger.exception(f"Failed to prepare initial shutdown on channel {ch}")
                    failed.append(ch)
                    channel_results[ch]["error"] = "initial shutdown failed"
                    channels_to_be_done.remove(ch)
            

            pending = set(channels_to_be_done)
            ready_channels = []
            deadline = time.monotonic() + 300

            while pending and time.monotonic() < deadline:
                for ch in list(pending):
                    try:
                        status = self.hv.getStatus(slave=ch)
                        voltage = self.hv.getVoltage(slave=ch)

                        if status == "TRIP":
                            self.logger.error("HV TRIP during initial ramp-down")
                            channel_results[ch]["error"] = "TRIP during initial ramp-down"
                            failed.append(ch)
                            pending.remove(ch)
                            self.hv.reset(slave=ch)
                            self.moveToBad(channel=ch)
                            continue

                        if status == "DOWN" and voltage < Vexpect[0]:
                            pending.remove(ch)
                            ready_channels.append(ch)

                    except Exception:
                        self.logger.exception(f"Failed while waiting for channel {ch} to ramp down")
                        pending.remove(ch)
                        failed.append(ch)
                        channel_results[ch]["error"] = "initial ramp-down read failed"
                        self.moveToBad(ch)

                if pending:
                    time.sleep(1)

            for ch in pending:
                self.logger.error(f"Timeout waiting for channel {ch} to fall below {Vexpect[0]} V")
                failed.append(ch)
                channel_results[ch]["error"] = "initial ramp-down timeout"


            start_channels = []
            for ch in ready_channels:
                try:
                    if not self._apply_calib_conf(ch, {ch: (1.0, 0.0)}):
                        self.logger.error(f"Channel {ch}: cannot install temporary coefficients")
                        failed.append(ch)
                        channel_results[ch]["error"] = "temporary coefficient write failed"
                        continue
                    self.hv.setRateRampup(value=25, slave=ch)
                    self.hv.setRateRampdown(value=25, slave=ch)
                    self.hv.setVoltageSet(value=Vexpect[0], slave=ch)
                    self.hv.powerOn(slave=ch)
                    start_channels.append(ch)

                except Exception:
                    self.logger.exception(f"Failed to configure/power on channel {ch}")
                    failed.append(ch)
                    channel_results[ch]["error"] = "initial power-on failed"
            

            pending = set(start_channels)
            up_channels = []
            deadline = time.monotonic() + 300

            while pending and time.monotonic() < deadline:
                for ch in list(pending):
                    try:
                        status = self.hv.getStatus(slave=ch)
                        if status == "UP":
                            pending.remove(ch)
                            up_channels.append(ch)
                        elif status == "TRIP":
                            self.logger.error("HV TRIP during initial ramp-up")
                            channel_results[ch]["error"] = "TRIP during initial ramp-up"
                            failed.append(ch)
                            pending.remove(ch)
                            self.hv.reset(slave=ch)
                            self.moveToBad(channel=ch)

                    except Exception:
                        self.logger.exception(f"Failed while waiting for channel {ch} to reach UP")
                        pending.remove(ch)
                        failed.append(ch)
                        channel_results[ch]["error"] = "initial ramp-up read failed"
                        self.moveToBad(ch)

                if pending:
                    time.sleep(1)

            for ch in pending:
                self.logger.error(f"Timeout waiting for channel {ch} to reach UP")
                failed.append(ch)
                channel_results[ch]["error"] = "initial ramp-up timeout"

            Vread = {ch: [] for ch in up_channels}
            active_channels = list(up_channels)
            
            for v in Vexpect:
                pending = set()
                for ch in list(active_channels):
                    try:
                        self.hv.setVoltageSet(value=v, slave=ch)
                        pending.add(ch)
                    except Exception:
                        self.logger.exception(f"Failed to set {v} V on channel {ch}")
                        failed.append(ch)
                        active_channels.remove(ch)
                        channel_results[ch]["error"] = f"setpoint failed at {v} V"

                ready_channels = []
                deadline = time.monotonic() + 300
                while pending and time.monotonic() < deadline:
                    for ch in list(pending):
                        try:
                            status = self.hv.getStatus(slave=ch)
                            if status == "UP":
                                pending.remove(ch)
                                ready_channels.append(ch)
                            elif status == "TRIP":
                                self.logger.error(f"Channel {ch}: TRIP at Vset={v} V")
                                failed.append(ch)
                                channel_results[ch]["error"] = f"TRIP at {v} V"
                                pending.remove(ch)
                                active_channels.remove(ch)
                                self.hv.reset(slave=ch)
                                self.moveToBad(ch)
                        except Exception:
                            self.logger.exception(f"Failed while waiting for channel {ch} to reach UP")
                            pending.remove(ch)
                            channel_results[ch]["error"] = f"status read failed at {v} V"
                            failed.append(ch)
                            active_channels.remove(ch)
                            self.moveToBad(ch)

                    if pending:
                        time.sleep(1)
    
                for ch in pending:
                    self.logger.error(f"Timeout waiting for channel {ch} at {v} V")
                    failed.append(ch)
                    channel_results[ch]["error"] = f"ramp timeout at {v} V"
                    active_channels.remove(ch)

                if ready_channels:
                    time.sleep(2)

                for ch in ready_channels:
                    try:
                        Vtemp = []
                        for _ in range(10):
                            Vtemp.append(self.hv.getVoltage(slave=ch))
                            time.sleep(0.5)
                        Vmeas = np.array(Vtemp, dtype=float)
                        if not np.all(np.isfinite(Vmeas)):
                            self.logger.error(f"Channel {ch}: non-finite voltage at {v} V")
                            failed.append(ch)
                            channel_results[ch]["error"] = f"non-finite voltage at {v} V"
                            active_channels.remove(ch)
                            continue
                        Vmeas.sort()
                        Vread[ch].append(float(Vmeas[1:-1].mean()))
                    except Exception:
                        self.logger.exception(f"Failed to sample channel {ch} at {v} V")
                        failed.append(ch)
                        channel_results[ch]["error"] = f"sampling failed at {v} V"
                        active_channels.remove(ch)


            for ch in active_channels:
                try:
                    if len(Vread[ch]) != len(Vexpect):
                        self.logger.error(f"Channel {ch}: incomplete voltage scan")
                        failed.append(ch)
                        channel_results[ch]["error"] = "incomplete voltage scan"
                        continue
                    x = np.array(Vread[ch], dtype=float)
                    y = np.array(Vexpect, dtype=float)
                    A = np.vstack([x, np.ones(len(x))]).T
                    if np.linalg.matrix_rank(A) < 2:
                        self.logger.error(f"Channel {ch}: measured voltages have zero spread")
                        failed.append(ch)
                        channel_results[ch]["error"] = "degenerate linear fit"
                        continue
                    slope, offset = np.linalg.lstsq(A, y, rcond=None)[0]
                    if not np.isfinite(slope) or not np.isfinite(offset) or slope <= 0:
                        self.logger.error(f"Channel {ch}: invalid fit slope={slope}, offset={offset}")
                        failed.append(ch)
                        channel_results[ch]["error"] = "invalid linear fit"
                        continue
                    new_calibration[ch] = (float(slope), float(offset))
                except Exception:
                    self.logger.exception(f"Failed to fit channel {ch}")
                    failed.append(ch)
                    channel_results[ch]["error"] = "linear fit failed"

        except Exception:
            self.logger.exception("Unexpected error during HV calibration")
            for ch in modified_channels:
                if ch not in failed:
                    failed.append(ch)
                    channel_results[ch]["error"] = "unexpected calibration error"

        finally:
            bad_channels = set(self.getBadChannels())

            for ch in modified_channels:
                previous = old_configuration.get(ch)
                if previous is None:
                    self.logger.error(f"Channel {ch}: previous configuration unavailable")

                    if ch not in failed:
                        failed.append(ch)

                    failed_restore.append(ch)

                    try:
                        self.hv.powerOff(slave=ch)
                        self.hv.reset(slave=ch)
                    except Exception:
                        self.logger.exception(f"Channel {ch}: failed to force safe state")

                    self.moveToBad(channel=ch)
                    continue
                

                if ch in bad_channels:
                    self.logger.warning(f"Channel {ch}: restore skipped because channel is BAD")

                    if ch not in failed:
                        failed.append(ch)

                    channel_results[ch]["restore_success"] = False

                    if ch not in failed_restore:
                        failed_restore.append(ch)

                    try:
                        self.hv.powerOff(slave=ch)
                        self.hv.reset(slave=ch)
                    except Exception:
                        self.logger.exception(f"Channel {ch}: failed to force safe state")

                    continue
                

                restore_ok = True

                if ch in new_calibration and ch not in failed:
                    selected_calibration = new_calibration[ch]
                    calibration_success = True
                else:
                    selected_calibration = previous["calibration"]
                    calibration_success = False

                if not self._apply_calib_conf(ch, {ch: selected_calibration}):
                    self.logger.error(f"Channel {ch}: failed to restore calibration coefficients")
                    restore_ok = False


                for name, method, value in (
                    (
                        "ramp-up",
                        self.hv.setRateRampup,
                        previous["rates"][0],
                    ),
                    (
                        "ramp-down",
                        self.hv.setRateRampdown,
                        previous["rates"][1],
                    ),
                    (
                        "voltage",
                        self.hv.setVoltageSet,
                        previous["voltage"],
                    ),
                    (
                        "threshold",
                        self.hv.setThreshold,
                        previous["threshold"],
                    ),
                ):

                    try:
                        method(value=value, slave=ch)
                    except Exception:
                        self.logger.exception(f"Channel {ch}: failed to restore {name}")
                        restore_ok = False

                if restore_ok:
                    try:
                        if previous["status"] == "UP":
                            result = self.on_and_wait(channels=[ch])
                            if ch not in result["successful_channels"]:
                                restore_ok = False
                        else:
                            result = self.off_and_wait(channels=[ch])
                            if ch not in result["successful_channels"]:
                                restore_ok = False
                    except Exception:
                        self.logger.exception(f"Channel {ch}: failed to restore power state")
                        restore_ok = False
                
                if not restore_ok:
                    if ch not in failed:
                        failed.append(ch)
                    if ch not in failed_restore:
                        failed_restore.append(ch)

                    channel_results[ch]["restore_success"] = False
                    channel_results[ch]["calibration_success"] = False
                    channel_results[ch]["error"] = "previous operating configuration restore failed"

                    try:
                        self.hv.powerOff(slave=ch)
                    except Exception:
                        self.logger.exception(f"Channel {ch}: failed to power off after restore failure")
                    
                    try:
                        self.hv.reset(slave=ch)
                    except Exception:
                        self.logger.exception(f"Channel {ch}: failed to reset after restore failure")
                    
                    self.moveToBad(ch)

                    continue

                channel_results[ch]["restore_success"] = True

                if ch not in restored:
                    restored.append(ch)

                if calibration_success:
                    if ch not in successful:
                        successful.append(ch)

                    channel_results[ch]["calibration_success"] = True
                    channel_results[ch]["slope"] = (
                        new_calibration[ch][0]
                    )
                    channel_results[ch]["offset"] = (
                        new_calibration[ch][1]
                    )

                else:
                    channel_results[ch]["calibration_success"] = False


        failed = sorted(set(failed))
        successful = sorted(set(successful))
        restored = sorted(set(restored))
        failed_restore = sorted(set(failed_restore))

        return {
            "requested_channels": list_channels_selected,
            "used_channels": channels_good_selected,
            "skipped_channels": channels_skipped,

            "successful_channels": successful,
            "failed_channels": failed,

            "restored_channels": restored,
            "failed_restore_channels": failed_restore,

            "channel_results": channel_results,

            "success": (
                bool(channels_good_selected)
                and not failed
                and not failed_restore
            ),

            "bad_channels": self.getBadChannels(),
            "ok_channels": self.getOkChannels(),
            "on_channels": self.getOnChannels(),
            "off_channels": self.getOffChannels(),
        }


        



                    







    
                
        
                
            
        
            
            
        

