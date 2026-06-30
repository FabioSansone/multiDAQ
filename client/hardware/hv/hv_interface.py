from client.utils.channels import channels_definition
from client.hardware.hv.hvmodbus import HVModBus
from client.utils.logger import get_logger
import threading
from typing import List
import time



class HV:
    
    def __init__(self, hv_port):
        self.logger = get_logger('hv')
        self.hv = HVModBus(hv_port)
        
        self.channels_lock = threading.Lock()
        self.ok_ch, self.bad_ch = self.checkChannel(channels="all")
        self.on_ch = []
        self.off_ch = []
        
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

                    elif status == "TRIP" or alarm in {"OV", "UV", "OC", "UC"}:
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







    
                
        
                
            
        
            
            
        

