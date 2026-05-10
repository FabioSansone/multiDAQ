from client.utils.channels import channels_definition
from client.hardware.hv.hvmodbus import HVModBus
from client.utils.logger import get_logger

from typing import List



class HV:
    
    def __init__(self, hv_port):
        self.logger = get_logger('hv')
        self.hv = HVModBus(hv_port)
        
        self.ok_ch, self.bad_ch = self.checkChannel(channels="all")
        self.on_ch = []
        self.off_ch = list(self.ok_ch)

    def getOkChannels(self):
        return list(self.ok_ch)

    def getBadChannels(self):
        return list(self.bad_ch)

    def getOnChannels(self):
        return list(self.on_ch)

    def getOffChannels(self):
        return list(self.off_ch)

    def moveToOk(self, channel: int) -> None:
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
        if channel in self.ok_ch:
            self.ok_ch.remove(channel)

        if channel not in self.bad_ch:
            self.bad_ch.append(channel)

        if channel in self.on_ch:
            self.on_ch.remove(channel)

        if channel in self.off_ch:
            self.off_ch.remove(channel)

        self.ok_ch = sorted(self.ok_ch)
        self.bad_ch = sorted(self.bad_ch)
        self.on_ch = sorted(self.on_ch)
        self.off_ch = sorted(self.off_ch)

    def moveToOn(self, channel: int) -> None:
        if channel not in self.ok_ch:
            return

        if channel in self.off_ch:
            self.off_ch.remove(channel)

        if channel not in self.on_ch:
            self.on_ch.append(channel)

        self.on_ch = sorted(self.on_ch)
        self.off_ch = sorted(self.off_ch)

    def moveToOff(self, channel: int) -> None:
        if channel in self.on_ch:
            self.on_ch.remove(channel)

        if channel not in self.off_ch:
            self.off_ch.append(channel)

        self.on_ch = sorted(self.on_ch)
        self.off_ch = sorted(self.off_ch)
   
    def _normalize_channels(self, channels):
        channel_list = channels_definition(
            channels=channels,
            hv_channels=True
        )

        ok = []
        bad = []

        for ch in channel_list:

            if not self.hv.checkAddressBoundary(ch):
                self.logger.error(f"Channel {ch} out of boundary")
                bad.append(ch)
                continue

            if not self.hv.checkAddress(ch):
                self.logger.error(f"Channel {ch} not responding")
                bad.append(ch)
                continue

            ok.append(ch)

        return ok, bad
        
    def checkChannel(self, channels):
        ok_channels, bad_channels = self._normalize_channels(channels)
        return ok_channels, bad_channels
    

    def set_common_voltage(self, channels: List[int] | str | int, common_voltage: int):

        list_channels_selected = channels_definition(
            channels=channels,
            hv_channels=True,
        )

        ok_ch_set = set(self.ok_ch)

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

    def get_ch_status(self, channels: List[int] | str | int):

        list_channels_selected = channels_definition(
            channels=channels,
            hv_channels=True
        )

        ok_ch_set = set(self.ok_ch)
        bad_ch_set = set(self.bad_ch)

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

        list_channels_selected = channels_definition(
            channels=channels,
            hv_channels=True
        )

        ok_ch_set = set(self.ok_ch)

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
        list_channels_selected = channels_definition(channels=channels, hv_channels=True)

        ok_ch_set = set(self.ok_ch)

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
        list_channels_selected = channels_definition(
            channels=channels,
            hv_channels=True
        )

        ok_ch_set = set(self.ok_ch)

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

    def reset(self, channels: List[int] | str | int):

        list_channels_selected = channels_definition(
            channels=channels,
            hv_channels=True
        )

        ok_ch_set = set(self.ok_ch)

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







    
                
        
                
            
        
            
            
        

