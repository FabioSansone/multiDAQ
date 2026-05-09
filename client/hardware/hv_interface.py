from client.utils.channels import channels_definition
from client.hardware.hvmodbus import HVModBus
from client.utils.logger import get_logger
import time
from typing import List


class HV:
    
    def __init__(self, hv_port):
        self.logger = get_logger('hv')
        self.hv = HVModBus(hv_port)
        
        self.ok_ch, self.bad_ch = self.checkChannel(channels="all")
   
    def _normalize_channels(self, channels):
        channel_list = channels_definition(
            channels=channels,
            hv_channels=True
        )

        ok = []
        bad = []

        for ch in channel_list:
            
            if not self.hv.open(ch):
                self.logger.error(f"Not possible to open channel {ch}")
                bad.append(ch)
                continue

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

        list_channels_selected = channels_definition(channels=channels, hv_channels=True)
        
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
        
        for ch in channels_good_selected:
            try:
                self.hv.open(channel=ch)
                self.hv.setVoltageSet(value=common_voltage)
                
                successful.append(ch)
                ok_ch_set.add(ch)
                bad_ch_set.discard(ch)

            except Exception as e:
                self.logger.error(f"Problem setting common voltage on channel {ch}: {e}")
                
                failed.append(ch)
                ok_ch_set.discard(ch)
                bad_ch_set.add(ch)
        
        self.ok_ch = sorted(ok_ch_set)
        self.bad_ch = sorted(bad_ch_set)
        
        return {
        "requested_channels": list_channels_selected,
        "used_channels": channels_good_selected,
        "skipped_channels": channels_skipped,
        "successful_channels": successful,
        "failed_channels": failed,
        "common_voltage": common_voltage,
    }







    def waitUntilStatus(self, channels, end_status='UP'):
        """
        Waits until the channels reach the specified end_status (e.g., 'UP' or 'DOWN').

        Returns a tuple:
            (channels_ok, channels_failed)
        """

        ok, bad = self._normalize_channels(channels)
        if not ok:
            self.logger.error("No valid channels to monitor.")
            return [], bad

        pending = ok.copy()
        reached_status = []

        self.logger.warning(f"Monitoring {len(pending)} channels until status '{end_status}'")

        while pending:
            channels_to_remove = []

            for ch in pending:
                try:
                    self.hv.open(ch)
                    alarm = self.hv.alarmString(self.hv.getAlarm())

                    if alarm != "none":
                        self.logger.warning(f"Alarm on channel {ch}: {alarm}")
                        channels_to_remove.append(ch)
                        continue

                    status = self.hv.statusString(self.hv.getStatus())
                    if status == end_status:
                        self.logger.info(f"Channel {ch} reached status '{end_status}'")
                        channels_to_remove.append(ch)

                except Exception as e:
                    self.logger.error(f"Error monitoring channel {ch}: {e}")
                    channels_to_remove.append(ch)

            for ch in channels_to_remove:
                pending.remove(ch)
                reached_status.append(ch)

            time.sleep(1)

        failed = [ch for ch in ok if ch not in reached_status]
        return reached_status, bad + failed
                
        
                
            
        
            
            
        

