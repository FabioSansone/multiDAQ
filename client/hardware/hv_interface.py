from experimental.client.utils.channels import channels_definition
from hvmodbus import HVModBus
from utils.logger import get_logger
import time


class HV:
    
    def __init__(self, hv_port):
        self.logger = get_logger('hv')
        self.hv = HVModBus(hv_port)
        
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
                
        
                
            
        
            
            
        

