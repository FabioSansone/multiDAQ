from client.utils.logger import get_logger
from client.utils.channels import channels_definition
import mmap
from typing import Optional
import datetime



class RC:

    def __init__(self):
        self.logger = get_logger('run_control')
        self.max_regs = 50
        self.num_channels = 7

        try:
            self.fid = open('/dev/uio0', 'r+b', 0)
        except:
            self.logger.error("E: UIO device /dev/uio0 not found")
            
        try:
            self.regs = mmap.mmap(self.fid.fileno(), 0x10000)
        except Exception as e:
            self.logger.error(f"E: Failed to map memory: {e}")
            self.fid.close()
        
    def auto_int(self, x) -> int:
        if isinstance(x, int): 
            return x
        return int(x, 0)
    
    def checkRegBoundary(self, addr) -> bool:
      if (addr < 0 or addr > self.max_regs):
        self.logger.warning(f"Register {addr} is out of boundary: [0, {self.max_regs}]")
        return False
      return True
    
    def checkChannelsBoundary(self, channel) -> bool:
        if (channel < 0 or channel >= self.num_channels):
            self.logger.warning(f"Channel {channel} is out of boundary: [0, {self.num_channels}]")
            return False
        return True

    def read(self, addr) -> Optional[int]:
        if (self.checkRegBoundary(self.auto_int(addr))):
            try:
                value = int.from_bytes(self.regs[self.auto_int(addr)*4:(self.auto_int(addr)*4)+4], byteorder='little')
                return value
            except Exception as e:
                self.logger.error(f"Problem occured in reading the register {addr}: {e}")
                return None
        else:
            return None    
    
    def write(self, addr, value) -> bool:
        if (self.checkRegBoundary(self.auto_int(addr))):
            try:
                self.regs[addr*4:(addr*4)+4] = int.to_bytes(value, 4, byteorder='little')
                return True
            except Exception as e:
                self.logger.error(f"Problem occured in writing {value} in register {addr}: {e}")
                return False

        else:
            return False
    
    def reset_channel(self, channel: int) -> dict:
        """
        Single channel reset
        
        Returns:
            dict: {
                'success': bool,
                'channel': int,
                'message': str,
                'was_on': bool
            }
        """
        if not self.checkChannelsBoundary(channel):
            return {
                'success': False,
                'channel': channel,
                'message': f"Invalid channel {channel}",
                'was_on': False
            }
        
        
        reg0 = self.read(0)
        reg1 = self.read(1)
        
        if reg0 is None or reg1 is None:
            return {
                'success': False,
                'channel': channel,
                'message': "Failed to read status",
                'was_on': False
            }
        
        mask = 1 << (channel)
        is_on = (reg0 & mask) and (reg1 & mask)
        
        if not is_on:
            return {
                'success': True,
                'channel': channel,
                'message': f"Channel {channel} is OFF",
                'was_on': False
            }
        
        
        new_reg0 = reg0 & ~ mask
        new_reg1 = reg1 & ~ mask
        
        if self.write(0, new_reg0) and self.write(1, new_reg1):
            return {
                'success': True,
                'channel': channel,
                'message': f"Channel {channel} reset",
                'was_on': True
            }
        else:
            return {
                'success': False,
                'channel': channel,
                'message': f"Write failed for channel {channel}",
                'was_on': True
            }

    def reset(self, channels):
        """
        Reset of different or all channels
        """
        channel_list = channels_definition(channels=channels, n_channels=self.num_channels)
        
        results = []
        for ch in channel_list:
            result = self.reset_channel(ch)
            results.append(result)
            if result['success']:
                self.logger.info(result['message'])
            else:
                self.logger.warning(result['message'])
        
        success_count = sum(1 for r in results if r['success'])
        
        reset_channels = [r['channel'] for r in results if r['success']]
        failed_channels = [r['channel'] for r in results if not r['success']]
        
        return {
            'success': success_count > 0,
            'reset_channels': reset_channels,
            'failed_channels': failed_channels
        }
    
    def boot_channel(self, channel) -> dict:
        """
        Start single channel in boot mode
        
        Returns:
            dict: {
                'success': bool,
                'channel': int,
                'message': str,
                'was_boot': bool
            }
        """

        if not self.checkChannelsBoundary(channel):
            return {
                'success': False,
                'channel': channel,
                'message': f"Invalid channel {channel}",
                'was_boot': False
            }
        
        reg0 = self.read(0)
        reg1 = self.read(1)
        reg17 = self.read(17)

        mask = 1 << (channel)
        is_boot = (reg0 & mask) and (reg1 & mask) and (reg17 & mask)

        if is_boot:
            return {
                'success': True,
                'channel': channel,
                'message': f"Channel {channel} was already in boot mode",
                'was_boot': True
            }
        
        new_reg0 = reg0 | mask
        new_reg1 = reg1 | mask
        new_reg17 = reg17 | mask
        
        if self.write(17, new_reg17) and self.write(0, new_reg0) and self.write(1, new_reg1) :
            return {
                'success': True,
                'channel': channel,
                'message': f"Channel {channel} in boot mode",
                'was_boot': True
            }
        else:
            return {
                'success': False,
                'channel': channel,
                'message': f"Write failed for channel {channel}",
                'was_boot': False
            }
        
    def boot(self, channels):
        """
        Set selected channels in boot mode.
        Only selected channels remain active.
        """
        channel_list = channels_definition(
            channels=channels,
            n_channels=self.num_channels,
        )

        if not channel_list:
            return {
                "success": False,
                "boot_channels": [],
                "failed_channels": [],
                "message": "No valid channels selected",
            }

        mask = 0
        for ch in channel_list:
            if not self.checkChannelsBoundary(ch):
                return {
                    "success": False,
                    "boot_channels": [],
                    "failed_channels": [ch],
                    "message": f"Invalid channel {ch}",
                }

            mask |= 1 << ch

        ok17 = self.write(17, mask)
        ok0 = self.write(0, mask)
        ok1 = self.write(1, mask)

        if ok0 and ok1 and ok17:
            return {
                "success": True,
                "boot_channels": channel_list,
                "failed_channels": [],
                "register_mask": mask,
                "message": f"Channels {channel_list} in boot mode",
            }

        return {
            "success": False,
            "boot_channels": [],
            "failed_channels": channel_list,
            "register_mask": mask,
            "message": f"Write failed for channels {channel_list}",
        }
    

    def start_channel(self, channel) -> dict:
        """
        Start single channel in data mode
        
        Returns:
            dict: {
                'success': bool,
                'channel': int,
                'message': str,
            }
        """

        if not self.checkChannelsBoundary(channel):
            return {
                'success': False,
                'channel': channel,
                'message': f"Invalid channel {channel}",
            }
        
        reg0 = self.read(0)
        reg1 = self.read(1)

        mask = 1 << (channel)
        is_on = (reg0 & mask) and (reg1 & mask) 

        if is_on:
            return {
                'success': True,
                'channel': channel,
                'message': f"Channel {channel} was already in data mode",
            }
        
        new_reg0 = reg0 | mask
        new_reg1 = reg1 | mask
        
        if self.write(1, new_reg1) and self.write(0, new_reg0) :
            return {
                'success': True,
                'channel': channel,
                'message': f"Channel {channel} in data mode",
            }
        else:
            return {
                'success': False,
                'channel': channel,
                'message': f"Write failed for channel {channel}",
            }
        
    def start(self, channels):
        """
        Set selected channels in data mode.
        Only selected channels remain active.
        """
        channel_list = channels_definition(
            channels=channels,
            n_channels=self.num_channels,
        )

        if not channel_list:
            return {
                "success": False,
                "started_channels": [],
                "failed_channels": [],
                "message": "No valid channels selected",
            }

        mask = 0
        for ch in channel_list:
            if not self.checkChannelsBoundary(ch):
                return {
                    "success": False,
                    "started_channels": [],
                    "failed_channels": [ch],
                    "message": f"Invalid channel {ch}",
                }

            mask |= 1 << ch

        ok1 = self.write(1, mask)
        ok0 = self.write(0, mask)

        if ok0 and ok1:
            return {
                "success": True,
                "started_channels": channel_list,
                "failed_channels": [],
                "register_mask": mask,
                "message": f"Channels {channel_list} in data mode",
            }

        return {
            "success": False,
            "started_channels": [],
            "failed_channels": channel_list,
            "register_mask": mask,
            "message": f"Write failed for channels {channel_list}",
        }
    

    def free_rate_monitoring(self, channels):
        """
        Read free-running rates for specified channels.
        Free mode registers: 20-26 (channel 0-6)
        """
        channel_list = channels_definition(channels=channels, n_channels=self.num_channels)
        
        if not channel_list:
            self.logger.warning(f"No valid channels specified: {channels}")
            return {
                "type": "data",
                "data_type": "free_rc_mon",
                "success": False,
                "message": "No valid channels"
            }
        
        result = {
            "type": "data",
            "data_type": "free_rc_mon",
            "timestamp": datetime.datetime.now().isoformat(),
            "channels": {}
        }
        
        for channel in channel_list:
            reg_addr = channel + 20  # Register 20-26
            value = self.read(reg_addr)
            
            result["channels"][str(channel)] = {
                "value": value,
                "register": reg_addr,
            }
            
            if value is None:
                self.logger.error(f"Failed to read free rate for channel {channel} (reg {reg_addr})")
        
        return result

    def trg_rate_monitoring(self, channels):
        """
        Read trigger-gated rates for specified channels.
        Trigger mode registers: 30-36 (channel 0-6)
        """
        channel_list = channels_definition(channels=channels, n_channels=self.num_channels)
        
        if not channel_list:
            self.logger.warning(f"No valid channels specified: {channels}")
            return {
                "type": "data",
                "data_type": "trg_rc_mon",
                "success": False,
                "message": "No valid channels"
            }
        
        result = {
            "type": "data",
            "data_type": "trg_rc_mon",
            "timestamp": datetime.datetime.now().isoformat(),
            "channels": {}
        }
        
        for channel in channel_list:
            reg_addr = channel + 32  # Register 32-38
            value = self.read(reg_addr)
            
            result["channels"][str(channel)] = {
                "value": value,
                "register": reg_addr,
            }
            
            if value is None:
                self.logger.error(f"Failed to read trigger rate for channel {channel} (reg {reg_addr})")
        
        return result

    def monitor_all_rates(self):
        """
        Convenience method: read both free and trigger rates for all channels.
        """
        all_channels = list(range(self.num_channels))
        
        return {
            "type": "data",
            "data_type": "all_rates",
            "timestamp": datetime.datetime.now().isoformat(),
            "free": self.free_rate_monitoring(all_channels),
            "trigger": self.trg_rate_monitoring(all_channels)
        }
        
