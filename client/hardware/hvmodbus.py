import pymodbus.client as ModbusClient
from pymodbus import FramerType, ModbusException
import struct
from client.utils.logger import get_logger


class HVModBus:
    
    
    def __init__(self, hv_port):
        self.logger = get_logger('hvmodbus')
        self.client = None
        self.port = hv_port
        self.ch_addr = None
        self.num_channels = 20
        
        self.client = ModbusClient.ModbusSerialClient(
            self.port, 
            framer=FramerType.RTU, 
            baudrate=115200,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=2
        )
        
        if not self.client.connect():
            self.logger.error("ModBus Serial Connection was not successfull")
 

    def _safe_read(self, addr, count, slave, desc="unknown"):
        rr = None
        try:
            rr = self.client.read_holding_registers(address=addr, count=count, device_id=slave)
            if rr is None or rr.isError():
                self.logger.error(f"Invalid response when reading for {desc} at {hex(addr)}")
            return rr.registers
        except ModbusException as e:
            self.logger.error(f"Exception during read of {desc}: {e}")
            return None
    
    def _safe_write(self, addr, value, slave, desc="unknown"):
        try:
            rr = self.client.write_register(address=addr, value=value, slave=slave)
            if rr is None or rr.isError():
                self.logger.error(f"Invalid response when writing for {desc} at {hex(addr)}")
                return 
        except ModbusException as e:
            self.logger.error(f"Exception during write of {desc}: {e}")
            return 

    def _safe_write_multiple(self, addr, values, slave, desc="unknown"):
        try:
            rr = self.client.write_registers(address=addr, values=values, slave=slave)
            if rr is None or rr.isError():
                raise self.logger.error(f"Invalid response when writing for {desc} at {hex(addr)}")
        except ModbusException as e:
            raise self.logger.error(f"Exception during write of {desc}: {e}")
        
    def handleInterrupt(self):
        try:
            if hasattr(self.client, "connected") and not self.client.connected:
                self.client.close()
                return False
        except Exception as e:
            self.logger.error(f"Error checking the connection status of the client: {e}")
            return False
    
    
    def open(self, channel):
        self.handleInterrupt()
        self._safe_read(addr=0, count=1, slave=channel, desc="opening channel")
        self.ch_addr = channel
        return True
    
    def checkAddressBoundary(self, channel):
        if (channel < 0 or channel > self.num_channels):
            self.logger.warning(f"Channel {channel} is out of boundary: [0, {self.num_channels}]")
            return False
        return True
    
    def isConnected(self):
        return self.ch_addr is not None
    
    def getAddress(self):
        return self.ch_addr
    
    def checkConnection(self):
        if not self.isConnected():
            self.logger.error("Was not possible to check for connection")
            return False
        return True
    
    def checkAddress(self, ch_addr):
        if self.open(channel=ch_addr):
            if self.getAddress() == ch_addr and self.isConnected() : #Address and channel as variables go from 1 to 7
                return True
            else:
                self.logger.warning(f"The HV board selected doesn't match the channel interested: {self.getAddress} != {ch_addr}")
                return False
        else:
            self.logger.warning(f"It was not possible to check the address: error in opening the selected channel: {ch_addr}")
            return False
        
    def setModbusAddress(self, ch_addr, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x00, value=ch_addr, slave=slave, desc="address set")

    def getStatus(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=6, count=1, slave=slave, desc="get status")
        return rr[0]
    
    def getVoltage(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x2A, count=2, slave=slave, desc="get voltage")
        rr.reverse()
        return self.client.convert_from_registers(rr, data_type=self.client.DATATYPE.INT32) / 1000
    
    def getVoltageSet(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x26, count=1, slave=slave, desc="get voltage setted")
        return rr[0]
    
    def setVoltageSet(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x26, value=value, slave=slave, desc="voltage set")
        
    def getCurrent(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x28, count=2, slave=slave, desc="get current")
        rr.reverse()
        return self.client.convert_from_registers(rr, data_type=self.client.DATATYPE.INT32) / 1000
    
    def getTemperature(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x07, count=1, slave=slave, desc="get temperature")
        return rr[0]
    
    def convertTemperature(self, value):
        q = (value & 0xFF) / 1000
        i = (value >> 8) & 0xFF
        return round(q+i, 2)
    
    def getRate(self, fmt=str, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x23, count=2, slave=slave, desc="get rate")
        rup = rr[0]
        rdn = rr[1]
        return f'{rup}/{rdn}' if fmt == str else (rup, rdn)
    
    def setRateRampup(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x23, value=value, slave=slave, desc="set rate ramp-up")
        
    def setRateRampdown(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x24, value=value, slave=slave, desc="set rate ramp-down")
        
    def getLimit(self, fmt=str, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0, count=48, slave=slave, desc="gte limits")
        lv = rr[0x27]
        li = rr[0x25]
        lt = rr[0x2F]
        ltt = rr[0x22]
        return f'{lv}/{li}/{lt}/{ltt}' if fmt == str else (lv, li, lt, ltt)
    
    def setLimitVoltage(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x27, value=value, slave=slave, desc="set limit voltage")
        
    def setLimitCurrent(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x25, value=value, slave=slave, desc="set limit current")
        
    def setLimitTemperature(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x2F, value=value, slave=slave, desc="set limit temperature")
        
    def setLimitTriptime(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x22, value=value, slave=slave, desc="set limit trip time")
        
    def setThreshold(self, value, slave=None):
        slave = self.ch_addr if slave is None else slave
        self._safe_write(addr=0x2D, value=value, slave=slave, desc="set threshold")
        
    def getThreshold(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x2D, count=1, slave=slave, desc="get threshold")
        return rr[0]
    
    def getAlarm(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x2E, count=1, slave=slave, desc="get alarm")
        return rr[0]
    
    def getVref(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x2E, count=1, slave=slave, desc="get vref")
        return rr[0] / 10
    
    def powerOn(self, slave=None):
        slave = self.ch_addr if slave==None else slave
        try:
            self.client.write_coil(address=1, value=True, slave=slave)
        except ModbusException as e:
            self.logger.error(f"Error occured powering on channel {self.ch_addr}:{e}")
            raise e
    
    def powerOff(self, slave=None):
        slave = self.ch_addr if slave==None else slave
        try:
            self.client.write_coil(address=1, value=False, slave=slave)
        except ModbusException as e:
            self.logger.error(f"Error occured powering off channel {self.ch_addr}:{e}")
            raise e
    
    def reset(self, slave=None):
        slave = self.ch_addr if slave==None else slave
        try:
            self.client.write_coil(address=2, value=True, slave=slave)
        except ModbusException as e:
            self.logger.error(f"Error occured resetting channel {self.ch_addr}:{e}")
            raise e
    
    def getInfo(self, slave=None):
        slave = self.ch_addr if slave==None else slave
        l = None
        try:
            l = self.client.read_holding_registers(address=0x02, count=1, device_id=slave).registers
            fwver = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x08, count=6, device_id=slave).registers
            pmtsn = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x0E, count=6, device_id=slave).registers
            hvsn = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x14, count=6, device_id=slave).registers
            febsn = struct.pack(f'>{len(l)}h', *l).decode()
            l = self.client.read_holding_registers(address=0x04, count=2, device_id=slave).registers
            devid = (l[1] << 16) + l[0]
            return (fwver, pmtsn, hvsn, febsn, devid)
        except ModbusException as e:
            self.logger.error(f"Error occured getting info of channel {self.ch_addr}:{e}")
            raise e
    
    def readMonRegisters(self, slave=None):
        slave = self.addr if slave==None else slave
        rr = None
        monData = {}
        try:
            rr = self.client.read_holding_registers(address=0, count=48, device_id=slave)
            monData['status'] = rr.registers[0x0006]
            monData['Vset'] = rr.registers[0x0026]
            monData['V'] = ((rr.registers[0x002B] << 16) + rr.registers[0x002A]) / 1000
            monData['I'] = ((rr.registers[0x0029] << 16) + rr.registers[0x0028]) / 1000
            monData['T'] = self.convertTemperature(rr.registers[0x0007])
            monData['rateUP'] = rr.registers[0x0023]
            monData['rateDN'] = rr.registers[0x0024]
            monData['limitV'] = rr.registers[0x0027]
            monData['limitI'] = rr.registers[0x0025]
            monData['limitT'] = rr.registers[0x002F]
            monData['limitTRIP'] = rr.registers[0x0022]
            monData['threshold'] = rr.registers[0x002D]
            monData['alarm'] = rr.registers[0x002E]
            return monData
        except ModbusException as e:
            self.logger.error(f"Error occured reading monitoring registers of channel {self.ch_addr}:{e}")
            raise e
    
    def statusString(self, statusCode):
        statuses = {0: 'UP', 1: 'DOWN', 2: 'RUP', 3: 'RDN', 4: 'TUP', 5: 'TDN', 6: 'TRIP'}
        return statuses.get(statusCode, 'undef')

    def alarmString(self, alarmCode):
        msg = ' '
        if (alarmCode == 0):
            return 'none'
        if (alarmCode & 1):
            msg = msg + 'OV '
        if (alarmCode & 2):
            msg = msg + 'UV '
        if (alarmCode & 4):
            msg = msg + 'OC '
        if (alarmCode & 8):
            msg = msg + 'OT '
        return msg
    
    def readCalibRegisters(self, slave=None):
        slave = self.ch_addr if slave is None else slave
        rr = self._safe_read(addr=0x30, count=5, slave=slave, desc="read calib reg")
        mlsb = rr.registers[0]
        mmsb = rr.registers[1]
        qlsb = rr.registers[2]
        qmsb = rr.registers[3]
        calibt = rr.registers[4]

        calibm = ((mmsb << 16) + mlsb)
        calibm = struct.unpack('l', struct.pack('L', calibm & 0xffffffff))[0]
        calibm = calibm / 10000

        calibq = ((qmsb << 16) + qlsb)
        calibq = struct.unpack('l', struct.pack('L', calibq & 0xffffffff))[0]
        calibq = calibq / 10000

        calibt = calibt / 1.6890722

        return (calibm, calibq, calibt)
    
    def writeCalibSlope(self, slope, slave=None):
        slave = self.ch_addr if slave is None else slave
        slope = int(slope * 10000)
        lsb = (slope & 0xFFFF)
        msb = (slope >> 16) & 0xFFFF
        self._safe_write_multiple(addr=0x30, values=[lsb, msb], slave=slave, desc="write slop")

    def writeCalibOffset(self, offset, slave=None):
        slave = self.ch_addr if slave is None else slave
        offset = int(offset * 10000)
        lsb = (offset & 0xFFFF)
        msb = (offset >> 16) & 0xFFFF
        self._safe_write_multiple(addr=0x32, values=[lsb, msb], slave=slave, desc="write offset")
    
    def writeCalibDiscr(self, discr, slave=None):
        slave = self.ch_addr if slave is None else slave
        discr = int(discr * 1.6890722)
        self._safe_write(addr=0x34, value=discr, slave=slave, desc="write discr")
    

