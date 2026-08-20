# TLA2024.py
import smbus2
import time
from client.utils.logger import get_logger


def conf_struct(os=0x01, mux=0x00, pga=0x02, mode=0x01, dr=0x04, reserved=0x00):
    """
    Costruisce il valore a 16 bit per il registro di configurazione del TLA2024.
    :param os: Operational Status o Start Single Conversion (1 bit)
    :param mux: Multiplexer configuration (3 bit)
    :param pga: Programmable Gain Amplifier (3 bit)
    :param mode: Operating Mode (1 bit)
    :param dr: Data Rate (3 bit)
    :param reserved: Riservato, impostato a 0 (5 bit)
    :return: Valore del registro di configurazione a 16 bit.
    """
    conf_msb = (
        (os & 0x01) << 7 |
        (mux & 0x07) << 4 |
        (pga & 0x07) << 1 |
        (mode & 0x01)
    )
    conf_lsb = (
        (dr & 0x07) << 5 |
        (reserved & 0x1F)
    )
    return [conf_msb, conf_lsb]



# AIN0/AIN2: partitori resistivi sulle rotaie di tensione (schema)
AIN0_DIVIDER_RATIO = (40_000 + 20_000) / 20_000   # R41/R37 -> x3
AIN2_DIVIDER_RATIO = (20_000 + 20_000) / 20_000   # R30/R31 -> x2

# AIN1 ("I mon 1"): uscita dell'INA139 (U21), formula da datasheet TI:
# V_OUT = I_S * R_SHUNT * R_LOAD / 1kOhm. 
INA139_GM_DENOMINATOR_OHM = 1000.0
IMON1_SHUNT_OHM = 0.002        # R62
IMON1_LOAD_OHM = 500_000.0     # R34

READY_POLL_TIMEOUT_S = 0.5
READY_POLL_INTERVAL_S = 0.001


def imon1_mv_to_amps(v_out_mv: float) -> float:
    """Converte la lettura di 'I mon 1' (mV, dal TLA2024) in Ampere reali."""
    v_out_v = v_out_mv / 1000.0
    gain = IMON1_SHUNT_OHM * IMON1_LOAD_OHM / INA139_GM_DENOMINATOR_OHM  # = 1 V/A
    return v_out_v / gain


class TLA2024():

    CONFIGURATION_REGISTER = 0x01
    DATA_REGISTER = 0x00

    def __init__(self, chip_addr, candidate_buses=(0, 1, 2)):
        self.chip_addr = chip_addr
        self.iic_bus = None
        self.i2cbus = None

        self.logger = get_logger("tla2024")

        for bus_idx in candidate_buses:
            try:
                bus = smbus2.SMBus(bus_idx)
            except (IOError, FileNotFoundError):
                continue

            try:
                # Il TLA2024 non ha un registro di chip-ID: l'unica verifica
                # possibile e' che l'indirizzo risponda senza errore (probe
                # piu' debole di quello del BME280, che verifica il chip ID).
                bus.read_i2c_block_data(chip_addr, self.CONFIGURATION_REGISTER, 2)
            except OSError:
                bus.close()
                continue

            self.iic_bus = bus_idx
            self.i2cbus = bus
            self.logger.debug(f"TLA2024 found on I2C bus {bus_idx}")
            break

        if self.i2cbus is None:
            self.logger.error(
                f"TLA2024 not found on any candidate I2C bus {list(candidate_buses)} "
                f"(address 0x{chip_addr:02X})"
            )

    def read_conf_reg(self):
        conf = self.i2cbus.read_i2c_block_data(self.chip_addr, self.CONFIGURATION_REGISTER, 2)
        return (conf[0] << 8) | conf[1]

    def write_conf_reg(self, data):
        self.i2cbus.write_i2c_block_data(self.chip_addr, self.CONFIGURATION_REGISTER, data)

    def read_data_reg(self):
        data = self.i2cbus.read_i2c_block_data(self.chip_addr, self.DATA_REGISTER, 2)
        raw = (data[0] << 8) | data[1]
        value = raw >> 4
        if value & 0x800:          # bit di segno nel valore a 12 bit
            value -= 4096
        return value

    def isReady(self):
        ready = self.read_conf_reg() & 0x8000
        return bool(ready)

    def readAll(self):
        if self.i2cbus is None:
            self.logger.error("Cannot read TLA2024: no I2C bus available")
            return None

        output_mv = []
        mux = [0x04, 0x05, 0x06]
        fsr = [0x02, 0x02, 0x02]
        lsb_mv = [1, 1, 1]     # PGA=010 -> FSR=+-2.048V -> 1 mV/LSB (Table 1 datasheet)
        os = 0x01
        mode = 0x01
        dr = 0x04
        reserved = 0x03

        for i in range(3):
            config = conf_struct(os=os, mux=mux[i], pga=fsr[i], mode=mode, dr=dr, reserved=reserved)
            self.write_conf_reg(config)

            start = time.monotonic()
            while not self.isReady():
                if time.monotonic() - start > READY_POLL_TIMEOUT_S:
                    self.logger.error(f"TLA2024 conversion timeout on channel AIN{i}")
                    return None
                time.sleep(READY_POLL_INTERVAL_S)

            output_mv.append(self.read_data_reg() * lsb_mv[i])

        return output_mv

    def readAllPhysical(self):

        raw_mv = self.readAll()
        if raw_mv is None:
            return None

        ain0_mv, ain1_mv, ain2_mv = raw_mv

        return {
            "rail_ain0_v": (ain0_mv / 1000.0) * AIN0_DIVIDER_RATIO,
            "i_mon_1_a": imon1_mv_to_amps(ain1_mv),
            "rail_ain2_v": (ain2_mv / 1000.0) * AIN2_DIVIDER_RATIO,
        }
        
        
    def close(self) -> None:
        if self.i2cbus is None:
            return
        try:
            self.i2cbus.close()
        except Exception as e:
            self.logger.error(f"Error while closing TLA2024 I2C bus: {e}")