import smbus2
import time
from functools import wraps

from client.utils.logger import get_logger



BMM150_CHIP_ID = 0x32

BMM150_REG_CHIP_ID = 0x40
BMM150_REG_DATA_X_LSB = 0x42
BMM150_REG_POWER_CONTROL = 0x4B
BMM150_REG_OP_MODE = 0x4C

BMM150_DIG_X1_REG = 0x5D
BMM150_DIG_Z4_LSB_REG = 0x62
BMM150_DIG_Z2_LSB_REG = 0x68

BMM150_NORM_SELF_TEST_X = 0x42
BMM150_NORM_SELF_TEST_Y = 0x44
BMM150_NORM_SELF_TEST_Z = 0x46

BMM150_REG_REP_XY = 0x51
BMM150_REG_REP_Z = 0x52

BMM150_REPXY_REGULAR = 0x04
BMM150_REPZ_REGULAR = 0x07


BMM150_START_UP_TIME_S = 3000 / 1_000_000   # 3000us -> 3ms
BMM150_OP_MODE_MASK = 0x06                   # bit 1-2
BMM150_OP_MODE_POS = 1

BMM150_OPMODE_NORMAL = 0x00
BMM150_OPMODE_FORCED = 0x01
BMM150_OPMODE_SLEEP = 0x03

BMM150_OVERFLOW_ADCVAL_XYAXES_FLIP = -4096
BMM150_OVERFLOW_ADCVAL_ZAXIS_HALL = -16384
BMM150_OVERFLOW_OUTPUT = 0.0

BMM150_FORCED_MODE_SETTLING_S = 0.5   # margine oltre il tempo tipico di conversione


class BMM150():

    def __init__(self, chip_addr, candidate_buses=(0, 1, 2)):
        self.chip_addr = chip_addr
        self.iic_bus = None
        self.i2cbus = None
        self.trim_data = None

        self.logger = get_logger("bmm150")

        for bus_idx in candidate_buses:
            try:
                bus = smbus2.SMBus(bus_idx)
            except (IOError, FileNotFoundError):
                continue

            try:
                pwr = bus.read_byte_data(chip_addr, BMM150_REG_POWER_CONTROL)
                bus.write_byte_data(chip_addr, BMM150_REG_POWER_CONTROL, pwr | 0x01)
                time.sleep(BMM150_START_UP_TIME_S)

                chip_id = bus.read_byte_data(chip_addr, BMM150_REG_CHIP_ID)
            except OSError:
                bus.close()
                continue

            if chip_id == BMM150_CHIP_ID:
                self.iic_bus = bus_idx
                self.i2cbus = bus
                self.logger.debug(f"BMM150 found on I2C bus {bus_idx} (chip_id=0x{chip_id:02X})")
                break

            bus.close()

        if self.i2cbus is None:
            self.logger.error(
                f"BMM150 not found on any candidate I2C bus {list(candidate_buses)} "
                f"(address 0x{chip_addr:02X})"
            )
            return

        self._perform_normal_self_test()
        self.trim_data = self._read_trim_registers()
        self.logger.debug(f"BMM150 found on I2C bus {bus_idx}")


    def _read_reg(self, reg_addr):
        return self.i2cbus.read_byte_data(self.chip_addr, reg_addr)

    def _read_regs(self, start_reg_addr, num_regs):
        return self.i2cbus.read_i2c_block_data(self.chip_addr, start_reg_addr, num_regs)

    def _write_reg(self, reg_addr, reg_data) -> bool:
        try:
            self.i2cbus.write_byte_data(self.chip_addr, reg_addr, reg_data)
            return True
        except OSError as e:
            self.logger.warning(f"Failed to write 0x{reg_data:02X} to register 0x{reg_addr:02X}: {e}")
            return False


    def _to_signed(self, value, bits):
        limit = 1 << (bits - 1)
        return value - (1 << bits) if value & limit else value

    def _read_trim_registers(self):

        trim_x1y1 = self._read_regs(BMM150_DIG_X1_REG, 2)
        trim_xyz_data = self._read_regs(BMM150_DIG_Z4_LSB_REG, 4)
        trim_xy1xy2 = self._read_regs(BMM150_DIG_Z2_LSB_REG, 10)

        dig_x1 = self._to_signed(trim_x1y1[0], 8)
        dig_y1 = self._to_signed(trim_x1y1[1], 8)
        dig_x2 = self._to_signed(trim_xyz_data[2], 8)
        dig_y2 = self._to_signed(trim_xyz_data[3], 8)

        dig_z1 = (trim_xy1xy2[3] << 8) | trim_xy1xy2[2]   # unsigned
        dig_z2 = self._to_signed((trim_xy1xy2[1] << 8) | trim_xy1xy2[0], 16)
        dig_z3 = self._to_signed((trim_xy1xy2[7] << 8) | trim_xy1xy2[6], 16)
        dig_z4 = self._to_signed((trim_xyz_data[1] << 8) | trim_xyz_data[0], 16)

        dig_xy1 = trim_xy1xy2[9]    # unsigned
        dig_xy2 = self._to_signed(trim_xy1xy2[8], 8)

        dig_xyz1 = ((trim_xy1xy2[5] & 0x7F) << 8) | trim_xy1xy2[4]   # unsigned

        return {
            "dig_x1": dig_x1, "dig_y1": dig_y1,
            "dig_x2": dig_x2, "dig_y2": dig_y2,
            "dig_z1": dig_z1, "dig_z2": dig_z2, "dig_z3": dig_z3, "dig_z4": dig_z4,
            "dig_xy1": dig_xy1, "dig_xy2": dig_xy2,
            "dig_xyz1": dig_xyz1,
        }

    def _perform_normal_self_test(self) -> bool:
        self.set_sleep_mode()

        reg_data = self._read_reg(BMM150_REG_OP_MODE)
        self._write_reg(BMM150_REG_OP_MODE, reg_data | 0x01)   # Self Test bit = bit 0

        start = time.monotonic()
        while self._read_reg(BMM150_REG_OP_MODE) & 0x01 != 0:
            if time.monotonic() - start > 1.0:
                self.logger.error("BMM150 normal self-test timeout")
                return False
            time.sleep(0.05)

        self_test_x = bool(self._read_reg(BMM150_NORM_SELF_TEST_X) & 0x01)
        self_test_y = bool(self._read_reg(BMM150_NORM_SELF_TEST_Y) & 0x01)
        self_test_z = bool(self._read_reg(BMM150_NORM_SELF_TEST_Z) & 0x01)

        success = self_test_x and self_test_y and self_test_z

        if success:
            self.logger.info("BMM150 normal self-test successful")
        else:
            failed_axes = [
                axis for axis, ok in (("X", self_test_x), ("Y", self_test_y), ("Z", self_test_z)) if not ok
            ]
            self.logger.warning(f"BMM150 normal self-test failed on axis: {failed_axes}")

        return success
    
    def set_repetition_regular(self) -> bool:
        ok_xy = self._write_reg(BMM150_REG_REP_XY, BMM150_REPXY_REGULAR)
        ok_z = self._write_reg(BMM150_REG_REP_Z, BMM150_REPZ_REGULAR)
        return ok_xy and ok_z

    def get_pwr_ctrl_bit(self) -> bool:
        if self.i2cbus is None:
            return False
        return bool(self._read_reg(BMM150_REG_POWER_CONTROL) & 0x01)

    def set_power_control_bit(self, enable: bool) -> bool:
        if self.i2cbus is None:
            self.logger.error("Cannot set power control bit: no I2C bus available")
            return False

        current = self._read_reg(BMM150_REG_POWER_CONTROL)
        new_value = (current | 0x01) if enable else (current & ~0x01)

        ok = self._write_reg(BMM150_REG_POWER_CONTROL, new_value)
        if ok and enable:
            time.sleep(BMM150_START_UP_TIME_S)
        return ok

    def require_power_on(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.get_pwr_ctrl_bit():
                self.logger.warning(
                    f"{func.__name__}: BMM150 is in suspend mode (power bit=0), cannot execute"
                )
                return None
            return func(self, *args, **kwargs)
        return wrapper


    @require_power_on
    def get_op_mode(self):
        return (self._read_reg(BMM150_REG_OP_MODE) & BMM150_OP_MODE_MASK) >> BMM150_OP_MODE_POS

    @require_power_on
    def _set_op_mode(self, op_mode: int) -> bool:
        current = self._read_reg(BMM150_REG_OP_MODE)
        cleared = current & ~BMM150_OP_MODE_MASK
        new_value = cleared | ((op_mode << BMM150_OP_MODE_POS) & BMM150_OP_MODE_MASK)
        return self._write_reg(BMM150_REG_OP_MODE, new_value)

    @require_power_on
    def set_force_mode(self) -> bool:
        return self._set_op_mode(BMM150_OPMODE_FORCED)

    @require_power_on
    def set_sleep_mode(self) -> bool:
        return self._set_op_mode(BMM150_OPMODE_SLEEP)


    @require_power_on
    def read_raw_xyz_rhall(self):
        data = self._read_regs(BMM150_REG_DATA_X_LSB, 8)

        x = ((data[1] << 8) | data[0]) >> 3
        if x & 0x1000:
            x -= 0x2000

        y = ((data[3] << 8) | data[2]) >> 3
        if y & 0x1000:
            y -= 0x2000

        z = ((data[5] << 8) | data[4]) >> 1
        if z & 0x8000:
            z -= 0x10000

        rhall = ((data[7] << 8) | data[6]) >> 2

        return x, y, z, rhall


    def _compensate_x(self, mag_data_x, data_rhall) -> float:
        t = self.trim_data
        if mag_data_x == BMM150_OVERFLOW_ADCVAL_XYAXES_FLIP or data_rhall == 0 or t["dig_xyz1"] == 0:
            return BMM150_OVERFLOW_OUTPUT

        process_comp_x0 = t["dig_xyz1"] * 16384.0 / data_rhall
        retval = process_comp_x0 - 16384.0
        process_comp_x1 = t["dig_xy2"] * (retval * retval / 268435456.0)
        process_comp_x2 = process_comp_x1 + retval * t["dig_xy1"] / 16384.0
        process_comp_x3 = t["dig_x2"] + 160.0
        process_comp_x4 = mag_data_x * ((process_comp_x2 + 256.0) * process_comp_x3)
        return ((process_comp_x4 / 8192.0) + (t["dig_x1"] * 8.0)) / 16.0

    def _compensate_y(self, mag_data_y, data_rhall) -> float:
        t = self.trim_data
        if mag_data_y == BMM150_OVERFLOW_ADCVAL_XYAXES_FLIP or data_rhall == 0 or t["dig_xyz1"] == 0:
            return BMM150_OVERFLOW_OUTPUT

        process_comp_y0 = t["dig_xyz1"] * 16384.0 / data_rhall
        retval = process_comp_y0 - 16384.0
        process_comp_y1 = t["dig_xy2"] * (retval * retval / 268435456.0)
        process_comp_y2 = process_comp_y1 + retval * t["dig_xy1"] / 16384.0
        process_comp_y3 = t["dig_y2"] + 160.0
        process_comp_y4 = mag_data_y * ((process_comp_y2 + 256.0) * process_comp_y3)
        return ((process_comp_y4 / 8192.0) + (t["dig_y1"] * 8.0)) / 16.0

    def _compensate_z(self, mag_data_z, data_rhall) -> float:
        t = self.trim_data
        if (mag_data_z == BMM150_OVERFLOW_ADCVAL_ZAXIS_HALL or t["dig_z2"] == 0
                or t["dig_z1"] == 0 or t["dig_xyz1"] == 0 or data_rhall == 0):
            return BMM150_OVERFLOW_OUTPUT

        process_comp_z0 = mag_data_z - t["dig_z4"]
        process_comp_z1 = data_rhall - t["dig_xyz1"]
        process_comp_z2 = t["dig_z3"] * process_comp_z1
        process_comp_z3 = t["dig_z1"] * data_rhall / 32768.0
        process_comp_z4 = t["dig_z2"] + process_comp_z3
        process_comp_z5 = (process_comp_z0 * 131072.0) - process_comp_z2
        return (process_comp_z5 / (process_comp_z4 * 4.0)) / 16.0

    def read_compensated_xyz_ut(self):
        raw = self.read_raw_xyz_rhall()
        if raw is None:
            return None

        x, y, z, rhall = raw

        return (
            self._compensate_x(x, rhall),
            self._compensate_y(y, rhall),
            self._compensate_z(z, rhall),
        )

    def read_force_mode(self):
        if not self.set_power_control_bit(True):
            return None
        if not self.set_force_mode():
            return None

        start = time.monotonic()
        while self._read_reg(BMM150_REG_OP_MODE) & BMM150_OP_MODE_MASK != (BMM150_OPMODE_SLEEP << BMM150_OP_MODE_POS):
            if time.monotonic() - start > BMM150_FORCED_MODE_SETTLING_S:
                self.logger.error("BMM150 forced mode measurement timeout")
                return None
            time.sleep(0.002)

        return self.read_compensated_xyz_ut()
    
    
    def close(self) -> None:
        if self.i2cbus is None:
            return
        try:
            self.i2cbus.close()
        except Exception as e:
            self.logger.error(f"Error while closing BMM150 I2C bus: {e}")