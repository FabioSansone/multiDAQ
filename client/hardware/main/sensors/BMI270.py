import smbus2
import time
from functools import wraps

from client.utils.logger import get_logger
from client.hardware.main.sensors.bmi270_config_file import BMI270_CONFIG_FILE


BMI270_PWR_CONF_REG = 0x7C
BMI270_PWR_CTRL_REG = 0x7D
BMI270_INIT_CTRL_REG = 0x59
BMI270_INIT_DATA_REG = 0x5E
BMI270_INTERNAL_STATUS_REG = 0x21
BMI270_CHIP_ID_REG = 0x00
BMI270_INIT_ADDR_0_REG = 0x5B
BMI270_INIT_ADDR_1_REG = 0x5C
BMI270_CMD_REG = 0x7E
BMI270_ACC_RANGE_REG = 0x41
BMI270_ACC_SELF_TEST_REG = 0x6D
BMI270_ACC_CONF_REG = 0x40
BMI270_ACC_X_LSB_REG = 0x0C
BMI270_GYR_SELF_TEST_AXES_REG = 0x6E
BMI270_GYR_CONF_REG = 0x42
BMI270_GYR_RANGE_REG = 0x43
BMI270_GYR_X_LSB_REG = 0x12   # DATA_14, subito dopo i 6 byte dell'accelerometro (DATA_8-13)
BMI270_NV_CONF_REG = 0x70
BMI270_OFFSET_6_REG = 0x77
BMI270_TEMPERATURE_0_REG = 0x22
BMI270_STATUS_REG = 0x03
BMI270_INT_STATUS_0_REG = 0x1C
BMI270_FEAT_PAGE_REG = 0x2F

BMI270_ODR_50HZ = 0x07   # comune ad ACC_CONF e GYR_CONF, stesso valore/codifica

ACC_RANGE_TO_G = {0x00: 2.0, 0x01: 4.0, 0x02: 8.0, 0x03: 16.0}
GYR_RANGE_TO_DPS = {0x00: 2000.0, 0x01: 1000.0, 0x02: 500.0, 0x03: 250.0, 0x04: 125.0}

BMI270_ACC_SELF_TEST_EN_BIT = 0x01
BMI270_ACC_SELF_TEST_SIGN_BIT = 0x04

BMI270_SELF_TEST_PRE_WAIT_S = 0.003
BMI270_SELF_TEST_SIGN_WAIT_S = 0.051

BMI2_ST_ACC_X_SIG_MIN_DIFF = 16000
BMI2_ST_ACC_Y_SIG_MIN_DIFF = -15000
BMI2_ST_ACC_Z_SIG_MIN_DIFF = 10000

BMI270_G_TRIGGER_CMD = 0x02
BMI270_GYR_SELF_TEST_TIMEOUT_S = 0.5

BMI270_CHIP_ID = 0x24
BMI270_ADV_POWER_SAVE_OFF_WAIT_S = 0.001  # 1 ms
BMI270_CONFIG_WRITE_TIME_S = 0.1  # 100 ms
BMI270_CONFIG_CHUNK_SIZE = 32
BMI270_SOFTRESET_WAIT_S = 0.005  # 5 ms

BMI270_DATA_READY_TIMEOUT_S = 0.1   # ampio margine sopra un periodo a 50Hz (20ms)

# --- Any-motion detection (datasheet par. 4.8.2, registri FEATURES pagina 1) ---
# Le registrazioni FEATURES sono multiplexate su 8 pagine da 16 registri
# (0x30-0x3F) selezionate tramite FEAT_PAGE.page; ANYMO_1/ANYMO_2 vivono
# entrambe in pagina 1 e vanno scritte come word a 16 bit (LSB, poi MSB).
BMI270_FEAT_PAGE_ANY_MOTION = 0x01
BMI270_FEATURES_ANYMO_1_OFFSET = 0x3C
BMI270_FEATURES_ANYMO_2_OFFSET = 0x3E

BMI270_ANY_MOTION_OUT_BIT = 1 << 6   # INT_STATUS_0.any_motion_out

# ANYMO_1: duration (bit 12..0) | select_x (bit13) | select_y (bit14) | select_z (bit15)
BMI270_ANYMO_DURATION_MASK = 0x1FFF
BMI270_ANYMO_SELECT_X_BIT = 1 << 13
BMI270_ANYMO_SELECT_Y_BIT = 1 << 14
BMI270_ANYMO_SELECT_Z_BIT = 1 << 15

# ANYMO_2: threshold (bit 10..0) | out_conf (bit14..11) | enable (bit15)
BMI270_ANYMO_THRESHOLD_MASK = 0x7FF
BMI270_ANYMO_OUT_CONF_SHIFT = 11
BMI270_ANYMO_OUT_CONF_BIT_6 = 0x07   # instrada l'uscita su INT_STATUS_0 bit 6 (any_motion_out), come da reset default
BMI270_ANYMO_ENABLE_BIT = 1 << 15

BMI270_ANYMO_THRESHOLD_MG_PER_LSB = 1000.0 / 2048.0   # range 0..1g su 11 bit (0xAA=170 -> 83mg, da datasheet)
BMI270_ANYMO_DURATION_S_PER_LSB = 1.0 / 50.0          # 1 campione a 50Hz = 20ms

BMI270_ANYMO_THRESHOLD_DEFAULT_MG = 83.0   # reset value 0xAA
BMI270_ANYMO_DURATION_DEFAULT_S = 0.1      # reset value 5 campioni = 100ms


class BMI270():

    def __init__(self, chip_addr, candidate_buses=(0, 1, 2)):
        self.chip_addr = chip_addr
        self.iic_bus = None
        self.i2cbus = None
        self.trim_data = None
        self._acc_range = None
        self._gyr_range = None

        self.logger = get_logger("bmi270")

        for bus_idx in candidate_buses:
            try:
                bus = smbus2.SMBus(bus_idx)
            except (IOError, FileNotFoundError):
                continue

            try:
                if not self._initialize_device(bus):
                    bus.close()
                    continue

                chip_id = self._read_reg(BMI270_CHIP_ID_REG, slave_bus=bus)
            except OSError:
                bus.close()
                continue

            if chip_id == BMI270_CHIP_ID:
                self.iic_bus = bus_idx
                self.i2cbus = bus
                self.logger.debug(f"BMI270 found on I2C bus {bus_idx} (chip_id=0x{chip_id:02X})")
                break

            bus.close()

        if self.i2cbus is None:
            self.logger.error(
                f"BMI270 not found on any candidate I2C bus {list(candidate_buses)} "
                f"(address 0x{chip_addr:02X})"
            )
            return

        self._perform_gyr_self_test(slave_bus=self.i2cbus)
        self._perform_acc_self_test(slave_bus=self.i2cbus)

        if not self._initialize_device(self.i2cbus):
            self.logger.error("Re-initialization after self-tests failed")
            return

        self.configure_normal_operation(acc_range=0x02, gyr_range=0x02, slave_bus=self.i2cbus)


    def _read_reg(self, reg_addr, slave_bus=None):
        i2cbus = slave_bus if slave_bus is not None else self.i2cbus
        return i2cbus.read_byte_data(self.chip_addr, reg_addr)

    def _read_regs(self, start_reg_addr, num_regs, slave_bus=None):
        i2cbus = slave_bus if slave_bus is not None else self.i2cbus
        return i2cbus.read_i2c_block_data(self.chip_addr, start_reg_addr, num_regs)

    def _write_reg(self, reg_addr, reg_data, slave_bus=None) -> bool:
        i2cbus = slave_bus if slave_bus is not None else self.i2cbus
        try:
            i2cbus.write_byte_data(self.chip_addr, reg_addr, reg_data)
            return True
        except OSError as e:
            self.logger.warning(f"Failed to write 0x{reg_data:02X} to register 0x{reg_addr:02X}: {e}")
            return False

    def _to_signed(self, value, bits):
        limit = 1 << (bits - 1)
        return value - (1 << bits) if value & limit else value


    def _disable_advanced_power_save(self, slave_bus) -> bool:
        current = self._read_reg(BMI270_PWR_CONF_REG, slave_bus=slave_bus)
        ok = self._write_reg(BMI270_PWR_CONF_REG, current & ~0x01, slave_bus=slave_bus)
        if ok:
            time.sleep(BMI270_ADV_POWER_SAVE_OFF_WAIT_S)
        return ok

    def _write_config_file(self, config_data, slave_bus, chunk_size=BMI270_CONFIG_CHUNK_SIZE) -> bool:
        for offset in range(0, len(config_data), chunk_size):
            chunk = config_data[offset:offset + chunk_size]
            word_address = offset // 2
            addr_0 = word_address & 0x0F
            addr_1 = (word_address >> 4) & 0xFF

            if not self._write_reg(BMI270_INIT_ADDR_0_REG, addr_0, slave_bus=slave_bus):
                return False
            if not self._write_reg(BMI270_INIT_ADDR_1_REG, addr_1, slave_bus=slave_bus):
                return False

            try:
                slave_bus.write_i2c_block_data(self.chip_addr, BMI270_INIT_DATA_REG, chunk)
            except OSError as e:
                self.logger.warning(f"Failed to write config chunk at offset {offset}: {e}")
                return False

        return True

    def _initialize_device(self, slave_bus) -> bool:

        if not self._disable_advanced_power_save(slave_bus):
            return False
        if not self._write_reg(BMI270_INIT_CTRL_REG, 0x00, slave_bus=slave_bus):
            return False
        if not self._write_config_file(BMI270_CONFIG_FILE, slave_bus):
            return False
        if not self._write_reg(BMI270_INIT_CTRL_REG, 0x01, slave_bus=slave_bus):
            return False
        time.sleep(BMI270_CONFIG_WRITE_TIME_S)

        status_reg = self._read_reg(BMI270_INTERNAL_STATUS_REG, slave_bus=slave_bus)
        if (status_reg & 0x0F) != 0x01:
            self.logger.error(f"BMI270 initialization failed: INTERNAL_STATUS=0x{status_reg:02X}")
            return False
        return True

    def _perform_soft_reset(self, slave_bus):
        ok = self._write_reg(reg_addr=BMI270_CMD_REG, reg_data=0xB6, slave_bus=slave_bus)
        if ok:
            time.sleep(BMI270_SOFTRESET_WAIT_S)
        return ok


    def _perform_acc_self_test(self, slave_bus) -> bool:
        pwr_ctrl_data = self._read_reg(reg_addr=BMI270_PWR_CTRL_REG, slave_bus=slave_bus)
        pwr_ctrl_data |= (1 << 2)
        pwr_ok = self._write_reg(reg_addr=BMI270_PWR_CTRL_REG, reg_data=pwr_ctrl_data, slave_bus=slave_bus)

        if not pwr_ok:
            self.logger.error("Failed to enable accelerometer before self-test")
            return False

        if not self._write_reg(BMI270_ACC_RANGE_REG, 0x03, slave_bus=slave_bus):
            self.logger.error("Failed to set accelerometer range for self-test")
            return False

        acc_self_test_data = self._read_reg(reg_addr=BMI270_ACC_SELF_TEST_REG, slave_bus=slave_bus)
        acc_self_test_data |= (1 << 3)
        if not self._write_reg(BMI270_ACC_SELF_TEST_REG, acc_self_test_data, slave_bus=slave_bus):
            self.logger.error("Failed to set self-test amplitude")
            return False

        if not self._write_reg(BMI270_ACC_CONF_REG, 0xAC, slave_bus=slave_bus):
            self.logger.error("Failed to set accelerometer config for self-test")
            return False

        time.sleep(BMI270_SELF_TEST_PRE_WAIT_S)

        readings = {}

        for sign_name, sign_bit in (("positive", 1), ("negative", 0)):
            current = self._read_reg(reg_addr=BMI270_ACC_SELF_TEST_REG, slave_bus=slave_bus)
            current |= BMI270_ACC_SELF_TEST_EN_BIT
            if sign_bit:
                current |= BMI270_ACC_SELF_TEST_SIGN_BIT
            else:
                current &= ~BMI270_ACC_SELF_TEST_SIGN_BIT

            if not self._write_reg(BMI270_ACC_SELF_TEST_REG, current, slave_bus=slave_bus):
                self.logger.error(f"Failed to trigger {sign_name} self-test excitation")
                return False

            time.sleep(BMI270_SELF_TEST_SIGN_WAIT_S)

            data = self._read_regs(BMI270_ACC_X_LSB_REG, 6, slave_bus=slave_bus)
            x = self._to_signed((data[1] << 8) | data[0], 16)
            y = self._to_signed((data[3] << 8) | data[2], 16)
            z = self._to_signed((data[5] << 8) | data[4], 16)
            readings[sign_name] = (x, y, z)

        disable_data = self._read_reg(BMI270_ACC_SELF_TEST_REG, slave_bus=slave_bus)
        disable_data &= ~BMI270_ACC_SELF_TEST_EN_BIT
        self._write_reg(BMI270_ACC_SELF_TEST_REG, disable_data, slave_bus=slave_bus)

        diff_x = readings["positive"][0] - readings["negative"][0]
        diff_y = readings["positive"][1] - readings["negative"][1]
        diff_z = readings["positive"][2] - readings["negative"][2]

        lsb_per_g = (2 ** 16) / (2 * 16)
        diff_x_mg = (diff_x / lsb_per_g) * 1000
        diff_y_mg = (diff_y / lsb_per_g) * 1000
        diff_z_mg = (diff_z / lsb_per_g) * 1000

        success = (
            diff_x_mg > BMI2_ST_ACC_X_SIG_MIN_DIFF
            and diff_y_mg < BMI2_ST_ACC_Y_SIG_MIN_DIFF
            and diff_z_mg > BMI2_ST_ACC_Z_SIG_MIN_DIFF
        )

        self._perform_soft_reset(slave_bus)

        if success:
            self.logger.info("BMI270 accelerometer self-test successful")
        else:
            self.logger.warning(
                f"BMI270 accelerometer self-test failed: diff_mg=({diff_x_mg:.1f}, {diff_y_mg:.1f}, {diff_z_mg:.1f})"
            )

        return success

    def _perform_gyr_self_test(self, slave_bus) -> bool:
        if not self._perform_soft_reset(slave_bus):
            self.logger.error("Failed to reset device before gyroscope self-test")
            return False

        if not self._initialize_device(slave_bus):
            self.logger.error("Re-initialization before gyroscope self-test failed")
            return False

        pwr_ctrl_data = self._read_reg(BMI270_PWR_CTRL_REG, slave_bus=slave_bus)
        pwr_ctrl_data |= (1 << 2)   # acc_en, richiesto anche per il self-test del giroscopio
        if not self._write_reg(BMI270_PWR_CTRL_REG, pwr_ctrl_data, slave_bus=slave_bus):
            self.logger.error("Failed to enable accelerometer before gyroscope self-test")
            return False

        # Il dispositivo deve essere fermo durante l'esecuzione, altrimenti il test
        # si aborte automaticamente (g_trig_status=abort_err).

        if not self._write_reg(BMI270_CMD_REG, BMI270_G_TRIGGER_CMD, slave_bus=slave_bus):
            self.logger.error("Failed to send G_TRIGGER command")
            return False

        start = time.monotonic()
        while True:
            axes_reg = self._read_reg(BMI270_GYR_SELF_TEST_AXES_REG, slave_bus=slave_bus)
            if axes_reg & 0x01:   # gyr_st_axes_done
                break
            if time.monotonic() - start > BMI270_GYR_SELF_TEST_TIMEOUT_S:
                self.logger.error("Gyroscope self-test timeout waiting for completion")
                return False
            time.sleep(0.01)

        x_ok = bool(axes_reg & 0x02)
        y_ok = bool(axes_reg & 0x04)
        z_ok = bool(axes_reg & 0x08)

        success = x_ok and y_ok and z_ok

        if success:
            self.logger.info("BMI270 gyroscope self-test successful")
        else:
            failed_axes = [ax for ax, ok in (("X", x_ok), ("Y", y_ok), ("Z", z_ok)) if not ok]
            self.logger.warning(f"BMI270 gyroscope self-test failed on axis: {failed_axes}")

        return success


    def _enable_factory_offset_compensation(self, slave_bus=None) -> bool:
        nv_conf = self._read_reg(BMI270_NV_CONF_REG, slave_bus=slave_bus)
        if not self._write_reg(BMI270_NV_CONF_REG, nv_conf | (1 << 3), slave_bus=slave_bus):
            return False

        offset_6 = self._read_reg(BMI270_OFFSET_6_REG, slave_bus=slave_bus)
        if not self._write_reg(BMI270_OFFSET_6_REG, offset_6 | (1 << 6), slave_bus=slave_bus):
            return False

        return True


    def configure_normal_operation(self, acc_range=0x00, gyr_range=0x02, slave_bus=None) -> bool:
        """
        Configura ODR~50Hz e low-power mode (filter_perf=0) per entrambi i
        sensori, attiva la compensazione di fabbrica e il sensore di
        temperatura, e attende il primo campione realmente valido.
        """
        self._acc_range = acc_range
        self._gyr_range = gyr_range

        pwr_ctrl = self._read_reg(BMI270_PWR_CTRL_REG, slave_bus=slave_bus)
        pwr_ctrl |= (1 << 3) | (1 << 2) | (1 << 1)   # temp_en, acc_en, gyr_en
        if not self._write_reg(BMI270_PWR_CTRL_REG, pwr_ctrl, slave_bus=slave_bus):
            return False

        # ACC_CONF: filter_perf=0 (low power), bwp=osr4_avg1(0x00), odr=50Hz
        acc_conf = (0 << 7) | (0x00 << 4) | BMI270_ODR_50HZ
        if not self._write_reg(BMI270_ACC_CONF_REG, acc_conf, slave_bus=slave_bus):
            return False

        if not self._write_reg(BMI270_ACC_RANGE_REG, acc_range, slave_bus=slave_bus):
            return False

        # GYR_CONF: filter_perf=0, noise_perf=0 (low power), bwp=norm(0x02), odr=50Hz
        gyr_conf = (0 << 7) | (0 << 6) | (0x02 << 4) | BMI270_ODR_50HZ
        if not self._write_reg(BMI270_GYR_CONF_REG, gyr_conf, slave_bus=slave_bus):
            return False

        if not self._write_reg(BMI270_GYR_RANGE_REG, gyr_range, slave_bus=slave_bus):
            return False

        if not self._enable_factory_offset_compensation(slave_bus=slave_bus):
            return False

        # Attende che accelerometro e giroscopio abbiano prodotto almeno
        # un campione reale prima di considerarsi pronti -- senza questo,
        # una lettura immediatamente successiva potrebbe restituire il
        # valore di reset (0x0000) invece del primo dato vero.
        start = time.monotonic()
        while True:
            status = self._read_reg(BMI270_STATUS_REG, slave_bus=slave_bus)
            if (status & 0xC0) == 0xC0:   # drdy_acc (bit7) e drdy_gyr (bit6)
                break
            if time.monotonic() - start > BMI270_DATA_READY_TIMEOUT_S:
                self.logger.warning("Timeout waiting for first accel/gyro sample after configuration")
                break
            time.sleep(0.005)

        return True


    def read_raw_acc_gyr(self):
        if self.i2cbus is None:
            self.logger.error("Cannot read BMI270: no I2C bus available")
            return None

        data = self._read_regs(BMI270_ACC_X_LSB_REG, 12)

        acc_x = self._to_signed((data[1] << 8) | data[0], 16)
        acc_y = self._to_signed((data[3] << 8) | data[2], 16)
        acc_z = self._to_signed((data[5] << 8) | data[4], 16)

        gyr_x = self._to_signed((data[7] << 8) | data[6], 16)
        gyr_y = self._to_signed((data[9] << 8) | data[8], 16)
        gyr_z = self._to_signed((data[11] << 8) | data[10], 16)

        return (acc_x, acc_y, acc_z), (gyr_x, gyr_y, gyr_z)

    def read_die_temperature_c(self) -> float:
        if self.i2cbus is None:
            self.logger.error("Cannot read BMI270 die temperature: no I2C bus available")
            return None

        data = self._read_regs(BMI270_TEMPERATURE_0_REG, 2)
        raw = self._to_signed((data[1] << 8) | data[0], 16)
        if raw == -32768:   # 0x8000, valore esplicitamente marcato "invalid" dal datasheet
            return None
        return 23.0 + raw / 512.0

    def read_physical_acc_gyr(self):
        """
        Converte in g (accelerometro) e in gradi/s (giroscopio), usando il
        range attualmente configurato (impostato da configure_normal_operation).
        """
        raw = self.read_raw_acc_gyr()
        if raw is None:
            return None

        (ax, ay, az), (gx, gy, gz) = raw

        acc_range_g = ACC_RANGE_TO_G[self._acc_range]
        acc_lsb_per_g = (2 ** 16) / (2 * acc_range_g)

        gyr_range_dps = GYR_RANGE_TO_DPS[self._gyr_range]
        gyr_lsb_per_dps = (2 ** 16) / (2 * gyr_range_dps)

        return {
            "acc_x_g": ax / acc_lsb_per_g,
            "acc_y_g": ay / acc_lsb_per_g,
            "acc_z_g": az / acc_lsb_per_g,
            "gyr_x_dps": gx / gyr_lsb_per_dps,
            "gyr_y_dps": gy / gyr_lsb_per_dps,
            "gyr_z_dps": gz / gyr_lsb_per_dps,
        }

    def read_monitoring_snapshot(self) -> dict:

        physical = self.read_physical_acc_gyr()
        die_temp_c = self.read_die_temperature_c()

        if physical is None:
            return {
                "acc_x_g": None, "acc_y_g": None, "acc_z_g": None,
                "gyr_x_dps": None, "gyr_y_dps": None, "gyr_z_dps": None,
                "die_temperature_c": die_temp_c,
            }

        return {
            **physical,
            "die_temperature_c": die_temp_c,
        }

    def close(self) -> None:
        if self.i2cbus is None:
            return
        try:
            self.i2cbus.close()
        except Exception as e:
            self.logger.error(f"Error while closing BMI270 I2C bus: {e}")


    def _select_feature_page(self, page, slave_bus=None) -> bool:
        return self._write_reg(BMI270_FEAT_PAGE_REG, page, slave_bus=slave_bus)

    def _write_feature_word(self, page, offset, value_16bit, slave_bus=None) -> bool:

        i2cbus = slave_bus if slave_bus is not None else self.i2cbus
        if i2cbus is None:
            self.logger.error("Cannot write feature register: no I2C bus available")
            return False

        if not self._select_feature_page(page, slave_bus=slave_bus):
            self.logger.warning(f"Failed to select FEAT_PAGE={page}")
            return False

        data = [value_16bit & 0xFF, (value_16bit >> 8) & 0xFF]
        try:
            i2cbus.write_i2c_block_data(self.chip_addr, offset, data)
            return True
        except OSError as e:
            self.logger.warning(
                f"Failed to write feature word 0x{value_16bit:04X} at page {page} offset 0x{offset:02X}: {e}"
            )
            return False

    def _read_feature_word(self, page, offset, slave_bus=None):
        if not self._select_feature_page(page, slave_bus=slave_bus):
            self.logger.warning(f"Failed to select FEAT_PAGE={page}")
            return None

        data = self._read_regs(offset, 2, slave_bus=slave_bus)
        return data[0] | (data[1] << 8)

    def configure_any_motion(
        self,
        threshold_mg=BMI270_ANYMO_THRESHOLD_DEFAULT_MG,
        duration_s=BMI270_ANYMO_DURATION_DEFAULT_S,
        axes=("x", "y", "z"),
        enable=True,
        slave_bus=None,
    ) -> bool:
        """
        Configura il rilevamento any-motion (datasheet par. 4.8.2): il
        dispositivo segnala un evento quando la variazione di accelerazione
        (slope tra il campione corrente e un campione di riferimento) supera
        threshold_mg per duration_s consecutivi, sugli assi indicati in axes.
        L'uscita e' instradata di default su INT_STATUS_0.any_motion_out
        (bit 6), leggibile via get_any_motion_status().

        :param threshold_mg: soglia di slope in mg, range utile 0..~1000mg
        :param duration_s: durata minima in secondi (risoluzione 20ms, campioni a 50Hz)
        :param axes: assi da monitorare, sottoinsieme di ("x", "y", "z")
        :param enable: abilita/disabilita la feature
        """
        if self.i2cbus is None:
            self.logger.error("Cannot configure any-motion: no I2C bus available")
            return False

        threshold_raw = int(round(threshold_mg / BMI270_ANYMO_THRESHOLD_MG_PER_LSB))
        threshold_raw = max(0, min(threshold_raw, BMI270_ANYMO_THRESHOLD_MASK))

        duration_raw = int(round(duration_s / BMI270_ANYMO_DURATION_S_PER_LSB))
        duration_raw = max(0, min(duration_raw, BMI270_ANYMO_DURATION_MASK))

        select_bits = 0
        if "x" in axes:
            select_bits |= BMI270_ANYMO_SELECT_X_BIT
        if "y" in axes:
            select_bits |= BMI270_ANYMO_SELECT_Y_BIT
        if "z" in axes:
            select_bits |= BMI270_ANYMO_SELECT_Z_BIT

        anymo_1 = (duration_raw & BMI270_ANYMO_DURATION_MASK) | select_bits
        anymo_2 = (threshold_raw & BMI270_ANYMO_THRESHOLD_MASK) | (
            BMI270_ANYMO_OUT_CONF_BIT_6 << BMI270_ANYMO_OUT_CONF_SHIFT
        )
        if enable:
            anymo_2 |= BMI270_ANYMO_ENABLE_BIT

        ok = self._write_feature_word(
            BMI270_FEAT_PAGE_ANY_MOTION, BMI270_FEATURES_ANYMO_1_OFFSET, anymo_1, slave_bus=slave_bus
        )
        ok = ok and self._write_feature_word(
            BMI270_FEAT_PAGE_ANY_MOTION, BMI270_FEATURES_ANYMO_2_OFFSET, anymo_2, slave_bus=slave_bus
        )

        if ok:
            self.logger.debug(
                f"Any-motion configured: threshold={threshold_raw * BMI270_ANYMO_THRESHOLD_MG_PER_LSB:.1f}mg "
                f"(raw=0x{threshold_raw:03X}), duration={duration_raw * BMI270_ANYMO_DURATION_S_PER_LSB * 1000:.0f}ms "
                f"(raw={duration_raw}), axes={tuple(axes)}, enable={enable}"
            )
        else:
            self.logger.error("Failed to configure any-motion feature")

        return ok

    def disable_any_motion(self, slave_bus=None) -> bool:
        return self.configure_any_motion(enable=False, slave_bus=slave_bus)

    def get_any_motion_status(self) -> bool:
        """
        Legge INT_STATUS_0.any_motion_out (bit 6). ATTENZIONE: per datasheet
        (par. 5.2.28) INT_STATUS_0 viene azzerato alla lettura, quindi ogni
        chiamata "consuma" lo stato corrente: due letture ravvicinate senza
        un nuovo evento nel mezzo restituiranno False la seconda volta.
        """
        if self.i2cbus is None:
            self.logger.error("Cannot read any-motion status: no I2C bus available")
            return False

        status = self._read_reg(BMI270_INT_STATUS_0_REG)
        return bool(status & BMI270_ANY_MOTION_OUT_BIT)