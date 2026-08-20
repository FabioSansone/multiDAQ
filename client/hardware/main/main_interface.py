import threading
from typing import List
import time

from client.hardware.main.sensors.BME280 import BME280
from client.hardware.main.sensors.BMM150 import BMM150
from client.hardware.main.sensors.BMI270 import BMI270
from client.hardware.main.sensors.TLA2024 import TLA2024
from client.utils.logger import get_logger

BME280_ADDR = 0x76
TLA2024_ADRR = 0x49
BMM150_ADDR = 0x10
BMI270_ADDR = 0x68



class MAIN:
    
    def __init__(self):
        self.logger = get_logger('main')
        
        self.bme = BME280(chip_addr=BME280_ADDR)
        self.bmm = BMM150(chip_addr=BMM150_ADDR)
        self.bmi = BMI270(chip_addr=BMI270_ADDR)
        self.tla = TLA2024(chip_addr=TLA2024_ADRR)
        
        
    def get_bme_data(self):
        bme_data = self.bme.readReg()
        if bme_data is None:
            self.logger.error("BME280 read failed: sensor not available")
            return None

        temperature, pressure, humidity = bme_data  

        return {
            "temperature_c": temperature,
            "pressure_hpa": pressure,
            "humidity_pct": humidity,
        }


    def get_bmm_data(self):
        bmm_data = self.bmm.read_force_mode()
        if bmm_data is None:
            self.logger.error("BMM150 read failed: sensor not available or force-mode timeout")
            return None

        bx, by, bz = bmm_data

        return {
            "mag_x_ut": bx,
            "mag_y_ut": by,
            "mag_z_ut": bz,
        }


    def get_tla_data(self):
        tla_data = self.tla.readAllPhysical()
        if tla_data is None:
            self.logger.error("TLA2024 read failed: sensor not available")
            return None

        return tla_data  


    def get_bmi_data(self):
        try:
            return self.bmi.read_monitoring_snapshot()
        except Exception as e:
            self.logger.error(f"BMI270 read failed: {e}")
            return None


    def get_all_sensors_data(self):
        return {
            "env": self.get_bme_data(),
            "power": self.get_tla_data(),
            "mag": self.get_bmm_data(),
            "motion": self.get_bmi_data(),
        }


    def close(self) -> None:
        for sensor_name, sensor in (
            ("bme", self.bme), ("bmm", self.bmm), ("bmi", self.bmi), ("tla", self.tla)
        ):
            try:
                sensor.close()
            except Exception as e:
                self.logger.error(f"Error while closing {sensor_name}: {e}")

