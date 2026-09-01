import threading
from typing import List
import time

from client.hardware.main.sensors.BME280 import BME280
from client.hardware.main.sensors.BMM150 import BMM150
from client.hardware.main.sensors.BMI270 import BMI270
from client.hardware.main.sensors.TLA2024 import TLA2024
from client.hardware.main.sensors.XADC import XADC
from client.utils.logger import get_logger

BME280_ADDR = 0x76
TLA2024_ADRR = 0x49
BMM150_ADDR = 0x10
BMI270_ADDR = 0x68

MAIN_SENSOR_THRESHOLDS = {
    "env.temperature_c": {
        "min": -25.0,
        "max": 40.0,
    },

    "env.humidity_pct": {
        "min": 0.0,
        "max": 75.0,
    },

    "power.rail_ain0_v": {
        "min": 4.4,
        "max": 5.4,
    },

    "power.rail_ain2_v": {
        "min": 3.2,
        "max": 3.4,
    },

    "fpga.temperature_c": {
        "max": 80,
    },
}



class MAIN:
    
    def __init__(self, cached_i2c_bus: int | None = None):
        self.logger = get_logger('main')
        
        if cached_i2c_bus is None:
            candidate_buses = (0, 1, 2)
        else:
            candidate_buses = (cached_i2c_bus, *(bus for bus in (0, 1, 2) if bus != cached_i2c_bus))
        
        self.logger.info(f"Main Board I2C candidate buses: {candidate_buses}")
        
        self.bme = BME280(chip_addr=BME280_ADDR, candidate_buses=candidate_buses)
        self.bmm = BMM150(chip_addr=BMM150_ADDR, candidate_buses=candidate_buses)
        self.bmi = BMI270(chip_addr=BMI270_ADDR, candidate_buses=candidate_buses)
        self.tla = TLA2024(chip_addr=TLA2024_ADRR, candidate_buses=candidate_buses)
        self.xadc = XADC()
        
        detected_buses = {
            sensor.iic_bus
            for sensor in (
                self.bme,
                self.bmm,
                self.bmi,
                self.tla,
            )
            if sensor.iic_bus is not None
        }

        if len(detected_buses) == 1:
            self.i2c_bus = next(iter(detected_buses))
            self.logger.info(f"Main Board sensors detected on I2C bus {self.i2c_bus}")

        elif len(detected_buses) == 0:
            self.i2c_bus = None
            self.logger.error(
                "No Main Board I2C sensor detected"
            )

        else:
            self.i2c_bus = None
            self.logger.error(
                "Main Board sensors detected on inconsistent I2C buses: "
                f"{sorted(detected_buses)}"
            )
        
        if self.bmi.available:
            any_motion_ok = (
                self.bmi.configure_any_motion()
            )

            if not any_motion_ok:
                self.logger.warning(
                    "BMI270 any-motion monitoring "
                    "could not be enabled"
                )
                
        
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

        if not self.bmi.available:
            return None

        try:
            return self.bmi.read_monitoring_snapshot()

        except Exception as e:
            self.logger.error(
                f"BMI270 read failed: {e}"
            )
            return None

    def get_xadc_data(self):
        temperature = self.xadc.get_temperature()

        if temperature is None:
            self.logger.error("XADC FPGA temperature read failed")
            return None

        return {
            "temperature_c": temperature,
        }

    def get_all_sensors_data(self):
        return {
            "env": self.get_bme_data(),
            "power": self.get_tla_data(),
            "mag": self.get_bmm_data(),
            "motion": self.get_bmi_data(),
            "fpga": self.get_xadc_data(),
        }
        
    def check_sensor_thresholds(self):

        env_data = self.get_bme_data()
        power_data = self.get_tla_data()
        fpga_data = self.get_xadc_data()

        sensor_values = {
            "env.temperature_c": (
                env_data.get("temperature_c")
                if env_data is not None
                else None
            ),

            "env.humidity_pct": (
                env_data.get("humidity_pct")
                if env_data is not None
                else None
            ),

            "power.rail_ain0_v": (
                power_data.get("rail_ain0_v")
                if power_data is not None
                else None
            ),

            "power.rail_ain2_v": (
                power_data.get("rail_ain2_v")
                if power_data is not None
                else None
            ),

            "fpga.temperature_c": (
                fpga_data.get("temperature_c")
                if fpga_data is not None
                else None
            ),
        }

        out_of_range = []
        unavailable = []

        for sensor_name, limits in MAIN_SENSOR_THRESHOLDS.items():

            value = sensor_values.get(sensor_name)

            if value is None:
                unavailable.append(sensor_name)
                continue

            min_value = limits.get("min")
            max_value = limits.get("max")

            if min_value is not None and value < min_value:

                out_of_range.append({
                    "sensor": sensor_name,
                    "value": value,
                    "min": min_value,
                    "max": max_value,
                    "direction": "low",
                })

                continue

            if max_value is not None and value > max_value:

                out_of_range.append({
                    "sensor": sensor_name,
                    "value": value,
                    "min": min_value,
                    "max": max_value,
                    "direction": "high",
                })

        return {
            "values": sensor_values,
            "out_of_range": out_of_range,
            "unavailable": unavailable,
        }
        
    def check_sensor_events(self):

        events = []

        if not self.bmi.available:
            return {
                "events": events,
            }

        try:
            if self.bmi.get_any_motion_status():

                events.append({
                    "event": "motion_detected",
                    "sensor": "bmi270",
                })

        except Exception as e:
            self.logger.error(
                f"BMI270 any-motion check failed: {e}"
            )

        return {
            "events": events,
        }
        

    def close(self) -> None:
        for sensor_name, sensor in (
            ("bme", self.bme), ("bmm", self.bmm), ("bmi", self.bmi), ("tla", self.tla), ("xadc", self.xadc)
        ):
            try:
                sensor.close()
            except Exception as e:
                self.logger.error(f"Error while closing {sensor_name}: {e}")
    
    
    def get_sensor_status(self) -> dict:
        
        threshold_check  = self.check_sensor_thresholds()
        sensor_values = threshold_check.get("values", {}) or {}
        unavailable  = set(threshold_check.get("unavailable", []) or [])
        out_of_range = {
            item.get("sensor"): item
            for item in (
                threshold_check.get(
                    "out_of_range",
                    [],
                )
                or []
            )
            if item.get("sensor") is not None
        }
        
        sensors = {}
        
        for sensor_name, limits in MAIN_SENSOR_THRESHOLDS.items():
            value = sensor_values.get(sensor_name)
            alarm_info = out_of_range.get(sensor_name)
            
            sensors[sensor_name] = {
                "value": value,

                "available": (
                    sensor_name
                    not in unavailable
                ),

                "alarm": (
                    alarm_info is not None
                ),

                "min": limits.get(
                    "min"
                ),

                "max": limits.get(
                    "max"
                ),

                "direction": (
                    alarm_info.get(
                        "direction"
                    )
                    if alarm_info is not None
                    else None
                ),
            }
        
        
        devices = {
            "bme280": {
                "available": self.bme.available,
                "bus": self.bme.iic_bus,
            },

            "tla2024": {
                "available": self.tla.available,
                "bus": self.tla.iic_bus,
            },

            "bmm150": {
                "available": self.bmm.available,
                "bus": self.bmm.iic_bus,
            },

            "bmi270": {
                "available": self.bmi.available,
                "bus": self.bmi.iic_bus,
            },

            "xadc": {
                "available": self.xadc.available,
                "bus": None,
            },
        }   
        
        
        has_alarm = any(
            sensor["alarm"]
            for sensor in sensors.values()
        )

        has_unavailable_quantity = any(
            not sensor["available"]
            for sensor in sensors.values()
        )

        has_unavailable_device = any(
            not device["available"]
            for device in devices.values()
        )

        return {
            "sensors": sensors,
            "devices": devices,

            "summary": {
                "alarm": has_alarm,
                "unavailable_quantity": (
                    has_unavailable_quantity
                ),
                "unavailable_device": (
                    has_unavailable_device
                ),
            },
        }
        
        
            

            
