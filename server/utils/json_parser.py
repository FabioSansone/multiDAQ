from pathlib import Path
import re
import json
from server.utils.logger import get_logger


class JsonParser:

    CONFIG_FILES_POSSIBLE_PATHS = [
        Path("multiDAQ/server/multipmt_config_files"),
        Path("/swgo/Test/multiPMT_analysis/config_files/calibration"),
        Path.home() / "multiPMT" / "multipmt_config_files",
        Path("swgo/multiPMT/multipmt_config_files")
    ]

    def __init__(self, multipmt_id:str, batch_id:str|int):
        
        self.logger = get_logger("json_parser")
        self.logger.debug("Json Parser initialized")

        self.multipmt_id = str(multipmt_id).lower()
        self.batch_id = self._normalize_batch_id(batch_id)

        self.config_file_path = None
        self.config_file = None

        self.config_files_folder = self._find_config_files_folder()
        self.config_file_path = self._find_config_file_path()

        if self.config_file_path is None or self.config_file is None:
            self.logger.error(
                f"No valid config file found for multipmt_id={self.multipmt_id}, "
                f"batch_id={self.batch_id}"
            )

    def _normalize_batch_id(self, batch_id: str | int) -> str:
        batch_id = str(batch_id).lower()

        if batch_id.startswith("batch_"):
            return batch_id

        return f"batch_{batch_id}"

    def _find_config_files_folder(self) -> Path | None:
        for path in self.CONFIG_FILES_POSSIBLE_PATHS:
            if path.exists() and path.is_dir():
                self.logger.info(f"Found config files folder at: {path}")
                return path

            self.logger.warning(f"Config files folder not found at: {path}")

        self.logger.error("Config files folder not found in possible paths")
        return None
    
    def _expected_filename(self) -> str:
        if self.multipmt_id and self.multipmt_id != "generic":
            return f"*{self.multipmt_id}*{self.batch_id}*.json"

        return f"*generic*calibration*{self.batch_id}*.json"
    
    def _find_config_file_path(self) -> Path | None:
        if self.config_files_folder is None:
            self.logger.error("Cannot search config file: config folder not found")
            return None

        expected_pattern = self._expected_filename()

        candidates = sorted(
            self.config_files_folder.rglob(expected_pattern)
        )

        if not candidates:
            self.logger.error(
                f"Config file not found. Expected pattern: {expected_pattern}"
            )
            return None

        for path in candidates:
            self.logger.info(f"Found candidate config file: {path}")

            self.config_file_path = path
            self.config_file = self._load()

            if self.config_file is None:
                self.logger.error(f"Cannot load candidate config file: {path}")
                self.config_file_path = None
                continue

            if self._validate_metadata():
                self.logger.info(f"Config file validated: {path}")
                return path

            self.logger.warning(f"Metadata validation failed for candidate: {path}")
            self.config_file_path = None
            self.config_file = None

        self.logger.error(
            f"No valid config file found for pattern: {expected_pattern}"
        )
        return None
    
    def _validate_metadata(self) -> bool:
        if self.config_file is None:
            self.logger.error("Cannot validate metadata: config file not loaded")
            return False

        for section in ("pedestal", "polarizer", "spe", "gain", "PE", "threshold"):
            metadata = self.config_file.get(section, {}).get("metadata", {})

            if metadata.get("batch_id") != self.batch_id:
                self.logger.error(
                    f"Batch mismatch in {section}: "
                    f"{metadata.get('batch_id')} != {self.batch_id}"
                )
                return False

            if (
                self.multipmt_id != "generic"
                and self.multipmt_id not in str(metadata.get("multipmt_id", "")).lower()
            ):
                self.logger.error(
                    f"multiPMT mismatch in {section}: "
                    f"{metadata.get('multipmt_id')} != {self.multipmt_id}"
                )
                return False

        return True
    
    def _load(self) -> dict | None:
        if self.config_file_path is None:
            self.logger.error(
                f"No config file path available for multipmt_id={self.multipmt_id}, "
                f"batch_id={self.batch_id}"
            )
            return None

        try:
            with self.config_file_path.open() as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load config file {self.config_file_path}: {e}")
            return None

    def load_pe_data(self) -> dict[int, float] | None:
        if self.config_file is None:
            self.logger.error("Cannot load PE data: config file not loaded")
            return None

        try:
            data = self.config_file["PE"]["data"]
        except KeyError:
            self.logger.error("Invalid config file structure: missing PE/data")
            return None

        pe_info = {}

        for ch, ch_data in data.items():
            try:
                pe_info[int(ch)] = ch_data["PE_ADC"]
            except KeyError:
                self.logger.error(f"Missing PE_ADC for channel {ch}")
                return None

        return pe_info

    def load_voltage_gain_data(self) -> dict[int, float]:
        if self.config_file is None:
            self.logger.error("Cannot load PE data: config file not loaded")
            return None
        
        try:
            data = self.config_file["gain"]["data"]
        except KeyError:
            self.logger.error("Invalid config file structure: missing gain/data")
            return None

        v_gain_info = {}

        for ch, ch_data in data.items():
            try:
                v_gain_info[int(ch)] = ch_data["voltage"]
            except KeyError:
                self.logger.error(f"Missing voltage for channel {ch}")
                return None

        return v_gain_info


    def load_threshold_parameters(self) -> dict[int, tuple[float, float]]:
        if self.config_file is None:
            self.logger.error("Cannot load PE data: config file not loaded")
            return None
        
        try:
            data = self.config_file["threshold"]["data"]
        except KeyError:
            self.logger.error("Invalid config file structure: missing threshold/data")
            return None

        thr_fit_parameters = {}

        for ch, ch_data in data.items():
            try:
                thr_fit_parameters[int(ch)] = (ch_data["m"], ch_data["q"])
            except KeyError:
                self.logger.error(f"Missing threshold for channel {ch}")
                return None

        return thr_fit_parameters


    def get_ch_configuration(self, pe_thr: int | float) -> dict[int, dict[str, float]] | None:
        if pe_thr <= 0:
            self.logger.error(f"Invalid PE threshold requested: {pe_thr}")
            return None

        thr_fit_parameters = self.load_threshold_parameters()
        pe_reference = self.load_pe_data()
        voltage_gain = self.load_voltage_gain_data()

        if thr_fit_parameters is None or pe_reference is None or voltage_gain is None:
            self.logger.error("Cannot build channel configuration: missing input data")
            return None

        ch_config = {}

        for ch, pe_adc in pe_reference.items():

            if ch not in thr_fit_parameters:
                self.logger.error(f"Missing threshold fit parameters for channel {ch}")
                return None

            if ch not in voltage_gain:
                self.logger.error(f"Missing voltage gain data for channel {ch}")
                return None

            m_ch, q_ch = thr_fit_parameters[ch]

            if m_ch == 0:
                self.logger.error(f"Invalid threshold fit for channel {ch}: m=0")
                return None

            adc_thr = pe_thr * pe_adc
            thr_value_mv = (adc_thr - q_ch) / m_ch
            voltage = voltage_gain[ch]

            ch_config[ch] = {
                "voltage": voltage,
                "threshold": thr_value_mv,
                "threshold_pe": pe_thr,
                "threshold_adc": adc_thr,
            }

        return ch_config









    




