import threading
from typing import List
import time
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass

from server.services.client_command_service import CommandPlane
from server.utils.logger import get_logger
from server.core.server_state import ServerFSM, ServerFSMEvent
from server.utils.json_parser import JsonParser

ACQ_REGISTER_ADDRESSES = [0, 1, 10, 15, 16, 18, 19, 31, 39]

TRIGGER_REG15_MASK = (
    (1 << 1)   # trigger enable
    | (1 << 4) # external / auto
    | (1 << 7) # differential / single-ended
    | (1 << 8) # polarity
)

@dataclass
class TriggerConfiguration:

    mode: str = "self"

    input_type: str = "differential" #differential, single-ended
    polarity: str = "default" #default, inverted

    window_ns: int = 400
    delay_ns: int = 800

    auto_logic: str | None = None #None, majority, exact
    multiplicity: int | None = None #for majority auto logic
    auto_channels: list[int] | None = None #for exact auto logic

    save_external: bool = False
    save_auto: bool = False

    


class AcquisitionService:
    def __init__(
        self,
        server_state,
        data_receiver_service,
        command_service,
        mac_identity_registry,
        output_func=None,
    ) -> None:

        self.server_state = server_state
        self.command_service = command_service
        self.data_receiver_service = data_receiver_service
        self.mac_identity_registry = mac_identity_registry

        self.poutput = output_func or (lambda message: None)

        self.logger = get_logger("acquisition_service")
        self.logger.debug("Acquisition Service initialized")

        self._session_lock = threading.Lock()
        self._session_active = False
        self._session_complete_event = threading.Event()
        self._session_complete_event.set()
        self._last_finalize_success = True

        self._stop_requested = threading.Event()
    
    def _parse_channel_value_map(self, value: str, value_type: float) -> dict[int, float] | None:
        
        result = {}
        for item in value.split(","):
            item = item.strip()
            if not item:
                continue
            if ":" not in item:
                return None
            ch_str, value_str = item.split(":", 1)
            try:
                channel = int(ch_str.strip())
                result[channel] = value_type(value_str.strip())
            except ValueError:
                return None
        
        return result
    
    def _convert_pe_map_to_mv(self, client_id: bytes, pe_by_channel: dict[int, float]) -> dict[int, float] | None:
        identity = self.server_state.get_identity(client_id)
        
        if identity is None:
            self.logger.error(f"Cannot resolve PE calibration: no identity for client {client_id!r}")
            return None
        
        multipmt_id = identity.get("multipmt_id")
        batch_id = identity.get("batch_id")
        
        if not multipmt_id or not batch_id:
            self.logger.error(f"Cannot resolve PE calibration: incomplete identity for client {client_id!r}")
            return None
        
        parser = JsonParser(multipmt_id=multipmt_id, batch_id=batch_id)
        thr_fit_parameters = parser.load_threshold_parameters()
        pe_reference = parser.load_pe_data()
        
        if thr_fit_parameters is None or pe_reference is None:
            self.logger.error(f"Cannot resolve PE calibration data for client {client_id!r}")
            return None
        
        threshold_mv_by_channel = {}
        for ch, pe_value in pe_by_channel.items():
            if ch not in thr_fit_parameters or ch not in pe_reference:
                self.logger.error(f"Missing calibration data for channel {ch}, client {client_id!r}")
                return None

            m_ch, q_ch = thr_fit_parameters[ch]
            pe_adc = pe_reference[ch]
            
            if m_ch == 0:
                self.logger.error(f"Invalid threshold fit for channel {ch}: m=0")
                return None

            adc_thr = pe_value * pe_adc
            threshold_mv_by_channel[ch] = (adc_thr - q_ch) / m_ch
        
        return threshold_mv_by_channel
    
    
    def resolve_hv_overrides(self, client_id: bytes, args) -> tuple[dict[int, dict] | None, str | None]:
        voltage = getattr(args, "voltage", None)
        voltage_map_raw = getattr(args, "voltage_map", None)
        threshold_mv = getattr(args, "threshold_mv", None)
        threshold_pe = getattr(args, "threshold_pe", None)
        threshold_mv_map_raw = getattr(args, "threshold_mv_map", None)
        threshold_pe_map_raw = getattr(args, "threshold_pe_map", None)

        if not any([voltage, voltage_map_raw, threshold_mv, threshold_pe, threshold_mv_map_raw, threshold_pe_map_raw]):
            return None, None   # nessun override richiesto: comportamento invariato, usa i default già mandati

        client_name = client_id.decode(errors="ignore")

        threshold_by_channel: dict[int, float] = {}

        if threshold_mv_map_raw is not None:
            parsed = self._parse_channel_value_map(threshold_mv_map_raw, float)
            if parsed is None:
                return None, f"Invalid --threshold-mv-map format for client {client_name}"
            threshold_by_channel = parsed

        elif threshold_pe_map_raw is not None:
            parsed = self._parse_channel_value_map(threshold_pe_map_raw, float)
            if parsed is None:
                return None, f"Invalid --threshold-pe-map format for client {client_name}"
            threshold_by_channel = self._convert_pe_map_to_mv(client_id=client_id, pe_by_channel=parsed)
            if threshold_by_channel is None:
                return None, f"Cannot resolve PE calibration for client {client_name}"

        elif threshold_pe is not None:
            threshold_by_channel = self._convert_pe_map_to_mv(client_id=client_id, pe_by_channel={ch: threshold_pe for ch in range(7)})
            if threshold_by_channel is None:
                return None, f"Cannot resolve PE calibration for client {client_name}"

        elif threshold_mv is not None:
            threshold_by_channel = {ch: threshold_mv for ch in range(7)}

        voltage_by_channel: dict[int, float] = {}

        if voltage_map_raw is not None:
            parsed = self._parse_channel_value_map(voltage_map_raw, int)
            if parsed is None:
                return None, f"Invalid --voltage-map format for client {client_name}"
            voltage_by_channel = parsed

        elif voltage is not None:
            target_channels = set(threshold_by_channel.keys()) if threshold_by_channel else set(range(7))
            voltage_by_channel = {ch: voltage for ch in target_channels}

        all_channels = set(voltage_by_channel.keys()) | set(threshold_by_channel.keys())

        if not all_channels:
            return None, None

        current_params = self.server_state.get_client_hv_parameters(client_id)

        acq_info = {}
        for ch in all_channels:
            v = voltage_by_channel.get(ch)
            t = threshold_by_channel.get(ch)

            if v is None:
                v = current_params.get(ch, {}).get("voltage")
            if t is None:
                t = current_params.get(ch, {}).get("threshold")

            if v is None or t is None:
                return None, (
                    f"Channel {ch} for client {client_name} has no known "
                    f"{'voltage' if v is None else 'threshold'} to fall back on "
                    "(never configured for this client)"
                )

            acq_info[ch] = {"voltage": v, "threshold": t}

        return acq_info, None
    
    
    def record_hv_parameters_from_reply(self, client_id: bytes, command: str, payload: dict, reply_result: dict) -> None:
        successful = set(reply_result.get("successful_channels", []))
        if not successful:
            return

        if command == "set_common_voltage":
            voltage = payload.get("common_voltage")
            update = {ch - 1: {"voltage": voltage} for ch in successful}   

        elif command == "set_common_threshold":
            threshold = payload.get("common_threshold")
            update = {ch - 1: {"threshold": threshold} for ch in successful}

        elif command == "set_acquisition_configuration":
            acq_configuration = payload.get("acquisition_configuration", {})
            update = {}
            for ch in successful:
                external_ch = ch - 1
                ch_config = acq_configuration.get(external_ch) or acq_configuration.get(str(external_ch))
                if ch_config:
                    update[external_ch] = ch_config

        else:
            return

        self.server_state.set_client_hv_parameters(client_id, update)
        
    
    def apply_hv_override(self, client_id: bytes, acq_info: dict[int, dict]) -> bool:
        client_name = client_id.decode(errors="ignore")

        reply, reason = self.command_service.send_hv_command(
            client_id=client_id,
            command="set_acquisition_configuration",
            payload={
                "channels": [ch + 1 for ch in acq_info.keys()],   
                "acquisition_configuration": acq_info,             
            },
            plane=CommandPlane.ACQUISITION,
            timeout_s=300.0,
        )

        if reply is None:
            self.logger.error(f"HV override failed for client {client_name}: {reason}")
            return False

        payload = reply.payload or {}
        if payload.get("status") != "ok":
            self.logger.error(f"HV override error for client {client_name}: {payload.get('error')}")
            return False

        self.record_hv_parameters_from_reply(
            client_id=client_id, command="set_acquisition_configuration",
            payload={"acquisition_configuration": acq_info}, reply_result=payload.get("result", {}),
        )
        return True
            

    
    def build_multipmt_configuration(self, client_id: bytes, pe_thr: int | float) -> dict[int, dict[str, float]] | None:
        client_name = client_id.decode(errors="ignore")
        identity = self.server_state.get_identity(client_id)
                
        if identity is None:
            self.logger.error(f"Cannot build multipmt configuration: no identity for client {client_name}")
            return None
        
        multipmt_id = identity.get("multipmt_id")
        batch_id = identity.get("batch_id")
        
        if not multipmt_id or not batch_id:
            self.logger.error(f"Cannot build multipmt configuration: incomplete identity for client {client_name}")
            return None
        
        parser = JsonParser(multipmt_id=multipmt_id, batch_id=batch_id)
        
        if parser.config_file is None:
            self.logger.error(f"Cannot build multipmt configuration: no calibration file loaded for client {client_name}")
            return None
        
        json_serial_by_channel = parser.get_json_serial_channel_map()
        json_ch_configuration = parser.get_ch_configuration(pe_thr=pe_thr)
        
        if json_serial_by_channel is None or json_ch_configuration is None:
            self.logger.error(f"Cannot build multipmt configuration: incomplete calibration data for client {client_name}")
            return None
        
        analysis_channel_by_serial: dict[str, int] = {}
        for analysis_ch, serial in json_serial_by_channel.items():
            if not serial or serial == "0":
                continue
            if serial in analysis_channel_by_serial:
                self.logger.error(
                    f"Duplicate serial {serial!r} in calibration JSON: "
                    f"channels {analysis_channel_by_serial[serial]} and {analysis_ch}"
                )
                return None
            analysis_channel_by_serial[serial] = analysis_ch
            
        real_serial_map = self.command_service.get_pmt_serial_map_clients(
            client_id=client_id, requested_channels="all", plane=CommandPlane.ACQUISITION,
        ) or {}   
        
        if not real_serial_map:
            self.logger.error(f"Cannot build multipmt configuration: no real serial map for client {client_name}")
            return None
        
        acq_info = {}
        excluded_channels = []
        
        for real_ch, real_serial in real_serial_map.items():
            if not real_serial:
                excluded_channels.append(real_ch)
                continue
            
            analysis_ch = analysis_channel_by_serial.get(real_serial)
            
            if analysis_ch is None:
                self.logger.warning(
                    f"Client {client_name}: real channel {real_ch} (serial {real_serial}) "
                    "has no matching calibration entry. Channel excluded."
                )
                excluded_channels.append(real_ch)
                continue
            
            if analysis_ch not in json_ch_configuration:
                self.logger.warning(
                    f"Client {client_name}: analysis channel {analysis_ch} (real channel "
                    f"{real_ch}, serial {real_serial}) has no gain/threshold data. Channel excluded."
                )
                excluded_channels.append(real_ch)
                continue
            
            ch_config = json_ch_configuration[analysis_ch]
            acq_info[real_ch] = {"voltage": ch_config["voltage"], "threshold": ch_config["threshold"],}
        
        if excluded_channels:
            self.server_state.set_calibration_excluded_channels(client_id, excluded_channels)
            self.poutput(
                f"Client {client_name}: channels excluded from multiPMT configuration "
                f"(no matching calibration): {sorted(excluded_channels)}"
            )
        else:
            self.server_state.set_calibration_excluded_channels(client_id, [])
        
        if not acq_info:
            self.logger.error(f"Cannot build multipmt configuration: no channels matched for client {client_name}")
            return None
        
        return acq_info
    
    def recheck_calibration(self, client_id: bytes) -> bool:

        client_name = client_id.decode(errors="ignore")

        if self.server_state.get_mode() != "multipmt":
            self.logger.error("recheck_calibration is only meaningful in multipmt mode")
            return False

        acq_info = self.build_multipmt_configuration(client_id=client_id, pe_thr=1)

        if acq_info is None:
            self.logger.error(f"Calibration recheck failed for client {client_name}")
            return False

        self.server_state.set_client_hv_parameters(client_id, acq_info)
        self.logger.info(f"Calibration rechecked for client {client_name}: {sorted(acq_info.keys())} channels matched")
        return True
                

    
    def build_trigger_configuration(self, args,) -> TriggerConfiguration | None:

        trigger_mode = args.trigger_mode

        if (trigger_mode not in {"self", "external", "auto"}):
            self.logger.error(f"Unsupported trigger mode: {trigger_mode}")
            return None

        ##############
        #SELF TRIGGER#
        ##############

        if (trigger_mode == "self"):
            return TriggerConfiguration(
                mode="self",
                input_type="differential",
                polarity="default",
                window_ns=0,
                delay_ns=0,
                auto_logic=None,
                multiplicity=None,
                auto_channels=None,
                save_external=False,
                save_auto=False,
            )

        ########################
        #TRIGGERED ACQUISITIONS#
        ########################

        window_ns = args.window_ns
        delay_ns = args.delay_ns

        if window_ns <= 0:
            self.logger.error(f"Trigger window must be > 0 ns, got {window_ns}")
            return None

        if delay_ns < 0:
            self.logger.error(f"Trigger delay must be >= 0 ns, got {delay_ns}")
            return None

        if window_ns % 5 != 0:
            self.logger.error(f"Trigger window must be a multiple of 5 ns, got {window_ns}")
            return None

        if delay_ns % 5 != 0:
            self.logger.error(f"Trigger delay must be a multiple of 5 ns, got {delay_ns}")
            return None

        ##################
        #EXTERNAL TRIGGER#
        ##################

        if trigger_mode == "external":
            trigger_input = args.trigger_input
            polarity = args.polarity

            if trigger_input not in {"differential","single-ended"}:
                self.logger.error(f"Unsupported external trigger input: {trigger_input}")
                return None

            if polarity not in {"default","inverted"}:
                self.logger.error(f"Unsupported external trigger polarity: {polarity}")
                return None


            return TriggerConfiguration(
                mode="external",
                input_type=trigger_input,
                polarity=polarity,
                window_ns=window_ns,
                delay_ns=delay_ns,
                auto_logic=None,
                multiplicity=None,
                auto_channels=None,
                save_external=True,
                save_auto=args.save_auto,
            )

        ##############
        #AUTO TRIGGER#
        ##############

        auto_logic = args.auto_logic

        if (auto_logic not in {"majority", "exact"}):
            self.logger.error(f"Unsupported auto-trigger logic: {auto_logic}")
            return None

        if (auto_logic == "majority"):
            multiplicity = args.multiplicity

            if multiplicity is None:
                self.logger.error("Missing multiplicity for auto majority trigger")
                return None

            if multiplicity < 1 or multiplicity > 7:
                self.logger.error(f"Auto-trigger multiplicity must be between 1 and 7, got {multiplicity}")
                return None

            return TriggerConfiguration(
                mode="auto",
                input_type="differential",
                polarity="default",
                window_ns=window_ns,
                delay_ns=delay_ns,
                auto_logic="majority",
                multiplicity=multiplicity,
                auto_channels=None,
                save_external=args.save_external,
                save_auto=True,
            )

        raw_channels = args.channels

        if not isinstance(raw_channels, str):
            self.logger.error(f"Invalid exact auto-trigger channel selection: {raw_channels!r}")
            return None

        auto_channels = []
        for item in raw_channels.split(","):
            item = item.strip()

            if not item:
                continue

            try:
                channel = int(item)
            except ValueError:
                self.logger.error(f"Invalid auto-trigger channel value: {item!r}")
                return None

            if channel < 0 or channel >= 7:
                self.logger.error(f"Invalid auto-trigger channel {channel}. Valid channels are 0..6.")
                return None

            if channel not in auto_channels:
                auto_channels.append(channel)

        auto_channels.sort()

        if not auto_channels:
            self.logger.error("At least one channel is required for exact auto-trigger")
            return None

        return TriggerConfiguration(
            mode="auto",
            input_type="differential",
            polarity="default",
            window_ns=window_ns,
            delay_ns=delay_ns,
            auto_logic="exact",
            multiplicity=None,
            auto_channels=auto_channels,
            save_external=args.save_external,
            save_auto=True,
        )

    def _build_trigger_registers(self,trigger_config: TriggerConfiguration,effective_channels: list[int],) -> dict | None:

        if trigger_config is None:
            self.logger.error("Cannot build trigger registers: trigger configuration is None")
            return None

        if not effective_channels:
            self.logger.error("Cannot build trigger registers without effective channels")
            return None

        #
        # Normalize effective channels
        #
        normalized_channels = []

        for channel in effective_channels:

            if not isinstance(channel, int):
                self.logger.error(
                    f"Invalid RC channel type: {channel!r}"
                )
                return None

            if channel < 0 or channel >= 7:
                self.logger.error(
                    f"Invalid effective RC channel: {channel}. "
                    "Valid channels are 0..6."
                )
                return None

            if channel not in normalized_channels:
                normalized_channels.append(channel)

        normalized_channels.sort()

        #
        # Base channel mask
        #
        channel_mask = 0

        for channel in normalized_channels:
            channel_mask |= 1 << channel

        #
        # Register 15
        #
        reg15_value = 0

        if trigger_config.mode != "self":
            # bit 1 = trigger enable
            reg15_value |= 1 << 1

        if trigger_config.mode == "auto":
            # bit 4:
            # 0 -> external trigger
            # 1 -> auto-trigger
            reg15_value |= 1 << 4

        if (
            trigger_config.mode == "external"
            and trigger_config.input_type == "single-ended"
        ):
            # bit 7:
            # 0 -> differential
            # 1 -> single-ended
            reg15_value |= 1 << 7

        if (
            trigger_config.mode == "external"
            and trigger_config.polarity == "inverted"
        ):
            # bit 8:
            # 0 -> default polarity
            # 1 -> inverted polarity
            reg15_value |= 1 << 8

        #
        # Registers 16 / 18
        #
        if trigger_config.mode == "self":

            reg16_value = 0
            reg18_value = 0

        else:

            if trigger_config.window_ns <= 0:
                self.logger.error(
                    f"Invalid trigger window: {trigger_config.window_ns} ns"
                )
                return None

            if trigger_config.delay_ns < 0:
                self.logger.error(
                    f"Invalid trigger delay: {trigger_config.delay_ns} ns"
                )
                return None

            if trigger_config.window_ns % 5 != 0:
                self.logger.error(
                    f"Trigger window must be a multiple of 5 ns, "
                    f"got {trigger_config.window_ns}"
                )
                return None

            if trigger_config.delay_ns % 5 != 0:
                self.logger.error(
                    f"Trigger delay must be a multiple of 5 ns, "
                    f"got {trigger_config.delay_ns}"
                )
                return None

            reg16_value = trigger_config.window_ns // 5
            reg18_value = trigger_config.delay_ns // 5

        #
        # Register 31
        #
        reg31_value = 0

        if trigger_config.mode == "auto":

            #
            # MAJORITY
            #
            if trigger_config.auto_logic == "majority":

                multiplicity = trigger_config.multiplicity

                if multiplicity is None:
                    self.logger.error(
                        "Missing multiplicity for auto majority trigger"
                    )
                    return None

                if multiplicity < 1 or multiplicity > 7:
                    self.logger.error(
                        f"Invalid auto-trigger multiplicity: {multiplicity}"
                    )
                    return None

                if multiplicity > len(normalized_channels):
                    self.logger.error(
                        f"Auto-trigger multiplicity {multiplicity} cannot be "
                        f"satisfied with only {len(normalized_channels)} "
                        f"effective channels: {normalized_channels}"
                    )
                    return None

                for bit in range(multiplicity):
                    reg31_value |= 1 << bit

            #
            # EXACT CHANNEL COMBINATION
            #
            elif trigger_config.auto_logic == "exact":

                auto_channels = trigger_config.auto_channels or []

                if not auto_channels:
                    self.logger.error(
                        "Exact auto-trigger requested without channels"
                    )
                    return None

                missing_channels = []

                for channel in auto_channels:
                    if channel not in normalized_channels:
                        missing_channels.append(channel)

                if missing_channels:
                    self.logger.error(
                        f"Exact auto-trigger requires unavailable channels "
                        f"{missing_channels}. Effective channels are "
                        f"{normalized_channels}"
                    )
                    return None

                # bit 7 selects exact-channel logic
                reg31_value |= 1 << 7

                for channel in auto_channels:
                    reg31_value |= 1 << channel

            else:
                self.logger.error(
                    f"Invalid auto-trigger logic: "
                    f"{trigger_config.auto_logic}"
                )
                return None

        #
        # Register 19
        #
        reg19_value = channel_mask

        if trigger_config.save_external:
            reg19_value |= 1 << 7

        if trigger_config.save_auto:
            reg19_value |= 1 << 8

        #
        # Register 39
        #
        reg39_value = channel_mask

        if trigger_config.mode != "self":
            # During every triggered acquisition we keep both
            # external-trigger and auto-trigger rates enabled.
            reg39_value |= 1 << 7
            reg39_value |= 1 << 8

        register_configuration = {
            "reg15": {
                "mask": TRIGGER_REG15_MASK,
                "value": reg15_value,
            },
            "registers": {
                16: reg16_value,
                18: reg18_value,
                19: reg19_value,
                31: reg31_value,
                39: reg39_value,
            },
        }

        self.logger.debug(
            f"Trigger register configuration built: "
            f"mode={trigger_config.mode}, "
            f"effective_channels={normalized_channels}, "
            f"configuration={register_configuration}"
        )

        return register_configuration
        

    def configure_acquisition_client(self, client_id: bytes, trigger_config: TriggerConfiguration, effective_channels: list[int],) -> bool:

        client_name = client_id.decode(errors="ignore")

        register_configuration = self._build_trigger_registers(
            trigger_config=trigger_config,
            effective_channels=effective_channels,
        )

        if register_configuration is None:
            self.logger.error(
                f"Cannot configure acquisition for client {client_name}: "
                "failed to build RC register configuration"
            )
            return False

        success = self.command_service.set_rc_acquisition_registers(
            client_id=client_id,
            rc_acq_dict=register_configuration,
            plane=CommandPlane.ACQUISITION,
            priority=2,
        )

        if not success:
            self.logger.error(
                f"Failed to configure acquisition RC registers "
                f"for client {client_name}"
            )
            return False

        self.logger.info(
            f"Acquisition RC registers configured successfully "
            f"for client {client_name}: "
            f"channels={effective_channels}, "
            f"trigger_mode={trigger_config.mode}"
        )

        return True

    def configure_acquisition_end_client(self, client_id: bytes,) -> bool:
    
        client_name = client_id.decode(errors="ignore")

        register_configuration = {
            "reg15": {
                            "mask": TRIGGER_REG15_MASK,
                            "value": 0,
                        },

            "registers": {
                            16: 0,
                            18: 0,
                        },
        }

        success = self.command_service.set_rc_acquisition_registers(
            client_id=client_id,
            rc_acq_dict=register_configuration,
            plane=CommandPlane.ACQUISITION,
            priority=2,
        )

        if not success:
            self.logger.error(
                f"Failed to clear acquisition trigger configuration "
                f"for client {client_name}"
            )
            return False

        self.logger.info(
            f"Acquisition trigger configuration cleared "
            f"for client {client_name}"
        )

        return True

        

    def _resolve_numeric_id(self, client_id: bytes) -> str | None:
        identity = self.server_state.get_identity(client_id)
        if identity is None:
            self.logger.error(f"Cannot resolve numeric id: no identity for client {client_id!r}")
            return None

        mac_address = identity.get("mac_address")
        if mac_address is None:
            self.logger.error(f"Cannot resolve numeric id: no mac_address in identity for client {client_id!r}")
            return None

        numeric_id = self.mac_identity_registry.get_id_from_mac(mac_address)
        if numeric_id is None:
            self.logger.error(f"Cannot resolve numeric id for MAC {mac_address}")
            return None

        return str(numeric_id)


    def open_file_for_client(
        self,
        client_id: bytes,
        metadata: dict,
        acq_type: str,
        file_format: str = "csv", #"csv" or "bin"
        suffix: str = "",
        run_id=None,
        run_folder=None,
    ) -> dict | None:
        numeric_id = self._resolve_numeric_id(client_id)
        if numeric_id is None:
            return None

        client_identity = client_id.decode(errors="ignore")

        return self.data_receiver_service.open_file(
            client_id=numeric_id,
            client_identity=client_identity,
            metadata=metadata,
            file_format=file_format,
            acq_type=acq_type,
            suffix=suffix,
            run_id=run_id,
            run_folder=run_folder,
        )

    def close_file_for_client(self, client_id: bytes) -> bool:
        numeric_id = self._resolve_numeric_id(client_id)
        if numeric_id is None:
            return False

        return self.data_receiver_service.close_file(numeric_id)

    def open_acq_all_clients(
        self,
        client_ids: List[bytes],
        metadata_by_client: dict[bytes, dict],
        acq_type: str,
        file_format: str = "csv", #"csv" or "bin"
        suffix: str = "",
        run_id=None,
        run_folder: dict[bytes, Path] | None = None,
    ) -> dict[bytes, dict | None]:

        results = {}

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            client_run_folder = (
                run_folder.get(client_id)
                if run_folder is not None
                else None
            )

            metadata = metadata_by_client.get(client_id)

            if metadata is None:
                self.logger.error(
                    f"Missing run metadata for client {client_name}"
                )
                results[client_id] = None
                continue

            result = self.open_file_for_client(
                client_id=client_id,
                metadata=metadata,
                file_format=file_format,
                acq_type=acq_type,
                suffix=suffix,
                run_id=run_id,
                run_folder=client_run_folder,
            )

            results[client_id] = result

        return results

    def stop_acq_all_clients(self, client_ids: List[bytes]) -> bool:
        overall_success = True

        for client_id in client_ids:
            if not self.close_file_for_client(client_id=client_id):
                client_name = client_id.decode(errors="ignore")
                self.logger.warning(f"Failed to close file for client {client_name}")
                overall_success = False

        return overall_success


    def begin_session(self) -> None:
        with self._session_lock:
            self._session_active = True
        self._session_complete_event.clear()
        self._stop_requested.clear()

    def request_stop(self) -> None:
        self._stop_requested.set()

    def wait_for_session_end(self, timeout: float | None = None) -> bool:
        return self._session_complete_event.wait(timeout=timeout)

    def get_last_finalize_success(self) -> bool:
        return self._last_finalize_success

    def close_session(self, success: bool, reason: str) -> None:
        if self.server_state.get_server_state() == ServerFSM.FINALIZING:
            if success:
                self.server_state.process_event(
                    event=ServerFSMEvent.FINALIZATION_SUCCEEDED,
                    reason=reason,
                    source="acquisition_service.close_session",
                )
            else:
                self.server_state.process_event(
                    event=ServerFSMEvent.FINALIZATION_FAILED,
                    reason=f"{reason} (finalization error)",
                    source="acquisition_service.close_session",
                )

        with self._session_lock:
            self._session_active = False
        self._last_finalize_success = success
        self._session_complete_event.set()

    def run_acquisition_session(
        self,
        client_ids: List[bytes],
        acq_type: str,
        file_format: str = "csv", #"csv" or "bin"
        acq_type_param=None,
        suffix: str = "",
        run_id=None,
        run_folder_clients: dict[bytes, "Path"] | None = None,
        duration: float | None = None,
        reason: str = "acquisition session completed",
        manage_session: bool = True,
        reset_trigger_config: bool = True
    ) -> bool:

        if manage_session:
            self.begin_session()
        
        metadata_by_client = self.build_run_metadata(
            client_ids=client_ids,
            acq_mode=self.server_state.get_mode(),
            acq_type=acq_type,
            acq_type_param=acq_type_param,
        )

        open_results = self.open_acq_all_clients(
            client_ids=client_ids, acq_type=acq_type, metadata_by_client=metadata_by_client, file_format=file_format, suffix=suffix, run_id=run_id, run_folder=run_folder_clients
        )
        opened_client_ids = [cid for cid, result in open_results.items() if result is not None]

        if not opened_client_ids:
            self.poutput("No clients ready for acquisition (all OPEN failed).")
            if manage_session:
                self.close_session(success=False, reason="no clients opened")
            return False

        if duration is not None and duration > 0:
            stopped_early = self._stop_requested.wait(timeout=duration)
            if stopped_early:
                self.logger.info("Acquisition stopped early by request, before duration elapsed")
            else:
                self.logger.info(f"Acquisition duration ({duration}s) elapsed naturally")
                if manage_session:
                    self.server_state.process_event(
                        event=ServerFSMEvent.RECEIVER_COMPLETED,
                        reason="Acquisition duration elapsed",
                        source="acquisition_service.run_acquisition_session",
                    )
        else:
            self._stop_requested.wait()  

        success = self.run_hardware_stop_and_flush(client_ids=opened_client_ids, reason=reason, reset_trigger_config=reset_trigger_config)
        if manage_session:
            self.close_session(success=success, reason=reason)
        return success


    def run_hardware_stop_and_flush(self, client_ids: List[bytes], reason: str, reset_trigger_config: bool = True) -> bool:
        self.poutput(f"Finalizing: {reason}")
        self.logger.info(f"Finalizing: {reason}")


        if not client_ids:
            self.poutput("No active clients. Nothing to close.")
            return True
        
        overall_success = True

        disable_results = self.disable_rc_channels(client_ids=client_ids)

        if not disable_results:
            overall_success = False

        self.poutput("Pushing FIFO flush via register 15...")
        flush_results = self.flush_clients(client_ids=client_ids)
        if not flush_results:
            overall_success = False

        if reset_trigger_config:
            for client_id in client_ids:
                ok = self.configure_acquisition_end_client(client_id=client_id)

                if not ok:
                    overall_success = False
        else:
            self.poutput("Trigger configuration preserved (multi-step acquisition, not the final point).")

        success = self.stop_acq_all_clients(client_ids=client_ids)

        if not success:
            overall_success = False

        if overall_success:
            self.poutput("Finalization completed.")
            self.logger.info("Finalization completed")
        else:
            self.poutput("Finalization completed with errors (see log).")
            self.logger.error("Finalization completed with errors")

        return overall_success

    def check_acquisition_busy(self) -> bool:
        state = self.server_state.get_server_state()
        return state in (ServerFSM.ACQUIRING, ServerFSM.FINALIZING)

    def get_receiver_exit_code(self) -> int | None:
        return self.data_receiver_service.get_exit_code()


    def _read_client_acq_registers(self, client_id: bytes) -> dict[int, int | None]:
        reply, reason = self.command_service.send_rc_command(
            client_id=client_id,
            command="rc_read_acq_registers",
            payload={"rc_acq_registers": ACQ_REGISTER_ADDRESSES},
            plane=CommandPlane.ACQUISITION,
            timeout_s=30.0,
        )

        if reply is None:
            self.logger.error(f"Failed to read acquisition RC registers for {client_id!r}: {reason}")
            return {addr: None for addr in ACQ_REGISTER_ADDRESSES}

        result = reply.payload.get("result", {})
        raw_values = result.get("value", {})

        return {int(address): value for address, value in raw_values.items()}

    def build_run_metadata(
        self,
        client_ids: list[bytes],
        acq_mode: str,
        acq_type: str,
        acq_type_param=None,
    ) -> dict[bytes, dict]:

        start_timestamp = int(time.time())

        start_timestamp_utc = (
            datetime.now(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )

        metadata_by_client = {}

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            register_values = self._read_client_acq_registers(client_id)

            serial_map_client = (
                self.command_service.get_pmt_serial_map_clients(
                    client_id,
                    "all",
                    plane=CommandPlane.ACQUISITION
                )
                or {}
            )

            metadata = {
                "version": 1,
                "timestamp_raw": start_timestamp,
                "timestamp_utc": start_timestamp_utc,
                "acquisition_mode": acq_mode,
                "acquisition_type": acq_type,
                "acq_type_param": (
                    acq_type_param
                    if acq_type_param is not None
                    else ""
                ),
                "client_id": client_name,
            }

            for address in ACQ_REGISTER_ADDRESSES:
                value = register_values.get(address)

                metadata[f"status_reg{address}"] = (
                    value if value is not None else -1
                )

            for ch in range(7):
                metadata[f"serial_pmt{ch}"] = (
                    serial_map_client.get(ch, "")
                )

            metadata_by_client[client_id] = metadata

        return metadata_by_client


    def flush_client(self, client_id: bytes) -> bool:
        client_name = client_id.decode(errors="ignore")

        read_prev = self.command_service.read_rc_register(
            client_id=client_id, address=15, plane=CommandPlane.ACQUISITION
        )

        if read_prev is None:
            self.poutput(f"Client {client_name}: flush skipped, no valid read from register 15.")
            return False

        if not self.command_service.write_rc_register(
            client_id=client_id, address=15, value=read_prev + 32, plane=CommandPlane.ACQUISITION
        ):
            self.poutput(f"Client {client_name}: flush failed while writing register 15.")
            return False

        time.sleep(2.0)

        read_now = self.command_service.read_rc_register(
            client_id=client_id, address=15, plane=CommandPlane.ACQUISITION
        )

        if read_now is None:
            self.poutput(f"Client {client_name}: flush failed, missing final read.")
            return False

        if read_now - read_prev - 32 == 64:
            self.poutput(f"Client {client_name}: data flushing ended successfully.")
            self.command_service.write_rc_register(
                client_id=client_id, address=15, value=read_prev, plane=CommandPlane.ACQUISITION
            )
            return True

        self.poutput(f"Client {client_name}: flush error. Please check.")
        self.logger.error(f"Flush check failed for client {client_name}: prev={read_prev}, now={read_now}")
        return False

    def flush_clients(self, client_ids: List[bytes]) -> bool:
        time.sleep(10.0)
        overall_success = True
        for client_id in client_ids:
            if not self.flush_client(client_id=client_id):
                overall_success = False
        return overall_success


    def enable_rc_channels(self, client_id: bytes, channels: List[int]) -> bool:
        client_name = client_id.decode(errors="ignore")

        if not channels:
            self.poutput(f"Client {client_name}: no RC channels selected for acquisition.")
            self.logger.warning(f"Cannot enable RC channels for client {client_name}: empty channel list")
            return False

        register_address = 19
        register_value = 0

        for ch in channels:
            if ch < 0 or ch >= 7:
                self.poutput(f"Client {client_name}: invalid RC channel: {ch}")
                self.logger.error(f"Invalid RC channel requested for acquisition: {ch}")
                return False
            register_value |= 1 << ch

        ok = self.command_service.write_rc_register(
            client_id=client_id, address=register_address, value=register_value, plane=CommandPlane.ACQUISITION
        )

        if not ok:
            self.poutput(f"Client {client_name}: failed to enable RC channels.")
            return False

        self.poutput(
            f"Client {client_name}: RC register {register_address} written with "
            f"value {register_value} (enabled channels: {channels})"
        )
        return True

    def disable_rc_channels(self, client_ids: List[bytes] | None = None) -> bool:
        if client_ids is None:
            client_ids = self.command_service.list_clients_on_plane(CommandPlane.ACQUISITION)

        overall_success = True

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            ok = self.command_service.write_rc_register(
                client_id=client_id, address=19, value=0, plane=CommandPlane.ACQUISITION,
            )

            if ok:
                self.poutput(f"Client {client_name}: RC acquisition channels disabled.")
            else:
                overall_success = False
                self.poutput(f"Client {client_name}: failed to disable RC acquisition channels.")

        return overall_success

    def get_active_clients(self) -> list[bytes]:
        return self.server_state.get_operational_clients()

    def resolve_batch_id(self, client_ids: List[bytes]) -> str | None:

        if not client_ids:
            return None

        client_id = client_ids[0]
        identity = self.server_state.get_identity(client_id) or {}

        batch_id = identity.get("batch_id")
        if batch_id:
            self.poutput(f"Using batch_id from client identity: {batch_id}")
            return batch_id

        multipmt_id = identity.get("multipmt_id")
        if multipmt_id:
            self.poutput(f"No batch_id in client identity. Using multipmt_id as acquisition folder id: {multipmt_id}")
            return multipmt_id

        return None

    def get_connected_clients(self, plane: CommandPlane = CommandPlane.ACQUISITION) -> list[bytes]:
        return self.command_service.list_clients_on_plane(plane)
    
    
    def get_client_run_folder(self, client_ids: List[bytes], acq_type: str, run_id: str | int | None):
        client_run_folder: dict[bytes, "Path"] = {}
        for client_id in client_ids:
            client_identity = client_id.decode(errors="ignore")
            client_run_folder[client_id] = self.data_receiver_service.get_run_folder(
                acq_type=acq_type, client_identity=client_identity, run_id=run_id
            )
        return client_run_folder