from enum import Enum
from typing import Optional

from common.message_handler import Channel, ProtocolMessage
from server.utils.channels import normalize_user_channels
from server.utils.logger import get_logger


class CommandPlane(str, Enum):
    CONTROL = "control"
    ACQUISITION = "acquisition"
    MONITORING = "monitoring"


class ClientCommandService:
    def __init__(
        self,
        control_manager,
        acquisition_manager,
        monitoring_manager,
        server_state,
    ) -> None:
        self.control_manager = control_manager
        self.acquisition_manager = acquisition_manager
        self.monitoring_manager = monitoring_manager
        
        self.server_state = server_state

        self.logger = get_logger("client_command_service")
        self.logger.debug("ClientCommandService initialized")

    def _get_manager(self, plane: CommandPlane):
        if plane == CommandPlane.CONTROL:
            return self.control_manager

        if plane == CommandPlane.ACQUISITION:
            return self.acquisition_manager

        if plane == CommandPlane.MONITORING:
            return self.monitoring_manager

        self.logger.error(f"Unsupported command plane: {plane}")
        raise ValueError(f"Unsupported command plane: {plane}")
        


    def _normalize_plane(
        self,
        plane: CommandPlane | str,
    ) -> Optional[CommandPlane]:
        try:
            return CommandPlane(plane)

        except ValueError:
            self.logger.error(
                f"Invalid command plane {plane!r}. "
                f"Expected one of: {[item.value for item in CommandPlane]}"
            )
            return None


    def _send_command_and_wait_reply(
        self,
        *,
        client_id: bytes,
        message: ProtocolMessage,
        plane: CommandPlane | str,
        timeout_s: float,
    ):
        normalized_plane = self._normalize_plane(plane)

        if normalized_plane is None:
            return None, "invalid command plane"

        if not self.server_state.is_client_on_plane(
            client_id=client_id,
            plane=normalized_plane.value,
        ):
            client_name = client_id.decode(errors="ignore")

            self.logger.error(
                f"Client {client_name} is not available on "
                f"{normalized_plane.value} plane"
            )

            return (
                None,
                f"client unavailable on {normalized_plane.value} plane",
            )

        manager = self._get_manager(normalized_plane)

        manager.queue_message(
            client_id=client_id,
            message=message,
        )

        return manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=message.request_id,
            timeout_s=timeout_s,
        )

    def _create_command(self, *, plane, channel, command, payload, priority: int | None = None):
        manager = self._get_manager(plane)
        return manager.message_handler.create_command(
            channel=channel, command=command, payload=payload, sender="server", priority=priority,
        )

    def send_hv_command(self, client_id, command, payload,
                        plane=CommandPlane.CONTROL, timeout_s=90.0, priority: int | None = None):
        normalized_plane = self._normalize_plane(plane)
        if normalized_plane is None:
            return None, "invalid command plane"

        if payload is not None and "channels" in payload:
            # The client converts user numbering (0..6) to HV numbering itself.
            try:
                payload = {**payload, "channels": normalize_user_channels(payload["channels"])}
            except (TypeError, ValueError) as exc:
                self.logger.error(f"HV command '{command}' not sent to {client_id!r}: {exc}")
                return None, f"invalid HV channels: {exc}"

        hv_command = self._create_command(
            plane=normalized_plane, channel=Channel.HV, command=command, payload=payload, priority=priority,
        )
        return self._send_command_and_wait_reply(
            client_id=client_id, message=hv_command, plane=normalized_plane, timeout_s=timeout_s,
        )

    def send_rc_command(self, client_id, command, payload,
                        plane=CommandPlane.CONTROL, timeout_s=35.0, priority: int | None = None):
        normalized_plane = self._normalize_plane(plane)
        if normalized_plane is None:
            return None, "invalid command plane"

        rc_command = self._create_command(
            plane=normalized_plane, channel=Channel.RC, command=command, payload=payload, priority=priority,
        )
        return self._send_command_and_wait_reply(
            client_id=client_id, message=rc_command, plane=normalized_plane, timeout_s=timeout_s,
        )
        
    def send_acq_mode_sync_command(self, client_id, payload,
                                   plane=CommandPlane.CONTROL, timeout_s=90.0, priority: int | None = None):
        """Ask the client to align its hardware to the acquisition mode (set_acq_mode_sync)."""
        normalized_plane = self._normalize_plane(plane)
        if normalized_plane is None:
            return None, "invalid command plane"

        mode_sync_command = self._create_command(
            plane=normalized_plane, channel=Channel.ACQUISITION, command="set_acq_mode_sync", payload=payload, priority=priority,
        )
        return self._send_command_and_wait_reply(
            client_id=client_id, message=mode_sync_command, plane=normalized_plane, timeout_s=timeout_s,
        )

    def send_main_command(
        self,
        client_id,
        command,
        payload,
        plane=CommandPlane.MONITORING,
        timeout_s=35.0,
        priority: int | None = None,
    ):
        normalized_plane = self._normalize_plane(plane)

        if normalized_plane is None:
            return None, "invalid command plane"

        main_command = self._create_command(
            plane=normalized_plane,
            channel=Channel.MAIN,
            command=command,
            payload=payload,
            priority=priority,
        )

        return self._send_command_and_wait_reply(
            client_id=client_id,
            message=main_command,
            plane=normalized_plane,
            timeout_s=timeout_s,
        )

    def read_rc_register(
        self,
        client_id: bytes,
        address: int,
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 35.0,
    ) -> int | None:
        client_name = client_id.decode(errors="ignore")

        reply, reason = self.send_rc_command(
            client_id=client_id,
            command="rc_read_register",
            payload={"address": address},
            plane=plane,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"RC read register {address} failed for "
                f"client {client_name}: {reason}"
            )
            return None

        payload = reply.payload or {}
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"RC read register {address} failed for "
                f"client {client_name}: {error}"
            )

            return None

        return result.get("value")

    def write_rc_register(
        self,
        client_id: bytes,
        address: int,
        value: int,
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 35.0,
    ) -> bool:
        client_name = client_id.decode(errors="ignore")

        reply, reason = self.send_rc_command(
            client_id=client_id,
            command="rc_write_register",
            payload={
                "address": address,
                "value": value,
            },
            plane=plane,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"RC write register {address} failed for "
                f"client {client_name}: {reason}"
            )
            return False

        payload = reply.payload or {}
        status = payload.get("status")
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"RC write register {address} failed for "
                f"client {client_name}: {error}"
            )

            return False

        return True


    def set_rc_acquisition_registers(self, client_id: bytes, rc_acq_dict: dict, plane: CommandPlane | str = CommandPlane.CONTROL, priority: int | None = None, timeout_s: float = 60.0,) -> bool:

        client_name = client_id.decode(errors="ignore")
    
        reply, reason = self.send_rc_command(
            client_id=client_id,
            command="set_rc_acq",
            payload={
                "rc_acq_dict": rc_acq_dict,
            },
            plane=plane,
            priority=priority,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"RC set acquisition registers {rc_acq_dict} failed for "
                f"client {client_name}: {reason}"
            )
            return False

        payload = reply.payload or {}
        status = payload.get("status")
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"RC set acquisition registers {rc_acq_dict} failed for "
                f"client {client_name}: {error}"
            )

            return False

        return True
    
    

    
    
    def get_pmt_serial_map_clients(self, client_id: bytes, requested_channels: str | int | list[int] = "all",
                                  plane: CommandPlane | str = CommandPlane.CONTROL,
                                  timeout_s: float = 35.0,):
        
        user_serial_map = {}
        client_name = client_id.decode(errors="ignore")
        
        reply, reason = self.send_hv_command(
            client_id=client_id,
            command="get_serial_map",
            payload={"channels": requested_channels},
            plane=plane,
            timeout_s=timeout_s,
        )
        
        if reply is None:
            self.logger.error(f"HV get pmt serial map failed for client {client_name}: {reason}")
            return {}

        payload = reply.payload or {}
        result = payload.get("result", {})
        error = payload.get("error")

        if error:
            self.logger.error(f"HV get pmt serial map error from client {client_name}: {error}")
            return {}
        
        
        serial_map = result.get("serial_map", {})
        
        if not serial_map:
            self.logger.warning(f"HV get pmt serial map for client {client_name}: no channels returned")
            return {}
        
        for ch_key in sorted(serial_map.keys(), key=lambda x: int(x)):
            hv_channel = int(ch_key)          
            user_channel = hv_channel - 1     
            user_serial_map[user_channel] = serial_map[ch_key]
        
        return user_serial_map

    def get_hv_voltage_configuration(
        self,
        client_id: bytes,
        requested_channels: str | int | list[int] = "all",
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 35.0,
    ) -> dict[int, int] | None:

        client_name = client_id.decode(errors="ignore")

        reply, reason = self.send_hv_command(
            client_id=client_id,
            command="get_voltage_channels",
            payload={
                "channels": requested_channels,
            },
            plane=plane,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"HV voltage configuration read failed for "
                f"client {client_name}: {reason}"
            )
            return None

        payload = reply.payload or {}
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"HV voltage configuration read failed for "
                f"client {client_name}: {error}"
            )

            # Some channels failed: the ones that could be read are still valid.
            if not result.get("voltage_configuration"):
                return None

        configuration = result.get(
            "voltage_configuration",
            {},
        )

        return {
            int(channel) - 1: value
            for channel, value in configuration.items()
        }


    def get_hv_threshold_configuration(
        self,
        client_id: bytes,
        requested_channels: str | int | list[int] = "all",
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 35.0,
    ) -> dict[int, int] | None:

        client_name = client_id.decode(errors="ignore")

        reply, reason = self.send_hv_command(
            client_id=client_id,
            command="get_threshold_channels",
            payload={
                "channels": requested_channels,
            },
            plane=plane,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"HV threshold configuration read failed for "
                f"client {client_name}: {reason}"
            )
            return None

        payload = reply.payload or {}
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"HV threshold configuration read failed for "
                f"client {client_name}: {error}"
            )

            # Some channels failed: the ones that could be read are still valid.
            if not result.get("threshold_configuration"):
                return None

        configuration = result.get(
            "threshold_configuration",
            {},
        )

        return {
            int(channel) - 1: value
            for channel, value in configuration.items()
        }


    def get_hv_volt_thr_configuration(
        self,
        client_id: bytes,
        requested_channels: str | int | list[int] = "all",
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 40.0,
    ) -> dict[int, dict[str, int]] | None:

        client_name = client_id.decode(errors="ignore")

        reply, reason = self.send_hv_command(
            client_id=client_id,
            command="get_volt_thr_channels",
            payload={
                "channels": requested_channels,
            },
            plane=plane,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"HV voltage/threshold configuration read failed for "
                f"client {client_name}: {reason}"
            )
            return None

        payload = reply.payload or {}
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"HV voltage/threshold configuration read failed for "
                f"client {client_name}: {error}"
            )

            # Some channels failed: the ones that could be read are still valid.
            if not result.get("volt_thr_configuration"):
                return None

        configuration = result.get(
            "volt_thr_configuration",
            {},
        )

        return {
            int(hv_channel) - 1: {
                "voltage": values.get("voltage"),
                "threshold": values.get("threshold"),
            }
            for hv_channel, values in configuration.items()
        }


    def get_hv_power_state(
        self,
        client_id: bytes,
        requested_channels: str | int | list[int] = "all",
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 300.0,
    ) -> dict[int, str] | None:

        client_name = client_id.decode(errors="ignore")

        reply, reason = self.send_hv_command(
            client_id=client_id,
            command="get_power_state_channels",
            payload={
                "channels": requested_channels,
            },
            plane=plane,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"HV power-state read failed for "
                f"client {client_name}: {reason}"
            )
            return None

        payload = reply.payload or {}
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"HV power-state read failed for "
                f"client {client_name}: {error}"
            )

            # Some channels failed: the ones that could be read are still valid.
            if not result.get("power_state"):
                return None

        raw_power_state = result.get(
            "power_state",
            {},
        )

        return {
            int(hv_channel) - 1: state
            for hv_channel, state in raw_power_state.items()
        }

    def send_monitoring_command(
        self,
        client_id,
        command,
        payload,
        timeout_s=10.0,
        priority: int | None = None,
    ):

        plane = CommandPlane.MONITORING

        monitoring_command = self._create_command(
            plane=plane,
            channel=Channel.MONITORING,
            command=command,
            payload=payload,
            priority=priority,
        )

        return self._send_command_and_wait_reply(
            client_id=client_id,
            message=monitoring_command,
            plane=plane,
            timeout_s=timeout_s,
        )


    @staticmethod
    def _int_keys(values: dict | None) -> dict:
        return {int(key): value for key, value in (values or {}).items()}

    def get_multipmt_hardware_configuration(self, client_id: bytes, requested_channels: str | int | list[int] = "all", rc_flag: bool = False, hv_flag: bool = False, power_flag: bool = False, serial_flag: bool = False, plane=CommandPlane.CONTROL, priority: int | None = None, timeout_s: float = 60.0) -> dict:
        client_name = client_id.decode(errors="ignore")

        configuration = {
            "success": False,
            "registers": {},
            "hv_parameters": {},
            "channel_state": {},
            "power_state": {},
            "serials": {},
            "errors": {},
        }

        normalized_plane = self._normalize_plane(plane)
        if normalized_plane is None:
            configuration["errors"]["command"] = "invalid command plane"
            return configuration

        try:
            requested_channels = normalize_user_channels(requested_channels)
        except (TypeError, ValueError) as exc:
            self.logger.error(f"Hardware configuration not requested to {client_name}: {exc}")
            configuration["errors"]["command"] = f"invalid channels: {exc}"
            return configuration

        configuration_command = self._create_command(
            plane=normalized_plane,
            channel=Channel.SYSTEM,
            command="get_multipmt_configuration",
            payload={
                "channels": requested_channels,
                "rc": rc_flag,
                "hv": hv_flag,
                "power": power_flag,
                "serial": serial_flag,
            },
            priority=priority,
        )

        reply, reason = self._send_command_and_wait_reply(
            client_id=client_id,
            message=configuration_command,
            plane=normalized_plane,
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(f"Hardware configuration read failed for client {client_name}: {reason}")
            configuration["errors"]["command"] = reason
            return configuration

        payload = reply.payload or {}
        result = payload.get("result") or {}

        # Even when a section failed, the client sends the ones it could read.
        try:
            if rc_flag:
                configuration["registers"] = self._int_keys(result.get("registers"))

            if hv_flag:
                configuration["hv_parameters"] = self._int_keys(result.get("hv_parameters"))

            if power_flag:
                configuration["channel_state"] = self._int_keys(result.get("channel_state"))
                configuration["power_state"] = self._int_keys(result.get("power_state"))

            if serial_flag:
                configuration["serials"] = self._int_keys(result.get("serials"))

        except (TypeError, ValueError) as exc:
            self.logger.error(f"Malformed hardware configuration from client {client_name}: {exc}")
            configuration["registers"] = {}
            configuration["hv_parameters"] = {}
            configuration["channel_state"] = {}
            configuration["power_state"] = {}
            configuration["serials"] = {}
            configuration["errors"]["response"] = f"malformed reply: {exc}"
            return configuration

        configuration["errors"] = dict(result.get("errors") or {})

        if payload.get("status") != "ok" and not configuration["errors"]:
            configuration["errors"]["command"] = payload.get("error") or "unknown error"

        configuration["success"] = payload.get("status") == "ok" and not configuration["errors"]

        if not configuration["success"]:
            self.logger.warning(f"Hardware configuration of client {client_name} incomplete: {configuration['errors']}")

        return configuration
