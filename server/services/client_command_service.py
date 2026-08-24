from enum import Enum
from typing import Optional

from common.message_handler import Channel, ProtocolMessage
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
        output_func=None,
    ) -> None:
        self.control_manager = control_manager
        self.acquisition_manager = acquisition_manager
        self.monitoring_manager = monitoring_manager
        
        self.server_state = server_state

        self.poutput = output_func or (lambda message: None)

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
            self.poutput(
                f"Client {client_name}: no reply while reading "
                f"RC register {address} ({reason})"
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
            self.poutput(
                f"Client {client_name}: failed to read "
                f"RC register {address}"
            )

            if error:
                self.poutput(f"Client {client_name}: error: {error}")

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
            self.poutput(
                f"Client {client_name}: no reply while writing "
                f"RC register {address} ({reason})"
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
            self.poutput(
                f"Client {client_name}: failed to write "
                f"RC register {address}"
            )

            if error:
                self.poutput(f"Client {client_name}: error: {error}")

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
            self.poutput(
                f"Client {client_name}: no reply while setting acquisition registers "
                f"RC register {rc_acq_dict} ({reason})"
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
            self.poutput(
                f"Client {client_name}: failed to set acquisition registers "
                f"RC register {rc_acq_dict}"
            )

            if error:
                self.poutput(f"Client {client_name}: error: {error}")

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
            self.poutput(f"Client {client_name}: HV get pmt serial map failed ({reason})")
            return {}

        payload = reply.payload or {}
        result = payload.get("result", {})
        error = payload.get("error")

        if error:
            self.logger.error(f"HV get pmt serial map error from client {client_name}: {error}")
            self.poutput(f"Client {client_name}: HV get pmt serial map error: {error}")
            return {}
        
        
        serial_map = result.get("serial_map", {})
        
        if not serial_map:
            self.poutput("  (no channels)")
            return {}
        
        for ch_key in sorted(serial_map.keys(), key=lambda x: int(x)):
            hv_channel = int(ch_key)          
            user_channel = hv_channel - 1     
            user_serial_map[user_channel] = serial_map[ch_key]
        
        return user_serial_map

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
