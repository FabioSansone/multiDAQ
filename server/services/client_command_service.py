from enum import Enum
from typing import Optional

from common.message_handler import Channel, ProtocolMessage
from server.utils.logger import get_logger


class CommandPlane(str, Enum):
    CONTROL = "control"
    ACQUISITION = "acquisition"


class ClientCommandService:
    def __init__(
        self,
        control_manager,
        acquisition_manager,
        output_func=None,
    ) -> None:
        self.control_manager = control_manager
        self.acquisition_manager = acquisition_manager

        self.poutput = output_func or (lambda message: None)

        self.logger = get_logger("client_command_service")
        self.logger.debug("ClientCommandService initialized")

    def _get_manager(self, plane: CommandPlane):
        if plane == CommandPlane.CONTROL:
            return self.control_manager

        if plane == CommandPlane.ACQUISITION:
            return self.acquisition_manager

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

    def _client_available_on_plane(
        self,
        client_id: bytes,
        plane: CommandPlane,
    ) -> bool:
        if plane == CommandPlane.CONTROL:
            available = (
                client_id
                in self.control_manager.server_state.list_connected_clients()
            )
        else:
            available = self.acquisition_manager.is_client_connected(client_id)

        if not available:
            client_name = client_id.decode(errors="ignore")
            self.logger.error(
                f"Client {client_name} is not available on "
                f"{plane.value} plane"
            )

        return available

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

        if not self._client_available_on_plane(
            client_id=client_id,
            plane=normalized_plane,
        ):
            return None, f"client unavailable on {normalized_plane.value} plane"

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

    def _create_command(
        self,
        *,
        plane: CommandPlane,
        channel: Channel,
        command: str,
        payload: dict,
    ) -> ProtocolMessage:
        manager = self._get_manager(plane)

        return manager.message_handler.create_command(
            channel=channel,
            command=command,
            payload=payload,
            sender="server",
        )

    def send_hv_command(
        self,
        client_id: bytes,
        command: str,
        payload: dict,
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 90.0,
    ):
        normalized_plane = self._normalize_plane(plane)

        if normalized_plane is None:
            return None, "invalid command plane"

        hv_command = self._create_command(
            plane=normalized_plane,
            channel=Channel.HV,
            command=command,
            payload=payload,
        )

        return self._send_command_and_wait_reply(
            client_id=client_id,
            message=hv_command,
            plane=normalized_plane,
            timeout_s=timeout_s,
        )

    def send_rc_command(
        self,
        client_id: bytes,
        command: str,
        payload: dict,
        plane: CommandPlane | str = CommandPlane.CONTROL,
        timeout_s: float = 35.0,
    ):
        normalized_plane = self._normalize_plane(plane)

        if normalized_plane is None:
            return None, "invalid command plane"

        rc_command = self._create_command(
            plane=normalized_plane,
            channel=Channel.RC,
            command=command,
            payload=payload,
        )

        return self._send_command_and_wait_reply(
            client_id=client_id,
            message=rc_command,
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
    
    
    def list_clients_on_plane(
        self,
        plane: CommandPlane | str,
    ) -> list[bytes]:
        normalized_plane = self._normalize_plane(plane)

        if normalized_plane is None:
            return []

        if normalized_plane == CommandPlane.CONTROL:
            return self.control_manager.server_state.list_connected_clients()

        return self.acquisition_manager.list_connected_clients()