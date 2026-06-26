from common.message_handler import Channel
from server.utils.logger import get_logger



class ClientCommandService:
    def __init__(self, control_manager, output_func=None) -> None:
        self.control_manager = control_manager
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("client_command_service")
        self.logger.debug("Client Command Service initialized")

    def send_hv_command(
        self,
        client_id: bytes,
        command: str,
        payload: dict,
        timeout_s: float = 90.0,
    ):
        hv_command = self.control_manager.message_handler.create_command(
            channel=Channel.HV,
            command=command,
            payload=payload,
            sender="server",
        )

        self.control_manager.queue_message(client_id, hv_command)

        return self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=hv_command.request_id,
            timeout_s=timeout_s,
        )

    def send_rc_command(
        self,
        client_id: bytes,
        command: str,
        payload: dict,
        timeout_s: float = 35.0,
    ):
        rc_command = self.control_manager.message_handler.create_command(
            channel=Channel.RC,
            command=command,
            payload=payload,
            sender="server",
        )

        self.control_manager.queue_message(client_id, rc_command)

        return self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=rc_command.request_id,
            timeout_s=timeout_s,
        )

    def read_rc_register(
        self,
        client_id: bytes,
        address: int,
        timeout_s: float = 35.0,
    ) -> int | None:
        client_name = client_id.decode(errors="ignore")

        reply, reason = self.send_rc_command(
            client_id=client_id,
            command="rc_read_register",
            payload={"address": address},
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"RC read register {address} failed for client {client_name}: {reason}"
            )
            self.poutput(
                f"Client {client_name}: no reply while reading RC register "
                f"{address} ({reason})"
            )
            return None

        payload = reply.payload or {}
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"RC read register {address} failed for client {client_name}: {error}"
            )
            self.poutput(f"Client {client_name}: failed to read RC register {address}")

            if error:
                self.poutput(f"Client {client_name}: error: {error}")

            return None

        return result.get("value")

    def write_rc_register(
        self,
        client_id: bytes,
        address: int,
        value: int,
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
            timeout_s=timeout_s,
        )

        if reply is None:
            self.logger.error(
                f"RC write register {address} failed for client {client_name}: {reason}"
            )
            self.poutput(
                f"Client {client_name}: no reply while writing RC register "
                f"{address} ({reason})"
            )
            return False

        payload = reply.payload or {}
        status = payload.get("status")
        error = payload.get("error")

        if status != "ok":
            self.logger.error(
                f"RC write register {address} failed for client {client_name}: {error}"
            )
            self.poutput(f"Client {client_name}: failed to write RC register {address}")

            if error:
                self.poutput(f"Client {client_name}: error: {error}")

            return False

        return True
