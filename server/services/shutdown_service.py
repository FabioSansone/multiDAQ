from server.utils.logger import get_logger


SHUTDOWN_ZERO_REGISTERS = [19, 15, 1, 0, 39, 16, 18]

logger = get_logger("shutdown_service")


class ShutdownService:
    def __init__(self, control_manager, command_service, output_func=None) -> None:
        self.control_manager = control_manager
        self.command_service = command_service
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("shutdown_service")
        self.logger.debug("Shutdown Service initialized")

    def zero_rc_registers_on_shutdown(self) -> None:
        client_ids = self.control_manager.server_state.list_connected_clients()

        if not client_ids:
            self.logger.warning("No connected clients. RC shutdown zero skipped.")
            return

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            for address in SHUTDOWN_ZERO_REGISTERS:
                ok = self.command_service.write_rc_register(
                    client_id=client_id,
                    address=address,
                    value=0,
                    timeout_s=10.0,
                )

                if not ok:
                    self.logger.error(
                        f"Shutdown zero failed for client {client_name}, "
                        f"register {address}"
                    )
                    continue

                self.logger.info(
                    f"Client {client_name}: register {address} reset to 0 on shutdown"
                )
