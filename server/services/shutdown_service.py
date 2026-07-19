from server.utils.logger import get_logger
from server.utils.channels import *
from server.services.client_command_service import CommandPlane

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
        client_ids = self.server_state.list_common_plane_clients()

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
    
    def power_off_hv_on_shutdown(
        self,
        plane: CommandPlane = CommandPlane.CONTROL,
    ) -> None:


        client_ids = self.control_manager.server_state.list_connected_clients()

        if not client_ids:
            self.logger.warning("No connected clients. HV shutdown power-off skipped.")
            return

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            sync_reply, reason = self.command_service.send_hv_command(
                client_id=client_id,
                command="set_hv_sync",
                payload={"channels": "all"},
                plane=plane,
                timeout_s=90.0,
            )

            if sync_reply is None:
                self.logger.error(
                    f"HV sync failed for client {client_name} during shutdown: {reason}"
                )
                continue

            sync_payload = sync_reply.payload or {}
            sync_result = sync_payload.get("result", {})
            sync_error = sync_payload.get("error")

            if sync_error:
                self.logger.error(
                    f"HV sync error for client {client_name} during shutdown: {sync_error}"
                )
                continue

            on_channels_hv = sorted(set(sync_result.get("on_channels", [])))

            if not on_channels_hv:
                self.logger.info(
                    f"Client {client_name}: no HV channels ON, nothing to power off."
                )
                continue


            on_channels_external = hv_to_user_channels(on_channels_hv)

            self.logger.info(
                f"Client {client_name}: powering OFF HV channels (HV numbering) "
                f"{on_channels_hv} before shutdown."
            )

            off_reply, reason = self.command_service.send_hv_command(
                client_id=client_id,
                command="hv_off_and_wait",
                payload={
                    "channels": on_channels_external,
                    "timeout_s": 120.0,
                    "poll_s": 2.0,
                },
                plane=plane,
                timeout_s=150.0,
            )

            if off_reply is None:
                self.logger.error(
                    f"HV off_and_wait failed for client {client_name} during "
                    f"shutdown: {reason}"
                )
                continue

            off_payload = off_reply.payload or {}
            off_result = off_payload.get("result", {})
            off_error = off_payload.get("error")

            failed = off_result.get("failed_channels", [])

            if off_error:
                self.logger.error(
                    f"HV off_and_wait error for client {client_name} "
                    f"during shutdown: {off_error}"
                )

            if failed:
                self.logger.warning(
                    f"Client {client_name}: some HV channels failed to power off "
                    f"cleanly (HV numbering): {failed}"
                )
            else:
                self.logger.info(
                    f"Client {client_name}: HV channels powered off successfully."
                )
        
        
        
