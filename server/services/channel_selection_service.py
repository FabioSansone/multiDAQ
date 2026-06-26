from typing import List

from server.utils.logger import get_logger




class ChannelSelectionService:
    def __init__(self, control_manager, command_service, output_func=None) -> None:
        self.control_manager = control_manager
        self.command_service = command_service
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("channel_selection_service")
        self.logger.debug("Channels Selection Service initialized")

    @staticmethod
    def hv_to_user_channels(channels: List[int]) -> List[int]:
        return [ch - 1 for ch in channels]

    def get_test_rc_channels(self, client_id: bytes) -> List[int]:
        """
        Return RC channels to enable in test mode.

        In test mode no HV power/configuration command is allowed.
        If HVService is available in monitor-only mode, use HV OK channels only.
        If HVService is unavailable, fall back to all RC channels for bench tests.
        """

        client_name = client_id.decode(errors="ignore")

        sync_reply, reason = self.command_service.send_hv_command(
            client_id=client_id,
            command="set_hv_sync",
            payload={"channels": "all"},
            timeout_s=90.0,
        )

        if sync_reply is None:
            self.logger.warning(
                f"HV sync unavailable in test mode for client {client_name}: {reason}. "
                "Falling back to all RC channels."
            )
            self.poutput(
                f"Client {client_name}: HV sync unavailable in test mode; "
                "enabling all RC channels."
            )
            return list(range(7))

        sync_payload = sync_reply.payload or {}
        sync_result = sync_payload.get("result", {})
        sync_error = sync_payload.get("error")

        if sync_error:
            self.logger.warning(
                f"HV sync error in test mode for client {client_name}: {sync_error}. "
                "Falling back to all RC channels."
            )
            self.poutput(
                f"Client {client_name}: HV sync error in test mode; "
                "enabling all RC channels."
            )
            return list(range(7))

        ok_channels = sorted(set(sync_result.get("ok_channels", [])))
        bad_channels = sorted(set(sync_result.get("bad_channels", [])))

        if bad_channels:
            self.poutput(
                f"Client {client_name}: BAD HV/FEB channels excluded in test mode: "
                f"{self.hv_to_user_channels(bad_channels)}"
            )

        rc_channels = self.hv_to_user_channels(ok_channels)

        if not rc_channels:
            self.logger.warning(
                f"No OK HV/FEB channels found in test mode for client {client_name}. "
                "Falling back to all RC channels."
            )
            self.poutput(
                f"Client {client_name}: no OK HV/FEB channels found in test mode; "
                "enabling all RC channels."
            )
            return list(range(7))

        self.poutput(
            f"Client {client_name}: test mode RC channels from HV/FEB presence: "
            f"{rc_channels}"
        )

        return rc_channels

    def prepare_hv_channels_for_acquisition(self) -> dict[bytes, List[int]]:
        """
        Synchronize HV status and switch ON all OK+OFF channels.

        Returns:
            dict[client_id, list[int]]
            Channels are returned in external numbering: 0..6.
            These are the channels that can be passed to RC register 19.
        """

        client_ids = self.control_manager.server_state.list_connected_clients()

        if not client_ids:
            self.poutput("No connected clients.")
            return {}

        enabled_channels_by_client = {}

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            sync_reply, reason = self.command_service.send_hv_command(
                client_id=client_id,
                command="set_hv_sync",
                payload={"channels": "all"},
                timeout_s=90.0,
            )

            if sync_reply is None:
                self.logger.error(f"HV sync failed for client {client_name}: {reason}")
                self.poutput(f"Client {client_name}: HV sync failed ({reason})")
                continue

            sync_payload = sync_reply.payload or {}
            sync_result = sync_payload.get("result", {})
            sync_error = sync_payload.get("error")

            if sync_error:
                self.logger.error(f"HV sync error from client {client_name}: {sync_error}")
                self.poutput(f"Client {client_name}: HV sync error: {sync_error}")
                continue

            ok_channels = set(sync_result.get("ok_channels", []))
            on_channels = set(sync_result.get("on_channels", []))
            off_channels = set(sync_result.get("off_channels", []))
            bad_channels = set(sync_result.get("bad_channels", []))

            ok_on_channels = sorted(ok_channels & on_channels)
            ok_off_channels = sorted(ok_channels & off_channels)

            if bad_channels:
                self.poutput(
                    f"Client {client_name}: BAD HV channels excluded: "
                    f"{self.hv_to_user_channels(sorted(bad_channels))}"
                )

            if not ok_channels:
                self.poutput(f"Client {client_name}: no OK HV channels available.")
                continue

            if ok_off_channels:
                user_ok_off_channels = self.hv_to_user_channels(ok_off_channels)

                self.poutput(
                    f"Client {client_name}: switching ON HV channels "
                    f"{user_ok_off_channels} and waiting for UP state..."
                )

                on_reply, reason = self.command_service.send_hv_command(
                    client_id=client_id,
                    command="hv_on_and_wait",
                    payload={"channels": user_ok_off_channels},
                    timeout_s=300.0,
                )

                if on_reply is None:
                    self.logger.error(
                        f"HV on_and_wait failed for client {client_name}: {reason}"
                    )
                    self.poutput(
                        f"Client {client_name}: HV on_and_wait failed ({reason})"
                    )
                    continue

                on_payload = on_reply.payload or {}
                on_result = on_payload.get("result", {})
                on_error = on_payload.get("error")

                if on_error:
                    self.logger.error(
                        f"HV on_and_wait error from client {client_name}: {on_error}"
                    )
                    self.poutput(
                        f"Client {client_name}: HV on_and_wait error: {on_error}"
                    )

                successful_on = set(
                    on_result.get("successful_channels", [])
                    or on_result.get("on_channels", [])
                    or on_result.get("up_channels", [])
                )

                failed_on = set(on_result.get("failed_channels", []))
                bad_after_on = set(on_result.get("bad_channels", []))

                if successful_on:
                    self.poutput(
                        f"Client {client_name}: HV channels UP: "
                        f"{self.hv_to_user_channels(sorted(successful_on))}"
                    )

                if failed_on:
                    self.poutput(
                        f"Client {client_name}: HV channels failed to go UP: "
                        f"{self.hv_to_user_channels(sorted(failed_on))}"
                    )

                if bad_after_on:
                    self.poutput(
                        f"Client {client_name}: HV channels moved to BAD: "
                        f"{self.hv_to_user_channels(sorted(bad_after_on))}"
                    )

                final_hv_channels = sorted(set(ok_on_channels) | successful_on)

            else:
                final_hv_channels = ok_on_channels
                self.poutput(
                    f"Client {client_name}: all usable OK channels already ON."
                )

            final_user_channels = self.hv_to_user_channels(final_hv_channels)

            if not final_user_channels:
                self.poutput(
                    f"Client {client_name}: no HV channels available for acquisition."
                )
                continue

            enabled_channels_by_client[client_id] = final_user_channels

            self.poutput(
                f"Client {client_name}: HV channels ready for acquisition: "
                f"{final_user_channels}"
            )

        return enabled_channels_by_client
