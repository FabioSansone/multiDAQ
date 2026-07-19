from typing import List

from server.utils.logger import get_logger
from server.utils.channels import *
from server.services.client_command_service import CommandPlane




class ChannelSelectionService:
    def __init__(self, command_service, output_func=None) -> None:
        self.command_service = command_service
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("channel_selection_service")
        self.logger.debug("Channels Selection Service initialized")

    
    @staticmethod
    def parse_user_channels(
        channels: str | int | list[int],
        n_channels: int = 7,
    ) -> list[int]:
        """
        Parse channels expressed in external/user numbering: 0..6.

        Accepted formats:
            "all"
            "0,1,4"
            3
            [0, 2, 5]
        """

        if channels == "all":
            return list(range(n_channels))

        if isinstance(channels, int):
            parsed_channels = [channels]

        elif isinstance(channels, str):
            try:
                parsed_channels = [
                    int(item.strip())
                    for item in channels.split(",")
                    if item.strip()
                ]
            except ValueError as exc:
                raise ValueError(
                    f"Invalid channel selection: {channels!r}"
                ) from exc

        elif isinstance(channels, list):
            try:
                parsed_channels = [int(channel) for channel in channels]
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid channel selection: {channels!r}"
                ) from exc

        else:
            raise TypeError(
                f"Unsupported channel selection type: {type(channels).__name__}"
            )

        invalid_channels = [
            channel
            for channel in parsed_channels
            if channel < 0 or channel >= n_channels
        ]

        if invalid_channels:
            raise ValueError(
                f"Channels outside valid range 0..{n_channels - 1}: "
                f"{invalid_channels}"
            )

        return sorted(set(parsed_channels))

    def get_test_rc_channels(
        self,
        client_id: bytes,
        requested_channels: str | int | list[int] = "all",
        plane: CommandPlane = CommandPlane.CONTROL
    ) -> List[int]:
        """
        Return RC channels to enable in test mode.

        The final channel selection is the intersection between:
            - channels requested by the user, in RC numbering 0..6;
            - HV/FEB channels detected as OK.

        If HV synchronization is unavailable, the requested channel selection
        is used as fallback.
        """

        client_name = client_id.decode(errors="ignore")

        try:
            requested_rc_channels = self.parse_user_channels(
                channels=requested_channels,
                n_channels=7,
            )
        except (TypeError, ValueError) as exc:
            self.logger.error(
                f"Invalid requested channels for client {client_name}: {exc}"
            )
            self.poutput(
                f"Client {client_name}: invalid channel selection: {exc}"
            )
            return []

        sync_reply, reason = self.command_service.send_hv_command(
            client_id=client_id,
            command="set_hv_sync",
            payload={"channels": "all"},
            plane=plane,
            timeout_s=90.0,
        )

        if sync_reply is None:
            if plane == CommandPlane.CONTROL:
                self.logger.warning(
                    f"HV sync unavailable in test mode for client {client_name}: "
                    f"{reason}. Using requested RC channels."
                )
                return requested_rc_channels

            self.logger.error(
                f"HV sync unavailable on acquisition plane for client "
                f"{client_name}: {reason}"
            )
            self.poutput(
                f"Client {client_name}: cannot determine safe channels "
                "for acquisition."
            )
            return []

        sync_payload = sync_reply.payload or {}
        sync_result = sync_payload.get("result", {})
        sync_error = sync_payload.get("error")

        if sync_error:
            if plane==CommandPlane.CONTROL:
                self.logger.warning(
                    f"HV sync error in test mode for client {client_name}: "
                    f"{sync_error}. Using requested RC channels."
                )
                self.poutput(
                    f"Client {client_name}: HV sync error; "
                    f"using requested RC channels: {requested_rc_channels}"
                )
                return requested_rc_channels
            
            self.logger.error(
                f"HV sync error on acquisition plane for client "
                f"{client_name}: {sync_error}"
            )
            self.poutput(
                f"Client {client_name}: cannot determine safe channels "
                "for acquisition."
            )
            return []

        ok_hv_channels = sorted(
            set(sync_result.get("ok_channels", []))
        )

        bad_hv_channels = sorted(
            set(sync_result.get("bad_channels", []))
        )

        available_rc_channels = hv_to_user_channels(ok_hv_channels)
        bad_rc_channels = hv_to_user_channels(bad_hv_channels)

        if bad_rc_channels:
            self.poutput(
                f"Client {client_name}: BAD HV/FEB channels excluded: "
                f"{bad_rc_channels}"
            )

        final_rc_channels = sorted(
            set(requested_rc_channels) & set(available_rc_channels)
        )

        excluded_requested_channels = sorted(
            set(requested_rc_channels) - set(final_rc_channels)
        )

        if excluded_requested_channels:
            self.poutput(
                f"Client {client_name}: requested channels unavailable and excluded: "
                f"{excluded_requested_channels}"
            )

        if not final_rc_channels:
            self.logger.warning(
                f"No requested OK HV/FEB channels available for client {client_name}. "
                f"Requested={requested_rc_channels}, available={available_rc_channels}"
            )
            self.poutput(
                f"Client {client_name}: no requested channels are currently available."
            )
            return []

        self.poutput(
            f"Client {client_name}: requested RC channels: "
            f"{requested_rc_channels}"
        )
        self.poutput(
            f"Client {client_name}: available RC channels: "
            f"{available_rc_channels}"
        )
        self.poutput(
            f"Client {client_name}: final RC channels enabled: "
            f"{final_rc_channels}"
        )

        return final_rc_channels

    def prepare_hv_channels_for_acquisition(self, plane: CommandPlane = CommandPlane.ACQUISITION,) -> dict[bytes, List[int]]:
        """
        Synchronize HV status and switch ON all OK+OFF channels.

        Returns:
            dict[client_id, list[int]]
            Channels are returned in external numbering: 0..6.
            These are the channels that can be passed to RC register 19.
        """

        client_ids = self.command_service.list_clients_on_plane(plane)

        if not client_ids:
            self.poutput(
                f"No clients available on {plane.value} plane."
            )
            return {}

        enabled_channels_by_client = {}

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
                    f"{hv_to_user_channels(sorted(bad_channels))}"
                )

            if not ok_channels:
                self.poutput(f"Client {client_name}: no OK HV channels available.")
                continue

            if ok_off_channels:
                user_ok_off_channels = hv_to_user_channels(ok_off_channels)

                self.poutput(
                    f"Client {client_name}: switching ON HV channels "
                    f"{user_ok_off_channels} and waiting for UP state..."
                )

                on_reply, reason = self.command_service.send_hv_command(
                    client_id=client_id,
                    command="hv_on_and_wait",
                    payload={"channels": user_ok_off_channels},
                    plane=plane,
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
                        f"{hv_to_user_channels(sorted(successful_on))}"
                    )

                if failed_on:
                    self.poutput(
                        f"Client {client_name}: HV channels failed to go UP: "
                        f"{hv_to_user_channels(sorted(failed_on))}"
                    )

                if bad_after_on:
                    self.poutput(
                        f"Client {client_name}: HV channels moved to BAD: "
                        f"{hv_to_user_channels(sorted(bad_after_on))}"
                    )

                final_hv_channels = sorted(set(ok_on_channels) | successful_on)

            else:
                final_hv_channels = ok_on_channels
                self.poutput(
                    f"Client {client_name}: all usable OK channels already ON."
                )

            final_user_channels = hv_to_user_channels(final_hv_channels)

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
