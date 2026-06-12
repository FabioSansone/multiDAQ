import argparse
import cmd2
from typing import List
from server.utils.logger import get_logger
from common.message_handler import Channel


logger = get_logger("acq_commands")

#######################
# ACQUISITION HELPERS #
#######################

def _hv_to_user_channels(channels: List[int]) -> List[int]:
    return [ch - 1 for ch in channels]


def _send_hv_command(self, client_id: bytes, command: str, payload: dict, timeout_s: float):
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


def _enable_hv_channels(self) -> dict[bytes, List[int]]:
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

        sync_reply, reason = _send_hv_command(
            self=self,
            client_id=client_id,
            command="set_hv_sync",
            payload={"channels": "all"},
            timeout_s=90.0,
        )

        if sync_reply is None:
            logger.error(f"HV sync failed for client {client_name}: {reason}")
            self.poutput(f"Client {client_name}: HV sync failed ({reason})")
            continue

        sync_payload = sync_reply.payload or {}
        sync_result = sync_payload.get("result", {})
        sync_error = sync_payload.get("error")

        if sync_error:
            logger.error(f"HV sync error from client {client_name}: {sync_error}")
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
                f"{_hv_to_user_channels(sorted(bad_channels))}"
            )

        if not ok_channels:
            self.poutput(f"Client {client_name}: no OK HV channels available.")
            continue

        if ok_off_channels:
            user_ok_off_channels = _hv_to_user_channels(ok_off_channels)

            self.poutput(
                f"Client {client_name}: switching ON HV channels "
                f"{user_ok_off_channels} and waiting for UP state..."
            )

            on_reply, reason = _send_hv_command(
                self=self,
                client_id=client_id,
                command="hv_on_and_wait",
                payload={"channels": user_ok_off_channels},
                timeout_s=300.0,
            )

            if on_reply is None:
                logger.error(
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
                logger.error(
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
                    f"{_hv_to_user_channels(sorted(successful_on))}"
                )

            if failed_on:
                self.poutput(
                    f"Client {client_name}: HV channels failed to go UP: "
                    f"{_hv_to_user_channels(sorted(failed_on))}"
                )

            if bad_after_on:
                self.poutput(
                    f"Client {client_name}: HV channels moved to BAD: "
                    f"{_hv_to_user_channels(sorted(bad_after_on))}"
                )

            final_hv_channels = sorted(
                set(ok_on_channels) | successful_on
            )

        else:
            final_hv_channels = ok_on_channels
            self.poutput(
                f"Client {client_name}: all usable OK channels already ON."
            )

        final_user_channels = _hv_to_user_channels(final_hv_channels)

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

def _enable_rc_channels(self, channels: List[int]) -> bool:
    """
    Enable selected RC acquisition channels by writing register 19.

    Channels are expected in external numbering: 0..6.
    """

    client_ids = self.control_manager.server_state.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")
        return False

    if not channels:
        self.poutput("No RC channels selected for acquisition.")
        logger.warning("Cannot enable RC channels: empty channel list")
        return False

    register_address = 19
    register_value = 0

    for ch in channels:
        if ch < 0 or ch >= 7:
            self.poutput(f"Invalid RC channel: {ch}")
            logger.error(f"Invalid RC channel requested for acquisition: {ch}")
            return False

        register_value |= 1 << ch

    command = "rc_write_register"
    payload = {
        "address": register_address,
        "value": register_value,
    }
    timeout_s = 35.0

    successful_clients = 0
    failed_clients = 0

    for client_id in client_ids:
        client_name = client_id.decode(errors="ignore")

        rc_command = self.control_manager.message_handler.create_command(
            channel=Channel.RC,
            command=command,
            payload=payload,
            sender="server",
        )

        self.control_manager.queue_message(client_id, rc_command)

        reply, reason = self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=rc_command.request_id,
            timeout_s=timeout_s,
        )

        if reply is None:
            failed_clients += 1
            logger.error(
                f"RC channel enable failed for client {client_name}: {reason}"
            )
            self.poutput(f"Client {client_name}: no reply ({reason})")
            continue

        reply_payload = reply.payload or {}
        status = reply_payload.get("status")
        result = reply_payload.get("result", {})
        error = reply_payload.get("error")

        if status != "ok":
            failed_clients += 1
            logger.error(
                f"RC channel enable failed for client {client_name}: {error}"
            )
            self.poutput(
                f"Client {client_name}: failed to write RC register {register_address}."
            )

            if error:
                self.poutput(f"Client {client_name}: error: {error}")

            continue

        written_address = result.get("address", register_address)
        written_value = result.get("value", register_value)

        successful_clients += 1

        self.poutput(
            f"Client {client_name}: RC register {written_address} "
            f"written with value {written_value} "
            f"(enabled channels: {channels})"
        )

        logger.info(
            f"Enabled RC acquisition channels for client {client_name}: "
            f"channels={channels}, register={written_address}, value={written_value}"
        )

    if successful_clients == 0:
        self.poutput("Failed to enable RC channels on all clients.")
        return False

    if failed_clients > 0:
        self.poutput(
            f"Warning: RC channels enabled on {successful_clients} client(s), "
            f"failed on {failed_clients} client(s)."
        )

    return True

########################
# ACQUISITION COMMANDS #
########################

acquisition_parser = argparse.ArgumentParser()

acquisition_subparsers = acquisition_parser.add_subparsers(
    dest="command",
    required=True,
)

start_parser = acquisition_subparsers.add_parser(
    "start",
    help="Start acquisition",
)

start_parser.add_argument(
    "--duration",
    type=float,
    default=None,
    help="Acquisition duration in seconds. If omitted, run until stopped.",
)

start_parser.add_argument(
    "--type",
    dest="acq_type",
    type=str,
    default="test",
    help="Acquisition type, e.g. test, pedestal, spe, gain, threshold.",
)

start_parser.add_argument(
    "--suffix",
    type=str,
    default="",
    help="Suffix for the output file name.",
)

start_parser.add_argument(
    "--run-id",
    type=str,
    default=None,
    help="Optional run ID. If omitted, an automatic acq_N folder is created.",
)

start_parser.add_argument(
    "--batch-id",
    type=str,
    default=None,
    help="Batch ID used to create the acquisition folder.",
)

start_parser.add_argument(
    "--force-compile",
    action="store_true",
    help="Force recompilation of evreceiver before starting acquisition.",
)


stop_parser = acquisition_subparsers.add_parser(
    "stop",
    help="Stop acquisition",
)


@cmd2.with_argparser(acquisition_parser)
@cmd2.with_category("Acquisition Commands")
def do_acquisition(self, args: argparse.Namespace) -> None:
    """Acquisition commands: acquisition start, acquisition stop."""

    if args.command == "start":
        self.poutput("Acquisition start command received.")

        enabled_channels_by_client = _enable_hv_channels(self=self)

        if not enabled_channels_by_client:
            self.poutput("No HV channels available for acquisition.")
            return

        for client_id, channels in enabled_channels_by_client.items():
            client_name = client_id.decode(errors="ignore")

            if not channels:
                self.poutput(
                    f"Client {client_name}: no channels available for RC enable."
                )
                continue

            rc_ok = _enable_rc_channels(
                self=self,
                channels=channels,
            )

            if not rc_ok:
                self.poutput(
                    f"Client {client_name}: RC channel enable failed. "
                    "Skipping acquisition start."
                )
                continue

        receiver_info = self.data_receiver_service.start(
            duration=args.duration,
            suffix=args.suffix,
            acq_type=args.acq_type,
            run_id=args.run_id,
            batch_id=args.batch_id,
            force_compile=args.force_compile,
        )

        if receiver_info is None:
            self.poutput("Failed to start data receiver.")
            logger.error("Failed to start data receiver")
            return

        self.poutput(
            f"Data receiver started. "
            f"PID={receiver_info['pid']}, file={receiver_info['file']}"
        )

        return

    if args.command == "stop":
        stopped = self.data_receiver_service.stop()

        if stopped:
            self.poutput("Acquisition stopped.")
        else:
            self.poutput("Failed to stop acquisition.")

        return

