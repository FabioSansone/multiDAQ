import argparse
import cmd2

from server.utils.logger import get_logger
from common.message_handler import Channel


logger = get_logger("rc_commands")


####################
# COMMON RC COMMANDS
####################

rc_parser = argparse.ArgumentParser()
rc_subparsers = rc_parser.add_subparsers(
    dest="parameter",
    required=True,
)

start_parser = rc_subparsers.add_parser("start")
start_parser.add_argument(
    "--channels",
    type=str,
    help='Channels selected. Can be "all" or comma separated string list',
    default="all",
)

reset_parser = rc_subparsers.add_parser("reset")
reset_parser.add_argument(
    "--channels",
    type=str,
    help='Channels selected. Can be "all" or comma separated string list',
    default="all",
)

boot_parser = rc_subparsers.add_parser("boot")
boot_parser.add_argument(
    "--channels",
    type=str,
    help='Channels selected. Can be "all" or comma separated string list',
    default="all",
)


@cmd2.with_argparser(rc_parser)
@cmd2.with_category("RC Commands")
def do_rc(self, args: argparse.Namespace) -> None:
    """RunControl commands."""

    command_map = {
        "start": "rc_acq_start",
        "reset": "rc_reset",
        "boot": "rc_boot",
    }

    command = command_map[args.parameter]

    client_ids = self.control_manager.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")
        return

    for client_id in client_ids:
        rc_command = self.control_manager.message_handler.create_command(
            channel=Channel.RC,
            command=command,
            payload={
                "channels": args.channels,
            },
            sender="server",
        )

        self.control_manager.queue_message(client_id, rc_command)

        reply, reason = self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=rc_command.request_id,
            timeout_s=35.0,
        )

        client_name = client_id.decode(errors="ignore")

        if reply is None:
            logger.error(
                f"RC command {command} failed for client {client_name}: {reason}"
            )
            self.poutput(f"No reply from client {client_name}. Reason: {reason}")
            continue

        payload = reply.payload
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        failed = result.get("failed_channels", [])

        if args.parameter == "start":
            successful = result.get("started_channels", [])
            action = "started in acquisition mode"

        elif args.parameter == "boot":
            successful = result.get("boot_channels", [])
            action = "started in boot mode"

        elif args.parameter == "reset":
            successful = result.get("reset_channels", [])
            action = "reset"

        else:
            successful = []
            action = args.parameter

        if status != "ok":
            logger.error(
                f"RC command {command} returned error from client {client_name}: {error}"
            )

            self.poutput(f"Client {client_name}: RC command failed.")

            if successful:
                self.poutput(
                    f"Client {client_name}: channels {action}: {successful}."
                )

            if failed:
                self.poutput(
                    f"Client {client_name}: failed channels: {failed}."
                )

            if error:
                self.poutput(f"Client {client_name}: error: {error}")

            continue

        if failed:
            self.poutput(
                f"Client {client_name}: channels {action}: {successful}. "
                f"Failed: {failed}."
            )
        else:
            self.poutput(
                f"Client {client_name}: channels {action}: {successful}."
            )