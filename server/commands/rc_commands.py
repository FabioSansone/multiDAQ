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

read_parser = rc_subparsers.add_parser("read")
read_parser.add_argument(
    "address",
    type=int,
    help="RC register address to read",
)

write_parser = rc_subparsers.add_parser("write")
write_parser.add_argument(
    "address",
    type=int,
    help="RC register address to write",
)
write_parser.add_argument(
    "value",
    type=int,
    help="Value to write into the RC register",
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
        "read": "rc_read_register",
        "write": "rc_write_register",
    }

    command = command_map[args.parameter]

    client_ids = self.control_manager.server_state.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")
        return

    if args.parameter == "read":
        payload = {
            "address": args.address,
        }
        timeout_s = 35.0

    elif args.parameter == "write":
        payload = {
            "address": args.address,
            "value": args.value,
        }
        timeout_s = 35.0

    else:
        payload = {
            "channels": args.channels,
        }
        timeout_s = 35.0

    for client_id in client_ids:
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

        if args.parameter == "read":
            value = result.get("value")
            address = result.get("address", args.address)

            if status != "ok":
                logger.error(
                    f"RC read register failed for client {client_name}: {error}"
                )
                self.poutput(
                    f"Client {client_name}: failed to read register {address}."
                )
                if error:
                    self.poutput(f"Client {client_name}: error: {error}")
                continue

            self.poutput(
                f"Client {client_name}: register {address} = {value}"
            )
            continue

        if args.parameter == "write":
            address = result.get("address", args.address)
            value = result.get("value", args.value)

            if status != "ok":
                logger.error(
                    f"RC write register failed for client {client_name}: {error}"
                )
                self.poutput(
                    f"Client {client_name}: failed to write register {address}."
                )
                if error:
                    self.poutput(f"Client {client_name}: error: {error}")
                continue

            self.poutput(
                f"Client {client_name}: register {address} written with value {value}"
            )
            continue

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