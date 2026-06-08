import argparse
import cmd2
from server.utils.logger import get_logger
from common.message_handler import Channel

logger = get_logger("hv_commands")

############
#HV HELPERS#
############

def _print_hv_reply(self, client_name: str, command: str, args: argparse.Namespace, reply) -> None:
    payload = reply.payload
    status = payload.get("status")
    result = payload.get("result", {})
    error = payload.get("error")

    successful = result.get("successful_channels", [])
    failed = result.get("failed_channels", [])
    skipped = result.get("skipped_channels", [])
    not_responding = result.get("not_responding_channels", [])
    bad = result.get("bad_channels", [])

    not_done = sorted(set(failed + skipped + not_responding))

    if command == "set_common_voltage":
        action = f"voltage set to {args.value} V"
    elif command == "set_common_threshold":
        action = f"threshold set to {args.value}"
    elif command == "hv_on":
        action = "powered ON"
    elif command == "hv_off":
        action = "powered OFF"
    else:
        action = command

    if successful:
        self.poutput(
            f"Client {client_name}: {action} on channels: {successful}."
        )

    if not_done:
        self.poutput(
            f"Client {client_name}: channels not completed: {not_done}."
        )

    if bad:
        self.poutput(
            f"Client {client_name}: current bad channels: {bad}."
        )

    if error:
        logger.error(
            f"HV command {command} returned error from client {client_name}: {error}"
        )
        self.poutput(f"Client {client_name}: error: {error}")

    if status == "ok" and not not_done:
        self.poutput(
            f"Client {client_name}: HV command completed successfully."
        )

#################
# HV COMMANDS
#################

hv_parser = argparse.ArgumentParser()
hv_subparsers = hv_parser.add_subparsers(
    dest="command_group",
    required=True,
)

# hv on
on_parser = hv_subparsers.add_parser("on")
on_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma separated string list',
)

# hv off
off_parser = hv_subparsers.add_parser("off")
off_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma separated string list',
)

# hv set_common ...
set_common_parser = hv_subparsers.add_parser("set_common")
set_common_subparsers = set_common_parser.add_subparsers(
    dest="parameter",
    required=True,
)

voltage_parser = set_common_subparsers.add_parser("voltage")
voltage_parser.add_argument("value", type=int, help="Common voltage value")
voltage_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma separated string list',
)

threshold_parser = set_common_subparsers.add_parser("threshold")
threshold_parser.add_argument("value", type=int, help="Common threshold value")
threshold_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma separated string list',
)

@cmd2.with_argparser(hv_parser)
@cmd2.with_category("HV Commands")
def do_hv(self, args: argparse.Namespace) -> None:
    """HV commands: hv on, hv off, hv set_common voltage, hv set_common threshold."""

    if args.command_group == "on":
        command = "hv_on"
        value = None
        payload = {
            "channels": args.channels,
        }
        timeout_s = 35.0

    elif args.command_group == "off":
        command = "hv_off"
        value = None
        payload = {
            "channels": args.channels,
        }
        timeout_s = 35.0

    elif args.command_group == "set_common":
        command_map = {
            "voltage": "set_common_voltage",
            "threshold": "set_common_threshold",
        }

        payload_key_map = {
            "voltage": "common_voltage",
            "threshold": "common_threshold",
        }


        command = command_map[args.parameter]
        payload_key = payload_key_map[args.parameter]
        value = args.value

        payload = {
            "channels": args.channels,
            payload_key: value,
        }
        timeout_s = 35.0

    else:
        self.poutput(f"Unknown HV command group: {args.command_group}")

    client_ids = self.control_manager.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")

    for client_id in client_ids:
        hv_command = self.control_manager.message_handler.create_command(
            channel=Channel.HV,
            command=command,
            payload=payload,
            sender="server",
        )

        self.control_manager.queue_message(client_id, hv_command)

        reply, reason = self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=hv_command.request_id,
            timeout_s=timeout_s,
        )

        client_name = client_id.decode(errors="ignore")

        if reply is None:
            logger.error(
                f"HV command {command} failed for client {client_name}: {reason}"
            )
            self.poutput(f"No reply from client {client_name}. Reason: {reason}")
            continue

        _print_hv_reply(
            self=self,
            client_name=client_name,
            command=command,
            args=args,
            reply=reply,
        )