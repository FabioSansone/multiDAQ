import argparse
import cmd2
from typing import List
from server.utils.logger import get_logger
from common.message_handler import Channel
from server.core.server_state import command_guard, ServerFSM
from server.utils.channels import hv_to_user_channels

logger = get_logger("hv_commands")


COMMAND_FSM_MAP = {
    "on": {ServerFSM.READY, ServerFSM.CONNECTED},
    "off": {ServerFSM.READY, ServerFSM.CONNECTED, ServerFSM.ERROR},
    "set_common": {ServerFSM.READY, ServerFSM.CONNECTED, ServerFSM.ERROR},
    "mark_bad": {ServerFSM.READY, ServerFSM.CONNECTED, ServerFSM.ERROR},
    "unmark_bad": {ServerFSM.READY, ServerFSM.CONNECTED, ServerFSM.ERROR},
    "status": {ServerFSM.READY, ServerFSM.CONNECTED, ServerFSM.ERROR},
}



############
#HV HELPERS#
############



def _print_hv_lists(self, client_name: str, result: dict):
    self.poutput(f"\nClient {client_name}: HV sync completed.")
    self.poutput(f"  OK  channels: {hv_to_user_channels(result.get('ok_channels', []))}")
    self.poutput(f"  BAD channels: {hv_to_user_channels(result.get('bad_channels', []))}")
    self.poutput(f"  ON  channels: {hv_to_user_channels(result.get('on_channels', []))}")
    self.poutput(f"  OFF channels: {hv_to_user_channels(result.get('off_channels', []))}")
    self.poutput(f"  FIXED BAD channels: {hv_to_user_channels(result.get('fixed_bad_channels', []))}")

def _print_hv_reply(self, client_name: str, command: str, args: argparse.Namespace, reply) -> None:
    payload = reply.payload
    status = payload.get("status")
    result = payload.get("result", {})
    error = payload.get("error")

    successful = hv_to_user_channels(result.get("successful_channels", []))
    failed = hv_to_user_channels(result.get("failed_channels", []))
    skipped = hv_to_user_channels(result.get("skipped_channels", []))
    not_responding = hv_to_user_channels(result.get("not_responding_channels", []))
    bad = hv_to_user_channels(result.get("bad_channels", []))
    fixed_bad = hv_to_user_channels(result.get("fixed_bad_channels", []))

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
    if fixed_bad:
        self.poutput(f"Client {client_name}: current FIXED BAD channels: {fixed_bad}.")

    if error:
        logger.error(
            f"HV command {command} returned error from client {client_name}: {error}"
        )
        self.poutput(f"Client {client_name}: error: {error}")

    if status == "ok" and not not_done:
        self.poutput(
            f"Client {client_name}: HV command completed successfully."
        )


def _parse_comma_separated_strings(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_hv_targets(self, args, connected_client_ids: list[bytes], command_label: str) -> list[bytes] | None:
    """
    Resolve which clients an hv subcommand should target, based on
    --client_ids / --batch_ids / --multipmt_ids. Returns None (and prints
    an error) if resolution fails or is ambiguous.
    """
    provided_groups = [
        (name, value) for name, value in (
            ("client_ids", args.client_ids),
            ("batch_ids", args.batch_ids),
            ("multipmt_ids", args.multipmt_ids),
        ) if value is not None
    ]

    if len(provided_groups) > 1:
        names = ", ".join(f"--{name}" for name, _ in provided_groups)
        self.poutput(f"Specify only ONE of --client_ids, --batch_ids, --multipmt_ids (got: {names}).")
        return None

    if not provided_groups:
        return list(connected_client_ids)

    group_name, group_value = provided_groups[0]
    raw_ids = _parse_comma_separated_strings(group_value)

    if not raw_ids:
        self.poutput(f"No valid values provided for --{group_name}.")
        return None

    resolved_client_ids: list[bytes] = []
    unresolved: list[str] = []

    for raw_id in raw_ids:
        if group_name == "client_ids":
            candidate = raw_id.encode()
            resolved = candidate if candidate in connected_client_ids else None
        elif group_name == "batch_ids":
            resolved = self.server_state.get_client_id_by_batch_id(raw_id)
        else:
            resolved = self.server_state.get_client_id_by_multipmt_id(raw_id)

        if resolved is None:
            unresolved.append(raw_id)
        else:
            resolved_client_ids.append(resolved)

    if unresolved:
        self.poutput(f"Could not resolve {group_name}: {unresolved}")
        logger.error(f"{command_label}: unresolved {group_name}: {unresolved}")

    if not resolved_client_ids:
        self.poutput(f"No clients resolved for {command_label}. Aborting.")
        return None

    return resolved_client_ids

#################
# HV COMMANDS
#################

hv_parser = argparse.ArgumentParser()
hv_subparsers = hv_parser.add_subparsers(
    dest="command_group",
    required=True,
)


on_parser = hv_subparsers.add_parser("on")
on_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma separated string list',
)


off_parser = hv_subparsers.add_parser("off")
off_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma separated string list',
)

mark_parser = hv_subparsers.add_parser("mark_bad")
mark_parser.add_argument("--channels", type=str, default="all", help='Channels selected. Can be "all" or comma separated string list')
mark_parser.add_argument("--client_ids", type=str, default=None, help='Comma separated string of client_ids')
mark_parser.add_argument("--batch_ids", type=str, default=None, help='Comma separated string of batch_ids to identify the clients')
mark_parser.add_argument("--multipmt_ids", type=str, default=None, help='Comma separated string of multipmt_ids to identify the clients')

unmark_parser = hv_subparsers.add_parser("unmark_bad")
unmark_parser.add_argument("--channels", type=str, default="all", help='Channels selected. Can be "all" or comma separated string list')
unmark_parser.add_argument("--client_ids", type=str, default=None, help='Comma separated string of client_ids')
unmark_parser.add_argument("--batch_ids", type=str, default=None, help='Comma separated string of batch_ids to identify the clients')
unmark_parser.add_argument("--multipmt_ids", type=str, default=None, help='Comma separated string of multipmt_ids to identify the clients')

status_parser = hv_subparsers.add_parser("status")
status_parser.add_argument("--channels", type=str, default="all", help='Channels selected. Can be "all" or comma separated string list')
status_parser.add_argument("--client_ids", type=str, default=None, help='Comma separated string of client_ids')
status_parser.add_argument("--batch_ids", type=str, default=None, help='Comma separated string of batch_ids to identify the clients')
status_parser.add_argument("--multipmt_ids", type=str, default=None, help='Comma separated string of multipmt_ids to identify the clients')


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
@command_guard([ServerFSM.READY, ServerFSM.CONNECTED, ServerFSM.ERROR])
def do_hv(self, args: argparse.Namespace) -> None:
    """HV commands: hv on, hv off, hv set_common voltage, hv set_common threshold."""

    current_state = self.server_state.get_server_state()
    allowed_states = COMMAND_FSM_MAP[args.command_group]

    if current_state not in allowed_states:
        allowed_names = ", ".join(sorted(s.value for s in allowed_states))
        self.poutput(
            f"Cannot run 'hv {args.command_group}' while server is '{current_state.value}'. "
            f"Allowed states: {allowed_names}."
        )
        logger.error(
            f"HV command '{args.command_group}' blocked: state={current_state.value}, "
            f"allowed={allowed_names}"
        )
        return

    client_ids = self.server_state.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")
        return
    
    target_client_ids = client_ids
    
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
    
    elif args.command_group == "mark_bad":
        command = "mark_bad"
        resolved = _resolve_hv_targets(self, args, client_ids, "mark_bad")
        if resolved is None:
            return
        target_client_ids = resolved
        payload = {"channels": args.channels}
        timeout_s = 35.0
    
    elif args.command_group == "unmark_bad":
        command = "unmark_bad"
        resolved = _resolve_hv_targets(self, args, client_ids, "unmark_bad")
        if resolved is None:
            return
        target_client_ids = resolved
        payload = {"channels": args.channels}
        timeout_s = 35.0

    elif args.command_group == "status":
        command = "set_hv_sync"
        resolved = _resolve_hv_targets(self, args, client_ids, "status")
        if resolved is None:
            return
        target_client_ids = resolved
        payload = {"channels": args.channels}
        timeout_s = 90.0

    else:
        self.poutput(f"Unknown HV command group: {args.command_group}")
        return


    for client_id in target_client_ids:
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

        if args.command_group == "status":
            _print_hv_lists(self, client_name, reply.payload.get("result", {}))
        else:
            _print_hv_reply(self=self, client_name=client_name, command=command, args=args, reply=reply)