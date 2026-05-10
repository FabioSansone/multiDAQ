import argparse
import cmd2
from server.utils.logger import get_logger
from common.message_handler import Channel

logger = get_logger('hv_commands')

####################
#COMMON HV COMMANDS#
####################


set_common_parser = argparse.ArgumentParser()
set_common_subparsers = set_common_parser.add_subparsers(
    dest="parameter",
    required=True,
)

voltage_parser = set_common_subparsers.add_parser("voltage")
voltage_parser.add_argument("value", type=int, help="Common voltage value")
voltage_parser.add_argument("--channels", type=str, help="Channels selected. Can be \"all\" or comma separated string list", default="all")

@cmd2.with_argparser(set_common_parser)
@cmd2.with_category("HV Commands")
def do_set_common(self, args: argparse.Namespace) -> None:
    """Set a common HV parameter for selected channels."""

    command_map = {
        "voltage": "set_common_voltage",
    }

    command = command_map[args.parameter]
    client_ids = self.control_manager.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")
        return

    for client_id in client_ids:
        hv_command = self.control_manager.message_handler.create_command(
            channel=Channel.HV,
            command=command,
            payload={
                "channels": args.channels,
                "common_voltage": args.value,
            },
            sender="server",
        )

        self.control_manager.queue_message(client_id, hv_command)

        reply, reason = self.control_manager.wait_for_reply(
            client_id=client_id,
            in_reply_to=hv_command.request_id,
            timeout_s=35.0,
        )

        client_name = client_id.decode(errors="ignore")

        if reply is None:
            logger.error(
                f"HV command {command} failed for client {client_name}: {reason}"
            )
            self.poutput(f"No reply from client {client_name}. Reason: {reason}")
            continue

        payload = reply.payload
        status = payload.get("status")
        result = payload.get("result", {})
        error = payload.get("error")

        successful = result.get("successful_channels", [])
        failed = result.get("failed_channels", [])
        skipped = result.get("skipped_channels", [])
        not_responding = result.get("not_responding_channels", [])
        bad = result.get("bad_channels", [])

        not_set = sorted(set(failed + skipped + not_responding))

        if status != "ok":
            logger.error(
                f"HV command {command} returned error from client {client_name}: {error}"
            )

            if successful:
                self.poutput(
                    f"Client {client_name}: voltage set to {args.value} V "
                    f"on channels: {successful}."
                )

            if not_set:
                self.poutput(
                    f"Client {client_name}: channels not set: {not_set}."
                )

            if not_responding:
                self.poutput(
                    f"Client {client_name}: not responding channels moved to bad: "
                    f"{not_responding}."
                )

            if bad:
                self.poutput(
                    f"Client {client_name}: current bad channels: {bad}."
                )

            if error:
                self.poutput(
                    f"Client {client_name}: error: {error}"
                )

            if not successful:
                self.poutput(
                    f"Client {client_name}: failed to set voltage on all selected channels."
                )

            continue

        if not_set:
            self.poutput(
                f"Client {client_name}: voltage set to {args.value} V "
                f"on channels: {successful}. Not set: {not_set}."
            )
        else:
            self.poutput(
                f"Client {client_name}: all selected channels were set to {args.value} V."
            )

