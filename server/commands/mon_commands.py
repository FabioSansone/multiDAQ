import argparse
import cmd2

from server.core.server_state import command_guard, ServerFSM
from server.utils.logger import get_logger


logger = get_logger("monitoring_commands")


MONITORING_STATUS_ALLOWED_STATES = [
    ServerFSM.CONTROL_CONNECTED,
    ServerFSM.CONNECTED,
    ServerFSM.CONFIGURING,
    ServerFSM.READY,
    ServerFSM.ACQUIRING,
    ServerFSM.FINALIZING,
    ServerFSM.ERROR,
]


MIN_WATCH_INTERVAL_S = 0.2
MAX_WATCH_DURATION_S = 120.0



########################
# MONITORING COMMANDS  #
########################

status_parser = argparse.ArgumentParser()

status_target_group = status_parser.add_mutually_exclusive_group()

status_target_group.add_argument(
    "--multipmt-id",
    dest="multipmt_id",
    type=str,
    default=None,
    help=(
        "Query status for a single client, identified by its multipmt_id. "
        "If neither --multipmt-id nor --batch-id is given, all connected "
        "clients are queried."
    ),
)

status_target_group.add_argument(
    "--batch-id",
    dest="batch_id",
    type=str,
    default=None,
    help="Query status for all clients belonging to this batch_id.",
)

status_parser.add_argument(
    "--main",
    action="store_true",
    help="Show Main Board sensors (temperature, humidity, orientation, currents).",
)

status_parser.add_argument(
    "--rc",
    action="store_true",
    help="Show RC channel rates (normal and trigger).",
)

status_parser.add_argument(
    "--hv",
    action="store_true",
    help="Show HV voltage/current/temperature per channel.",
)

status_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help=(
        'Channels to include in the "rc" and "hv" sections. '
        'Can be "all" or a comma-separated list. Ignored if neither '
        "--rc nor --hv is shown."
    ),
)

status_parser.add_argument(
    "--interval",
    type=float,
    default=None,
    help=(
        "Refresh interval in seconds for repeated printing. "
        "If omitted, a single instantaneous snapshot is printed. "
        "Requires --duration."
    ),
)

status_parser.add_argument(
    "--duration",
    type=float,
    default=None,
    help=(
        "Maximum total duration in seconds for repeated printing. "
        f"Requires --interval. Capped at {MAX_WATCH_DURATION_S:.0f}s "
        "(use the background monitoring service for longer periods)."
    ),
)


@cmd2.with_argparser(status_parser)
@cmd2.with_category("Monitoring Commands")
@command_guard(MONITORING_STATUS_ALLOWED_STATES)
def do_status(self, args: argparse.Namespace) -> None:


    if (args.interval is None) != (args.duration is None):
        self.poutput(
            "--interval and --duration must be specified together for "
            "repeated printing."
        )
        logger.error(
            f"Incomplete watch parameters: interval={args.interval}, "
            f"duration={args.duration}"
        )
        return

    if args.interval is not None:
        if args.interval < MIN_WATCH_INTERVAL_S:
            self.poutput(f"--interval must be at least {MIN_WATCH_INTERVAL_S}s.")
            logger.error(f"Invalid --interval value: {args.interval}")
            return

        if args.duration <= 0 or args.duration > MAX_WATCH_DURATION_S:
            self.poutput(
                f"--duration must be between 0 and {MAX_WATCH_DURATION_S:.0f}s "
                "(use background monitoring for longer periods)."
            )
            logger.error(f"Invalid --duration value: {args.duration}")
            return

        if args.duration < args.interval:
            self.poutput("--duration must be greater than or equal to --interval.")
            logger.error(
                f"--duration ({args.duration}) shorter than "
                f"--interval ({args.interval})"
            )
            return

    self.mon_orchestrator.status(args)