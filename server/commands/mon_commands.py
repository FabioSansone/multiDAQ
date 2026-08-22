import argparse
import cmd2

from server.core.server_state import command_guard, ServerFSM
from server.utils.logger import get_logger


logger = get_logger("monitoring_commands")


MONITORING_ALLOWED_STATES = [
    ServerFSM.CONTROL_CONNECTED,
    ServerFSM.CONNECTED,
    ServerFSM.CONFIGURING,
    ServerFSM.READY,
    ServerFSM.ACQUIRING,
    ServerFSM.FINALIZING,
    ServerFSM.ERROR,
]


MONITOR_POLL_INTERVALS_S = (1, 5, 10, 30)
DEFAULT_MONITOR_POLL_INTERVAL_S = 5
DEFAULT_MONITOR_POLL_DURATION_S = 30
MAX_MONITOR_POLL_DURATION_S = 120


########################
# MONITORING COMMANDS  #
########################

monitor_parser = argparse.ArgumentParser(
    description="Monitoring commands."
)

monitor_subparsers = monitor_parser.add_subparsers(
    dest="monitor_command",
    required=True,
)


# ---------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------

def add_monitor_target_arguments(parser: argparse.ArgumentParser) -> None:

    target_group = parser.add_mutually_exclusive_group()

    target_group.add_argument(
        "--multipmt-id",
        dest="multipmt_id",
        type=str,
        default=None,
        help=(
            "Select a single client by multipmt_id. "
            "If neither --multipmt-id nor --batch-id is given, all "
            "clients connected to the Monitoring Plane are selected."
        ),
    )

    target_group.add_argument(
        "--batch-id",
        dest="batch_id",
        type=str,
        default=None,
        help=(
            "Select a single client by batch_id. "
            "If neither --multipmt-id nor --batch-id is given, all "
            "clients connected to the Monitoring Plane are selected."
        ),
    )


def add_monitor_section_arguments(parser: argparse.ArgumentParser) -> None:

    parser.add_argument(
        "--main",
        action="store_true",
        help="Show Main Board monitoring data.",
    )

    parser.add_argument(
        "--rc",
        action="store_true",
        help="Show RC monitoring data.",
    )

    parser.add_argument(
        "--hv",
        action="store_true",
        help="Show HV monitoring data.",
    )

    parser.add_argument(
        "--channels",
        type=str,
        default="all",
        help=(
            'Channels to include in RC and HV monitoring. '
            'Can be "all" or a comma-separated list such as "0,1,2". '
            "Ignored when neither --rc nor --hv is selected."
        ),
    )


# ---------------------------------------------------------------------
# monitor snapshot
# ---------------------------------------------------------------------

snapshot_parser = monitor_subparsers.add_parser(
    "snapshot",
    help="Read and display an instantaneous monitoring snapshot.",
)

add_monitor_target_arguments(snapshot_parser)
add_monitor_section_arguments(snapshot_parser)


# ---------------------------------------------------------------------
# monitor poll
# ---------------------------------------------------------------------

poll_parser = monitor_subparsers.add_parser(
    "poll",
    help="Repeatedly read monitoring snapshots for a limited time.",
)

add_monitor_target_arguments(poll_parser)
add_monitor_section_arguments(poll_parser)

poll_parser.add_argument(
    "--interval",
    type=int,
    choices=MONITOR_POLL_INTERVALS_S,
    default=DEFAULT_MONITOR_POLL_INTERVAL_S,
    help=(
        "Polling interval in seconds. "
        f"Allowed values: {MONITOR_POLL_INTERVALS_S}. "
        f"Default: {DEFAULT_MONITOR_POLL_INTERVAL_S}s."
    ),
)

poll_parser.add_argument(
    "--duration",
    type=int,
    default=DEFAULT_MONITOR_POLL_DURATION_S,
    help=(
        "Total polling duration in seconds. "
        f"Maximum: {MAX_MONITOR_POLL_DURATION_S}s. "
        f"Default: {DEFAULT_MONITOR_POLL_DURATION_S}s."
    ),
)


# ---------------------------------------------------------------------
# monitor save        
# ---------------------------------------------------------------------

save_parser = monitor_subparsers.add_parser(
    "save",
    help="Start or stop persistent monitoring data storage.",
)

save_subparsers = save_parser.add_subparsers(
    dest="save_action",
    required=True,
)

save_start_parser = save_subparsers.add_parser(
    "start",
    help="Start saving monitoring data.",
)

add_monitor_target_arguments(save_start_parser)
add_monitor_section_arguments(save_start_parser)

save_start_parser.add_argument(
    "--interval",
    type=int,
    choices=MONITOR_POLL_INTERVALS_S,
    default=DEFAULT_MONITOR_POLL_INTERVAL_S,
    help=(
        "Monitoring acquisition interval in seconds. "
        f"Allowed values: {MONITOR_POLL_INTERVALS_S}. "
        f"Default: {DEFAULT_MONITOR_POLL_INTERVAL_S}s."
    ),
)

save_start_parser.add_argument(
    "--format",
    dest="save_format",
    choices=("csv", "parquet"),
    default="csv",
    help="Monitoring output format. Default: csv.",
)

save_subparsers.add_parser(
    "stop",
    help="Stop saving monitoring data.",
)


# ---------------------------------------------------------------------
# monitor show        
# ---------------------------------------------------------------------

show_parser = monitor_subparsers.add_parser(
    "show",
    help="Start or stop monitoring visualization.",
)

show_subparsers = show_parser.add_subparsers(
    dest="show_action",
    required=True,
)

show_subparsers.add_parser(
    "start",
    help="Start monitoring visualization.",
)

show_subparsers.add_parser(
    "stop",
    help="Stop monitoring visualization.",
)



@cmd2.with_argparser(monitor_parser)
@cmd2.with_category("Monitoring Commands")
@command_guard(MONITORING_ALLOWED_STATES)
def do_monitor(self, args: argparse.Namespace) -> None:

    if args.monitor_command == "snapshot":
        self.mon_orchestrator.snapshot(args)
        return

    if args.monitor_command == "poll":

        if args.duration <= 0:
            self.perror(
                "--duration must be greater than 0."
            )
            logger.error(
                f"Invalid monitoring poll duration: {args.duration}"
            )
            return

        if args.duration > MAX_MONITOR_POLL_DURATION_S:
            self.perror(
                f"--duration cannot exceed "
                f"{MAX_MONITOR_POLL_DURATION_S} seconds."
            )
            logger.error(
                f"Monitoring poll duration exceeds maximum: "
                f"{args.duration}"
            )
            return

        if args.duration < args.interval:
            self.perror(
                "--duration must be greater than or equal to --interval."
            )
            logger.error(
                f"Monitoring poll duration ({args.duration}s) is shorter "
                f"than interval ({args.interval}s)"
            )
            return

        self.mon_orchestrator.poll(args)
        return

    if args.monitor_command == "save":

        if args.save_action == "start":
            self.poutput(
                "monitor save start is not implemented yet (M4)."
            )
            return

        if args.save_action == "stop":
            self.poutput(
                "monitor save stop is not implemented yet (M4)."
            )
            return

    if args.monitor_command == "show":

        if args.show_action == "start":
            self.poutput(
                "monitor show start is not implemented yet (M5)."
            )
            return

        if args.show_action == "stop":
            self.poutput(
                "monitor show stop is not implemented yet (M5)."
            )
            return