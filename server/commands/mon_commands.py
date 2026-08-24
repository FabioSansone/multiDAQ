import argparse
import cmd2

from server.core.server_state import (
    command_guard,
    ServerFSM,
)
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


# =====================================================================
# Monitoring configuration
# =====================================================================

MONITOR_INTERVALS_S = (
    1,
    5,
    10,
    30,
    60,
)

DEFAULT_MONITOR_INTERVAL_S = 5

DEFAULT_MONITOR_POLL_DURATION_S = 30
MAX_MONITOR_POLL_DURATION_S = 120

MONITOR_SAVE_FORMATS = (
    "csv",
    "parquet",
)


# =====================================================================
# Parser root
# =====================================================================

monitor_parser = argparse.ArgumentParser(
    description="Monitoring commands."
)

monitor_subparsers = monitor_parser.add_subparsers(
    dest="monitor_command",
    required=True,
)


# =====================================================================
# Common parser helpers
# =====================================================================

def add_monitor_target_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """
    Add client-selection arguments.

    No target argument means all clients currently connected to the
    Monitoring Plane.
    """

    target_group = parser.add_mutually_exclusive_group()

    target_group.add_argument(
        "--multipmt-id",
        dest="multipmt_id",
        type=str,
        default=None,
        help=(
            "Select a single client by multipmt_id. "
            "If neither --multipmt-id nor --batch-id is given, "
            "all clients connected to the Monitoring Plane are selected."
        ),
    )

    target_group.add_argument(
        "--batch-id",
        dest="batch_id",
        type=str,
        default=None,
        help=(
            "Select a single client by batch_id. "
            "If neither --multipmt-id nor --batch-id is given, "
            "all clients connected to the Monitoring Plane are selected."
        ),
    )


def add_monitor_data_section_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_channels: bool = True,
) -> None:
    """
    Add periodic/snapshot monitoring sections.

    MAIN, RC and HV correspond to actual monitoring sample streams.
    """

    parser.add_argument(
        "--main",
        action="store_true",
        help="Select Main Board monitoring data.",
    )

    parser.add_argument(
        "--rc",
        action="store_true",
        help="Select RC monitoring data.",
    )

    parser.add_argument(
        "--hv",
        action="store_true",
        help="Select HV monitoring data.",
    )

    if include_channels:
        parser.add_argument(
            "--channels",
            type=str,
            default="all",
            help=(
                'Channels to include in RC and HV monitoring. '
                'Can be "all" or a comma-separated list such as '
                '"0,1,2". Ignored when neither RC nor HV is selected.'
            ),
        )


def add_monitor_save_section_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """
    Add sections supported by persistent monitoring.

    EVENTS is independent from the periodic sample streams and therefore
    has no sampling interval of its own.
    """

    parser.add_argument(
        "--main",
        action="store_true",
        help="Select Main Board samples.",
    )

    parser.add_argument(
        "--rc",
        action="store_true",
        help="Select RC samples.",
    )

    parser.add_argument(
        "--hv",
        action="store_true",
        help="Select HV samples.",
    )

    parser.add_argument(
        "--events",
        action="store_true",
        help="Select asynchronous monitoring events.",
    )


# =====================================================================
# monitor snapshot
# =====================================================================

snapshot_parser = monitor_subparsers.add_parser(
    "snapshot",
    help="Read and display an instantaneous monitoring snapshot.",
)

add_monitor_target_arguments(
    snapshot_parser
)

add_monitor_data_section_arguments(
    snapshot_parser,
    include_channels=True,
)


# =====================================================================
# monitor poll
# =====================================================================

poll_parser = monitor_subparsers.add_parser(
    "poll",
    help=(
        "Repeatedly read monitoring snapshots "
        "for a limited amount of time."
    ),
)

add_monitor_target_arguments(
    poll_parser
)

add_monitor_data_section_arguments(
    poll_parser,
    include_channels=True,
)

poll_parser.add_argument(
    "--interval",
    type=int,
    choices=MONITOR_INTERVALS_S,
    default=DEFAULT_MONITOR_INTERVAL_S,
    help=(
        "Polling interval in seconds. "
        f"Allowed values: {MONITOR_INTERVALS_S}. "
        f"Default: {DEFAULT_MONITOR_INTERVAL_S}s."
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


# =====================================================================
# monitor save
# =====================================================================

save_parser = monitor_subparsers.add_parser(
    "save",
    help=(
        "Control persistent monitoring streams "
        "and monitoring data storage."
    ),
)

save_subparsers = save_parser.add_subparsers(
    dest="save_action",
    required=True,
)


# ---------------------------------------------------------------------
# monitor save start
# ---------------------------------------------------------------------

save_start_parser = save_subparsers.add_parser(
    "start",
    help=(
        "Start one or more monitoring streams "
        "and persist their data."
    ),
)

add_monitor_target_arguments(
    save_start_parser
)

add_monitor_save_section_arguments(
    save_start_parser
)

save_start_parser.add_argument(
    "--interval",
    type=int,
    choices=MONITOR_INTERVALS_S,
    default=DEFAULT_MONITOR_INTERVAL_S,
    help=(
        "Sampling interval for the selected MAIN/RC/HV streams. "
        "Ignored for EVENTS. "
        f"Allowed values: {MONITOR_INTERVALS_S}. "
        f"Default: {DEFAULT_MONITOR_INTERVAL_S}s."
    ),
)

save_start_parser.add_argument(
    "--format",
    dest="save_format",
    choices=MONITOR_SAVE_FORMATS,
    default="csv",
    help=(
        "Persistence format. "
        f"Available formats: {MONITOR_SAVE_FORMATS}. "
        "Default: csv."
    ),
)


# ---------------------------------------------------------------------
# monitor save stop
# ---------------------------------------------------------------------

save_stop_parser = save_subparsers.add_parser(
    "stop",
    help=(
        "Stop one or more monitoring streams "
        "without affecting the others."
    ),
)

add_monitor_target_arguments(
    save_stop_parser
)

add_monitor_save_section_arguments(
    save_stop_parser
)


# ---------------------------------------------------------------------
# monitor save status
# ---------------------------------------------------------------------

save_status_parser = save_subparsers.add_parser(
    "status",
    help=(
        "Show active monitoring streams "
        "and persistence configuration."
    ),
)

add_monitor_target_arguments(
    save_status_parser
)


# =====================================================================
# monitor sensors
# =====================================================================

sensors_parser = monitor_subparsers.add_parser(
    "sensors",
    help="Main Board sensor diagnostics.",
)

sensors_subparsers = sensors_parser.add_subparsers(
    dest="sensors_action",
    required=True,
)


# ---------------------------------------------------------------------
# monitor sensors status
# ---------------------------------------------------------------------

sensors_status_parser = sensors_subparsers.add_parser(
    "status",
    help=(
        "Show current sensor threshold state, "
        "availability and latest motion information."
    ),
)

add_monitor_target_arguments(
    sensors_status_parser
)


# =====================================================================
# monitor orientation
# =====================================================================

orientation_parser = monitor_subparsers.add_parser(
    "orientation",
    help=(
        "Read detector orientation using "
        "accelerometer and magnetometer data."
    ),
)

add_monitor_target_arguments(
    orientation_parser
)


# =====================================================================
# monitor show
#
# M5 placeholder. Kept here because the same monitoring infrastructure
# will later feed live visualization.
# =====================================================================

show_parser = monitor_subparsers.add_parser(
    "show",
    help="Start or stop monitoring visualization (M5).",
)

show_subparsers = show_parser.add_subparsers(
    dest="show_action",
    required=True,
)

show_start_parser = show_subparsers.add_parser(
    "start",
    help="Start monitoring visualization.",
)

add_monitor_target_arguments(
    show_start_parser
)

show_stop_parser = show_subparsers.add_parser(
    "stop",
    help="Stop monitoring visualization.",
)

add_monitor_target_arguments(
    show_stop_parser
)


# =====================================================================
# Command dispatcher
# =====================================================================

@cmd2.with_argparser(monitor_parser)
@cmd2.with_category("Monitoring Commands")
@command_guard(MONITORING_ALLOWED_STATES)
def do_monitor(
    self,
    args: argparse.Namespace,
) -> None:

    # -----------------------------------------------------------------
    # snapshot
    # -----------------------------------------------------------------

    if args.monitor_command == "snapshot":
        self.mon_orchestrator.snapshot(args)
        return


    # -----------------------------------------------------------------
    # poll
    # -----------------------------------------------------------------

    if args.monitor_command == "poll":

        if args.duration <= 0:
            self.perror(
                "--duration must be greater than 0."
            )

            logger.error(
                f"Invalid monitoring poll duration: "
                f"{args.duration}"
            )
            return

        if args.duration > MAX_MONITOR_POLL_DURATION_S:
            self.perror(
                f"--duration cannot exceed "
                f"{MAX_MONITOR_POLL_DURATION_S} seconds."
            )

            logger.error(
                "Monitoring poll duration exceeds maximum: "
                f"{args.duration}"
            )
            return

        if args.duration < args.interval:
            self.perror(
                "--duration must be greater than "
                "or equal to --interval."
            )

            logger.error(
                f"Monitoring poll duration "
                f"({args.duration}s) is shorter than "
                f"interval ({args.interval}s)"
            )
            return

        self.mon_orchestrator.poll(args)
        return


    # -----------------------------------------------------------------
    # save
    # -----------------------------------------------------------------

    if args.monitor_command == "save":

        if args.save_action == "start":
            self.mon_orchestrator.save_start(
                args
            )
            return

        if args.save_action == "stop":
            self.mon_orchestrator.save_stop(
                args
            )
            return

        if args.save_action == "status":
            self.mon_orchestrator.save_status(
                args
            )
            return


    # -----------------------------------------------------------------
    # sensors
    # -----------------------------------------------------------------

    if args.monitor_command == "sensors":

        if args.sensors_action == "status":
            self.mon_orchestrator.sensors_status(
                args
            )
            return


    # -----------------------------------------------------------------
    # orientation
    # -----------------------------------------------------------------

    if args.monitor_command == "orientation":
        self.mon_orchestrator.orientation(
            args
        )
        return


    # -----------------------------------------------------------------
    # show 
    # -----------------------------------------------------------------

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