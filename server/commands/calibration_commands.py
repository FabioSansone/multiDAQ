import argparse
import cmd2

from server.core.server_state import (
    command_guard,
    acquisition_guard,
    ServerFSM,
    AcquisitionMode,
)

from server.utils.logger import get_logger


logger = get_logger("calibration_commands")


########################
# CALIBRATION COMMANDS #
########################

calibration_parser = argparse.ArgumentParser()

calibration_subparsers = (
    calibration_parser.add_subparsers(
        dest="command",
        required=True,
    )
)


#
# ==========================
# OPTICAL HARDWARE
# ==========================
#

wheel_parser = calibration_subparsers.add_parser(
    "wheel",
    help=(
        "Move one optical filter wheel "
        "to a selected position."
    ),
)

wheel_parser.add_argument(
    "which_wheel",
    choices=["near", "far"],
    help=(
        "Filter wheel to move."
    ),
)

wheel_parser.add_argument(
    "position",
    type=int,
    help=(
        "Target filter-wheel position."
    ),
)


wheels_parser = calibration_subparsers.add_parser(
    "wheels",
    help=(
        "Move both optical filter wheels. "
        "Arguments are always NEAR then FAR."
    ),
)

wheels_parser.add_argument(
    "near_position",
    type=int,
    help=(
        "Target position for the near filter wheel."
    ),
)

wheels_parser.add_argument(
    "far_position",
    type=int,
    help=(
        "Target position for the far filter wheel."
    ),
)


polarizer_parser = calibration_subparsers.add_parser(
    "polarizer",
    help=(
        "Move the optical polarizer "
        "to the selected position."
    ),
)

polarizer_parser.add_argument(
    "position",
    type=float,
    help=(
        "Target polarizer position."
    ),
)


#
# ==========================
# TTP SCAN
# ==========================
#

scan_ttp_parser = calibration_subparsers.add_parser(
    "scan_ttp",
    help=(
        "Run calibration acquisitions while scanning "
        "RC register 10 (time-to-peak)"
    ),
)

scan_ttp_values_group = (
    scan_ttp_parser
    .add_mutually_exclusive_group(
        required=True
    )
)

scan_ttp_values_group.add_argument(
    "--values",
    type=str,
    help=(
        "Comma-separated TTP values, "
        "e.g. 0,5,10,15"
    ),
)

scan_ttp_values_group.add_argument(
    "--range",
    nargs=3,
    metavar=("START", "STOP", "STEP"),
    type=int,
    help=(
        "TTP scan range: START STOP STEP, "
        "inclusive STOP"
    ),
)

scan_ttp_parser.add_argument(
    "--duration",
    type=float,
    required=True,
    help=(
        "Duration of each acquisition "
        "point in seconds"
    ),
)

scan_ttp_parser.add_argument(
    "--execution-mode",
    choices=["safe", "fast"],
    default="safe",
    help=(
        "Calibration execution policy. "
        "SAFE revalidates hardware before every point; "
        "FAST validates once before the scan. "
        "Default: safe."
    ),
)

scan_ttp_parser.add_argument(
    "--type",
    dest="acq_type",
    type=str,
    default="ttp",
    help="Acquisition type folder name",
)

scan_ttp_parser.add_argument(
    "--suffix",
    type=str,
    default="ttp",
    help="Base suffix for output files",
)

scan_ttp_parser.add_argument(
    "--run-id",
    type=str,
    default=None,
    help=(
        "Optional run ID. If omitted, "
        "automatic acquisition folder is created."
    ),
)

scan_ttp_parser.add_argument(
    "--file-format",
    choices=["csv", "bin"],
    default="csv",
    help="Output file format. Default: csv",
)

scan_ttp_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help=(
        'Channels selected. Can be "all" '
        "or a comma-separated list."
    ),
)


#
# Target selection.
#
target_group = (
    scan_ttp_parser
    .add_mutually_exclusive_group()
)

target_group.add_argument(
    "--multipmt-id",
    type=str,
    default=None,
    help="Target client by multiPMT ID.",
)

target_group.add_argument(
    "--batch-id",
    type=str,
    default=None,
    help="Target client by batch ID.",
)

target_group.add_argument(
    "--all-clients",
    action="store_true",
    help=(
        "Run the calibration on all eligible clients. "
        "This is also the default when no target "
        "selector is specified."
    ),
)


#
# Trigger override.
#
# IMPORTANT:
# None means "no explicit override".
#
scan_ttp_parser.add_argument(
    "--input",
    dest="trigger_input",
    choices=[
        "differential",
        "single-ended",
    ],
    default=None,
    help=(
        "Optional external-trigger electrical input. "
        "In CALIBRATION mode, omitted values use "
        "calibration defaults. In TEST/MULTIPMT, "
        "omitting all trigger options preserves the "
        "current hardware configuration."
    ),
)

scan_ttp_parser.add_argument(
    "--polarity",
    choices=[
        "default",
        "inverted",
    ],
    default=None,
    help="Optional external-trigger polarity override.",
)

scan_ttp_parser.add_argument(
    "--window-ns",
    type=int,
    default=None,
    help=(
        "Optional acquisition-window override in ns. "
        "Hardware resolution: 5 ns."
    ),
)

scan_ttp_parser.add_argument(
    "--delay-ns",
    type=int,
    default=None,
    help=(
        "Optional trigger-delay override in ns. "
        "Hardware resolution: 5 ns."
    ),
)


#
# ==========================
# HV CALIBRATION
# ==========================
#

hv_calibration_parser = calibration_subparsers.add_parser(
    "hv",
    help=(
        "Run the HV voltage calibration "
        "on the selected channels."
    ),
)

hv_calibration_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help=(
        'Channels selected. Can be "all" '
        "or a comma-separated list."
    ),
)


#
# Target selection.
#
hv_target_group = (
    hv_calibration_parser
    .add_mutually_exclusive_group()
)

hv_target_group.add_argument(
    "--multipmt-id",
    type=str,
    default=None,
    help="Target client by multiPMT ID.",
)

hv_target_group.add_argument(
    "--batch-id",
    type=str,
    default=None,
    help="Target client by batch ID.",
)

hv_target_group.add_argument(
    "--all-clients",
    action="store_true",
    help=(
        "Run the HV calibration on all eligible clients. "
        "This is also the default when no target "
        "selector is specified."
    ),
)


#
# ==========================
# STOP
# ==========================
#

calibration_subparsers.add_parser(
    "stop",
    help=(
        "Abort the active calibration and restore "
        "the hardware state."
    ),
)


@cmd2.with_argparser(calibration_parser)
@cmd2.with_category("Calibration Commands")
@command_guard([
    ServerFSM.READY,
    ServerFSM.CALIBRATING,
])
@acquisition_guard([
    AcquisitionMode.TEST,
    AcquisitionMode.CALIBRATION,
    AcquisitionMode.MULTIPMT,
])
def do_calibration(
    self,
    args: argparse.Namespace,
) -> None:
    """Calibration lifecycle commands."""

    if args.command == "scan_ttp":

        if (
            self.server_state.get_server_state()
            != ServerFSM.READY
        ):
            self.poutput(
                "Cannot start TTP calibration: "
                "another operation is already active."
            )
            return

        self.calibration_orchestrator.scan_ttp(
            args
        )

        return

    if args.command == "hv":

        if (
            self.server_state.get_server_state()
            != ServerFSM.READY
        ):
            self.poutput(
                "Cannot start HV calibration: "
                "another operation is already active."
            )
            return

        self.calibration_orchestrator.calibrate_hv(
            args
        )

        return

    if args.command == "stop":

        if (
            self.server_state.get_server_state()
            != ServerFSM.CALIBRATING
        ):
            self.poutput(
                "No calibration is currently running."
            )
            return

        self.calibration_orchestrator.request_calibration_stop()

        return


    if args.command == "wheel":
        if (
            self.server_state.get_server_state()
            != ServerFSM.READY
        ):
            self.poutput(
                "Cannot move filter wheel: "
                "another operation is active."
            )
            return

        if not self.optical_instrument_service.move_wheel(
            position=args.position,
            which_wheel=args.which_wheel,
        ):
            self.poutput(
                f"Failed to move "
                f"{args.which_wheel} filter wheel."
            )
            return

        self.poutput(
            f"{args.which_wheel.capitalize()} "
            f"filter wheel moved to "
            f"position {args.position}."
        )

        return


    if args.command == "wheels":
        if (
            self.server_state.get_server_state()
            != ServerFSM.READY
        ):
            self.poutput(
                "Cannot move filter wheels: "
                "another operation is active."
            )
            return

        near_ok = (
            self.optical_instrument_service.move_wheel(
                position=args.near_position,
                which_wheel="near",
            )
        )

        if not near_ok:
            self.poutput(
                "Failed to move near filter wheel."
            )
            return

        far_ok = (
            self.optical_instrument_service.move_wheel(
                position=args.far_position,
                which_wheel="far",
            )
        )

        if not far_ok:
            self.poutput(
                "Near filter wheel moved successfully, "
                "but far filter wheel movement failed."
            )
            return

        self.poutput(
            "Filter wheels moved successfully: "
            f"near={args.near_position}, "
            f"far={args.far_position}."
        )

        return


    if args.command == "polarizer":
        if (
            self.server_state.get_server_state()
            != ServerFSM.READY
        ):
            self.poutput(
                "Cannot move polarizer: "
                "another operation is active."
            )
            return

        if not self.optical_instrument_service.move_polarizer(
            position=args.position
        ):
            self.poutput(
                "Failed to move polarizer."
            )
            return

        self.poutput(
            "Polarizer moved successfully: "
            f"position={args.position}."
        )

        return