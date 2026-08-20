import argparse
import cmd2

from server.core.server_state import command_guard, acquisition_guard, ServerFSM, AcquisitionMode
from server.utils.logger import get_logger

logger = get_logger("calibration_commands")

########################
# CALIBRATION COMMANDS #
########################

calibration_parser = argparse.ArgumentParser()

calibration_subparsers = calibration_parser.add_subparsers(
    dest="command",
    required=True,
)

scan_ttp_parser = calibration_subparsers.add_parser(
    "scan_ttp",
    help="Run acquisitions while scanning RC register 10 (time-to-peak)",
)

scan_ttp_values_group = scan_ttp_parser.add_mutually_exclusive_group(required=True)

scan_ttp_values_group.add_argument(
    "--values",
    type=str,
    help="Comma-separated TTP values, e.g. 0,5,10,15",
)

scan_ttp_values_group.add_argument(
    "--range",
    nargs=3,
    metavar=("START", "STOP", "STEP"),
    type=int,
    help="TTP scan range: START STOP STEP, inclusive STOP",
)

scan_ttp_parser.add_argument(
    "--duration",
    type=float,
    required=True,
    help="Duration of each acquisition in seconds",
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
    help="Optional run ID. If omitted, automatic acq_N folder is created.",
)

# scan_ttp_parser.add_argument(
#     "--batch-id",
#     type=str,
#     default=None,
#     help="Batch ID used to create the acquisition folder.",
# )

scan_ttp_parser.add_argument(
    "--file-format",
    type=str,
    default="csv",
    help="Output file format: csv or bin. Default: csv",
)

scan_ttp_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma-separated list',
)

scan_ttp_parser.add_argument(
    "--input",
    dest="trigger_input",
    type=str,
    choices=["differential", "single-ended"],
    default="single-ended",   # <-- default diverso da acquisition (lì era differential)
    help=(
        "External trigger electrical input type for the TTP scan. "
        "Default: single-ended (typical for calibration)."
    ),
)

scan_ttp_parser.add_argument(
    "--polarity",
    type=str,
    choices=["default", "inverted"],
    default="default",
    help="External trigger polarity. Default: default.",
)

scan_ttp_parser.add_argument(
    "--window-ns",
    type=int,
    default=400,
    help="Trigger acquisition window in ns. Hardware resolution 5 ns. Default: 400 ns.",
)

scan_ttp_parser.add_argument(
    "--delay-ns",
    type=int,
    default=800,
    help="Delay before opening the trigger window, in ns. Hardware resolution 5 ns. Default: 800 ns.",
)

# scan_ttp_parser.add_argument(
#     "--force-compile",
#     action="store_true",
#     help="Force recompilation of evreceiver before starting acquisition.",
# )


@cmd2.with_argparser(calibration_parser)
@cmd2.with_category("Calibration Commands")
@command_guard([ServerFSM.READY])
@acquisition_guard([AcquisitionMode.TEST, AcquisitionMode.CALIBRATION]) #TEST should be deleted after testing the system
def do_calibration(self, args: argparse.Namespace) -> None:
    """Calibration commands: calibration scan_ttp ..."""

    if args.command == "scan_ttp":
        self.calibration_orchestrator.scan_ttp(args)
        return
    




recheck_parser = argparse.ArgumentParser()
recheck_parser.add_argument(
    "--multipmt-id",
    dest="multipmt_id",
    type=str,
    required=True,
    help="multiPMT identifier of the client whose calibration should be rechecked",
)


@cmd2.with_argparser(recheck_parser)
@cmd2.with_category("Calibration Commands")
@command_guard([ServerFSM.READY])
@acquisition_guard([AcquisitionMode.MULTIPMT])
def do_recheck_calibration(self, args: argparse.Namespace) -> None:
    """Re-check PMT serial calibration matching for a client, without changing acquisition mode: recheck_calibration --multipmt-id ..."""
    self.calibration_orchestrator.recheck_calibration(args)