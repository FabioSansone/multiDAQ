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

#
# ==========================
# PEDESTAL CALIBRATION
# ==========================
#

pedestal_parser = calibration_subparsers.add_parser(
    "pedestal",
    help=(
        "Acquire the electronic pedestal "
        "for the selected PMT channels."
    ),
)


pedestal_parser.add_argument(
    "--duration",
    type=float,
    default=60.0,
    help=(
        "Pedestal acquisition duration in seconds. "
        "Default: 60."
    ),
)


pedestal_parser.add_argument(
    "--type",
    dest="acq_type",
    type=str,
    default="pedestal",
    help=(
        "Acquisition type folder name. "
        "Default: pedestal."
    ),
)


pedestal_parser.add_argument(
    "--suffix",
    type=str,
    default="pedestal",
    help=(
        "Output file suffix. "
        "Default: pedestal."
    ),
)


pedestal_parser.add_argument(
    "--run-id",
    type=str,
    default=None,
    help=(
        "Optional run ID. If omitted, "
        "automatic acquisition folder is created."
    ),
)


pedestal_parser.add_argument(
    "--file-format",
    choices=["csv", "bin"],
    default="csv",
    help=(
        "Output file format. Default: csv."
    ),
)


pedestal_parser.add_argument(
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

pedestal_target_group = (
    pedestal_parser
    .add_mutually_exclusive_group()
)


pedestal_target_group.add_argument(
    "--multipmt-id",
    type=str,
    default=None,
    help="Target client by multiPMT ID.",
)


pedestal_target_group.add_argument(
    "--batch-id",
    type=str,
    default=None,
    help="Target client by batch ID.",
)


pedestal_target_group.add_argument(
    "--all-clients",
    action="store_true",
    help=(
        "Run the pedestal calibration on all eligible clients. "
        "This is also the default when no target "
        "selector is specified."
    ),
)


#
# ==========================
# SPE CALIBRATION
# ==========================
#

spe_parser = calibration_subparsers.add_parser(
    "spe",
    help="Acquire Single Photoelectron (SPE) spectra using optical instruments and HV (calibration mode only).",
)

# Acquisition configuration.
spe_parser.add_argument("--duration", type=float, default=1800.0, help="SPE acquisition duration in seconds. Default: 1800 (30 minutes).")
spe_parser.add_argument("--type", dest="acq_type", type=str, default="spe", help="Acquisition type folder name. Default: spe.")
spe_parser.add_argument("--suffix", type=str, default="spe", help="Output file suffix. Default: spe.")
spe_parser.add_argument("--run-id", type=str, default=None, help="Optional run ID. If omitted, an automatic ID is generated from the SPE configuration.")
spe_parser.add_argument("--file-format", choices=["csv", "bin"], default="csv", help="Output file format. Default: csv.")
spe_parser.add_argument("--channels", type=str, default="all", help='PMT channels selected. Can be "all" or a comma-separated list.')

# Optical hardware configuration.
spe_parser.add_argument("--near-wheel", type=int, default=10, help="Near filter-wheel position. Default: 10.")
spe_parser.add_argument("--far-wheel", type=int, default=8, help="Far filter-wheel position. Default: 8.")
spe_parser.add_argument("--polarizer", type=float, default=50.0, help="Polarizer position in degrees. Default: 50.")

# HV configuration.
spe_parser.add_argument("--voltage", type=int, default=1450, help="Common PMT voltage in V. Default: 1450.")
spe_parser.add_argument("--threshold", type=float, default=45.0, help="Common PMT threshold. Default: 45.")

# Trigger configuration: the SPE always uses the external trigger.
spe_parser.add_argument("--input", dest="trigger_input", choices=["differential", "single-ended"], default="single-ended", help="External trigger electrical input. Default: single-ended.")
spe_parser.add_argument("--polarity", choices=["default", "inverted"], default="default", help="External trigger polarity. Default: default.")
spe_parser.add_argument("--delay-ns", type=int, default=800, help="External trigger delay in ns. Hardware resolution: 5 ns. Default: 800.")
spe_parser.add_argument("--window-ns", type=int, default=400, help="Acquisition window in ns. Hardware resolution: 5 ns. Default: 400.")

# Target selection.
spe_target_group = spe_parser.add_mutually_exclusive_group()
spe_target_group.add_argument("--multipmt-id", type=str, default=None, help="Target client by multiPMT ID.")
spe_target_group.add_argument("--batch-id", type=str, default=None, help="Target client by batch ID.")
spe_target_group.add_argument("--all-clients", action="store_true", help="Run the SPE calibration on all eligible clients. This is also the default when no target selector is specified.")


#
# ==========================
# GAIN CALIBRATION
# ==========================
#

gain_parser = calibration_subparsers.add_parser(
    "gain",
    help="Acquire PMT gain spectra while scanning the HV voltage (calibration mode only).",
)

# Voltage scan.
gain_parser.add_argument("--start", type=int, default=950, help="Initial PMT voltage in V. Default: 950.")
gain_parser.add_argument("--stop", type=int, default=1450, help="Final voltage limit in V, inclusive if reached. Default: 1450.")
gain_parser.add_argument("--step", type=int, default=50, help="Voltage step in V. Default: 50.")
gain_parser.add_argument("--threshold", type=float, default=400.0, help="Common PMT threshold. Default: 400.")

# Acquisition configuration.
gain_parser.add_argument("--duration", type=float, default=60.0, help="Acquisition duration of each point in seconds. Default: 60.")
gain_parser.add_argument("--type", dest="acq_type", type=str, default="gain", help="Acquisition type folder name. Default: gain.")
gain_parser.add_argument("--suffix", type=str, default="gain", help="Output file suffix (the voltage is appended). Default: gain.")
gain_parser.add_argument("--run-id", type=str, default=None, help="Optional run ID. If omitted, an automatic ID is generated from the GAIN configuration.")
gain_parser.add_argument("--file-format", choices=["csv", "bin"], default="csv", help="Output file format. Default: csv.")
gain_parser.add_argument("--channels", type=str, default="all", help='PMT channels selected. Can be "all" or a comma-separated list.')

# Optical hardware configuration (fixed for the whole run).
gain_parser.add_argument("--near-wheel", type=int, default=9, help="Near filter-wheel position. Default: 9.")
gain_parser.add_argument("--far-wheel", type=int, default=9, help="Far filter-wheel position. Default: 9.")
gain_parser.add_argument("--polarizer", type=float, default=20.0, help="Polarizer position in degrees. Default: 20.")

# Trigger configuration: the GAIN always uses the external trigger.
gain_parser.add_argument("--input", dest="trigger_input", choices=["differential", "single-ended"], default="single-ended", help="External trigger electrical input. Default: single-ended.")
gain_parser.add_argument("--polarity", choices=["default", "inverted"], default="default", help="External trigger polarity. Default: default.")
gain_parser.add_argument("--delay-ns", type=int, default=800, help="External trigger delay in ns. Hardware resolution: 5 ns. Default: 800.")
gain_parser.add_argument("--window-ns", type=int, default=400, help="Acquisition window in ns. Hardware resolution: 5 ns. Default: 400.")

# Target selection.
gain_target_group = gain_parser.add_mutually_exclusive_group()
gain_target_group.add_argument("--multipmt-id", type=str, default=None, help="Target client by multiPMT ID.")
gain_target_group.add_argument("--batch-id", type=str, default=None, help="Target client by batch ID.")
gain_target_group.add_argument("--all-clients", action="store_true", help="Run the GAIN calibration on all eligible clients. This is also the default when no target selector is specified.")


@cmd2.with_argparser(calibration_parser)
@cmd2.with_category("Calibration Commands")
@command_guard([ServerFSM.READY])
@acquisition_guard([AcquisitionMode.TEST, AcquisitionMode.CALIBRATION, AcquisitionMode.MULTIPMT])
def do_calibration(self, args: argparse.Namespace) -> None:
    """Calibration commands: calibration scan_ttp ..., calibration pedestal ..., calibration spe ..., calibration gain ..."""

    if args.command == "pedestal":
        self.calibration_orchestrator.pedestal(args)
        return

    if args.command == "spe":
        self.calibration_orchestrator.spe(args)
        return

    if args.command == "gain":
        self.calibration_orchestrator.gain(args)
        return

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


@cmd2.with_category("Calibration Commands")
@command_guard([
    ServerFSM.CALIBRATING,
    ServerFSM.CALIBRATION_SEQUENCE,
    ServerFSM.CALIBRATION_FINALIZING,
])
def do_stop_calibration(self, _) -> None:
    """Stop the running calibration. The hardware is restored anyway; a restore already in progress is not interrupted."""

    if self.calibration_engine.request_stop():
        self.poutput("Calibration stop requested; the hardware restore runs in the background.")
    else:
        self.poutput("The calibration cannot be stopped now (see the log).")
