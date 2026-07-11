import argparse
import cmd2


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

scan_ttp_parser.add_argument(
    "--batch-id",
    type=str,
    default=None,
    help="Batch ID used to create the acquisition folder.",
)

scan_ttp_parser.add_argument(
    "--channels",
    type=str,
    default="all",
    help='Channels selected. Can be "all" or comma-separated list',
)

scan_ttp_parser.add_argument(
    "--force-compile",
    action="store_true",
    help="Force recompilation of evreceiver before starting acquisition.",
)


@cmd2.with_argparser(calibration_parser)
@cmd2.with_category("Calibration Commands")
def do_calibration(self, args: argparse.Namespace) -> None:
    """Calibration commands: calibration scan_ttp."""
    
    if args.command == "scan_ttp":
        self.calibration_orchestrator.scan_ttp(args)
        return