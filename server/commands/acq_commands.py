import argparse
import cmd2


########################
# ACQUISITION COMMANDS #
########################

acquisition_parser = argparse.ArgumentParser()

acquisition_subparsers = acquisition_parser.add_subparsers(
    dest="command",
    required=True,
)

start_parser = acquisition_subparsers.add_parser(
    "start",
    help="Start acquisition",
)

start_parser.add_argument(
    "--duration",
    type=float,
    default=None,
    help="Acquisition duration in seconds. If omitted, run until stopped.",
)

start_parser.add_argument(
    "--type",
    dest="acq_type",
    type=str,
    default="test",
    help="Acquisition type, e.g. test, pedestal, spe, gain, threshold.",
)

start_parser.add_argument(
    "--suffix",
    type=str,
    default="",
    help="Suffix for the output file name.",
)

start_parser.add_argument(
    "--run-id",
    type=str,
    default=None,
    help="Optional run ID. If omitted, an automatic acq_N folder is created.",
)

start_parser.add_argument(
    "--batch-id",
    type=str,
    default=None,
    help="Batch ID used to create the acquisition folder.",
)

start_parser.add_argument(
    "--force-compile",
    action="store_true",
    help="Force recompilation of evreceiver before starting acquisition.",
)

stop_parser = acquisition_subparsers.add_parser(
    "stop",
    help="Stop acquisition",
)


scan_ttp_parser = acquisition_subparsers.add_parser(
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
    default="test",
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

@cmd2.with_argparser(acquisition_parser)
@cmd2.with_category("Acquisition Commands")
def do_acquisition(self, args: argparse.Namespace) -> None:
    """Acquisition commands: acquisition start, acquisition stop."""

    if args.command == "start":
        self.acquisition_orchestrator.start(args)
        return

    if args.command == "stop":
        self.acquisition_orchestrator.stop()
        return
    
    if args.command == "scan_ttp":
        self.acquisition_orchestrator.scan_ttp(args)
        return
