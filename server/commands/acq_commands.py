import argparse
import cmd2
from server.core.server_state import command_guard, ServerFSM
from server.utils.logger import get_logger

logger = get_logger('acquisition_commands')

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




@cmd2.with_argparser(acquisition_parser)
@cmd2.with_category("Acquisition Commands")
@command_guard([ServerFSM.READY])
def do_acquisition(self, args: argparse.Namespace) -> None:
    """Acquisition commands: acquisition start, acquisition stop."""

    if args.command == "start":
        self.acquisition_orchestrator.start(args)
        return

    if args.command == "stop":
        self.acquisition_orchestrator.stop()
        return

    
