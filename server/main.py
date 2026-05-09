#!/usr/bin/env python3
# coding=utf-8

import argparse
import cmd2
import zmq

from server.utils.logger import get_logger, LoggerManager
from server.commands import app_commands, hv_commands
from server.communication.control_manager import ControlPlaneManager

POSSIBLE_MODES = ['test', 'calibration', 'multipmt']


class Server(cmd2.Cmd):
    "A terminal application to switch and interact with different multiPMTs"

    def __init__(self, acquisition_mode: str, control_manager: ControlPlaneManager) -> None:
        super().__init__(allow_cli_args=False)

        self.intro = "Welcome to the control interface for the multiPMTs. Type ? or help to list commands."
        self.logger = get_logger("app")
        self.mode = acquisition_mode
        self.control_manager = control_manager

        self.prompt = f"Server[{self.mode}]> "

        
        #GENERIC COMMANDS#
        self.do_change_mode = app_commands.do_change_mode.__get__(self, Server)
        self.do_connect = app_commands.do_connect.__get__(self, Server)
        self.do_quit = app_commands.do_quit.__get__(self, Server)
        
        #HV COMMANDS#
        self.do_set_common = hv_commands.do_set_common.__get__(self, Server)

        self.logger.info(f"Server started in {self.mode} mode")


def main() -> int:

    LoggerManager.initialize(log_level="INFO", log_to_console=True)

    server_parser = argparse.ArgumentParser()
    server_parser.add_argument(
        "start_mode",
        nargs='?',
        type=str,
        help="Acquisition Mode (test (no HV); calibration, multiPMT)",
        default="test"
    )
    args = server_parser.parse_args()

    mode_selected = args.start_mode.lower()
    if mode_selected not in POSSIBLE_MODES:
        print("Unrecognized starting mode. Set to default mode: test.")
        mode_selected = 'test'

    context = zmq.Context()

    control_manager = ControlPlaneManager(
        context=context,
        num_multi_clients=1,
    )

    app = Server(acquisition_mode=mode_selected, control_manager=control_manager)

    try:
        app.cmdloop()
    except KeyboardInterrupt:
        app.poutput("\nShutting down...")
    finally:
        if control_manager.socket is not None:
            control_manager.clear_queues()
            control_manager.close_connection()
        context.term()

    return 0


if __name__ == '__main__':
    
    raise SystemExit(main())