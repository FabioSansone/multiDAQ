#!/usr/bin/env python3
# coding=utf-8

import argparse
import cmd2
import zmq

from server.utils.logger import get_logger, LoggerManager
from server.commands import app_commands, hv_commands, rc_commands
from server.communication.control_manager import ControlPlaneManager
from common.constants import ACQUISITION_MODES




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
        self.do_force = app_commands.do_force.__get__(self, Server)
        
        #HV COMMANDS#
        self.do_hv = hv_commands.do_hv.__get__(self, Server)
        
        #RC COMMANDS#
        self.do_rc = rc_commands.do_rc.__get__(self, Server)

        #EVENT MESSAGES MANAGER#
        self.handle_event = app_commands.handle_event.__get__(self, Server)
        self.control_manager.event_callback = self.handle_event
    

    def set_mode(self, new_mode: str) -> bool:

        new_mode = new_mode.lower()

        if new_mode not in ACQUISITION_MODES:
            self.logger.error(f"Invalid mode: {new_mode}")
            return False

        self.mode = new_mode
        self.control_manager.acq_mode = new_mode
        self.prompt = f"Server[{self.mode}]> "

        self.logger.info(f"Mode changed to {self.mode}")
        return True


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
    if mode_selected not in ACQUISITION_MODES:
        print("Unrecognized starting mode. Set to default mode: test.")
        mode_selected = 'test'

    context = zmq.Context()

    control_manager = ControlPlaneManager(
        context=context,
        num_multi_clients=1,
        acq_mode=mode_selected,
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