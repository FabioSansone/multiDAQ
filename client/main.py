#!/usr/bin/env python3
# coding=utf-8

import argparse
import time
import zmq

from client.utils.logger import get_logger, LoggerManager
from client.communication.identity import ClientIdentity
from client.communication.control_manager import ControlPlaneManager


def main() -> int:
    LoggerManager.initialize(log_level="INFO", log_to_console=True)
    logger = get_logger("app")

    parser = argparse.ArgumentParser()
    parser.add_argument("--server-ip", type=str, default="127.0.0.1", help="Server IP")
    parser.add_argument("--control-port", type=int, default=8888, help="Control plane port")
    parser.add_argument("--hv-port", type=str, default="/dev/ttyPS1", help="HV port")
    args = parser.parse_args()

    context = zmq.Context()
    identity = ClientIdentity()

    control_manager = ControlPlaneManager(
        context=context,
        server_ip=args.server_ip,
        identity=identity,
        hv_port=args.hv_port,
    )

    try:
        while True:
            control_manager.reconnect_requested.clear()
            control_manager.stop_listening.clear()
            control_manager.clear_queues()

            if not control_manager.start_connection(port=args.control_port):
                logger.error("Failed to connect control socket")
                time.sleep(1)
                continue

            if not control_manager.handshake(max_retries=None):
                logger.error("Handshake failed")
                time.sleep(1)
                continue

            logger.info("Client control-plane handshake completed successfully")

            if not control_manager.start_listener():
                logger.error("Failed to start control listener")
                time.sleep(1)
                continue

            control_manager.handle_commands()

            if control_manager.reconnect_requested.is_set():
                control_manager.close()
                logger.info("Reconnecting after server shutdown...")
                time.sleep(1)
                continue

            break

    except KeyboardInterrupt:
        logger.info("Client interrupted, shutting down")

    finally:
        control_manager.close()
        context.term()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())