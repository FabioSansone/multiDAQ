#!/usr/bin/env python3
# coding=utf-8

import argparse
import time
import zmq

from experimental.client.utils.logger import get_logger, LoggerManager
from experimental.client.communication.identity import ClientIdentity
from experimental.client.communication.control_manager import ControlPlaneManager


def main() -> int:
    LoggerManager.initialize(log_level="INFO", log_to_console=True)
    logger = get_logger("app")

    parser = argparse.ArgumentParser()
    parser.add_argument("--server-ip", type=str, default="127.0.0.1", help="Server IP")
    parser.add_argument("--control-port", type=int, default=8888, help="Control plane port")
    args = parser.parse_args()

    context = zmq.Context()

    identity = ClientIdentity()

    control_manager = ControlPlaneManager(
        context=context,
        server_ip=args.server_ip,
        identity=identity,
    )

    if not control_manager.start_connection(port=args.control_port):
        logger.error("Failed to connect control socket")
        context.term()
        return 1

    success = control_manager.handshake()

    if success:
        logger.info("Client control-plane handshake completed successfully")
    else:
        logger.error("Client control-plane handshake failed")
        if control_manager.socket is not None:
            control_manager.socket.setsockopt(zmq.LINGER, 0)
            control_manager.socket.close()
        context.term()
        return 1

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Client interrupted, shutting down")
    finally:
        if control_manager.socket is not None:
            control_manager.socket.setsockopt(zmq.LINGER, 0)
            control_manager.socket.close()
        context.term()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())