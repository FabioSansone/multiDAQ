import argparse
import time
import zmq
import threading

from client.utils.logger import get_logger, LoggerManager
from client.communication.identity import ClientIdentity
from client.core.client_runtime import ClientRunTime
from client.communication.control_manager import ControlPlaneManager
from client.communication.acquisition_manager import AcquisitionPlaneManager


def main() -> int:
    LoggerManager.initialize(log_level="INFO", log_to_console=True)
    logger = get_logger("app")

    parser = argparse.ArgumentParser()
    parser.add_argument("--server-ip", type=str, default="127.0.0.1", help="Server IP")
    parser.add_argument("--control-port", type=int, default=8888, help="Control plane port")
    parser.add_argument("--acq-port", type=int, default=8889, help="Acquisition plane port")
    parser.add_argument("--hv-port", type=str, default="/dev/ttyPS1", help="HV port")
    args = parser.parse_args()

    context = zmq.Context()
    identity = ClientIdentity()

    runtime = ClientRunTime(
        identity=identity,
        server_ip=args.server_ip,
        hv_port=args.hv_port,
    )

    control_manager = ControlPlaneManager(
        context=context,
        runtime=runtime,
    )

    acquisition_manager = AcquisitionPlaneManager(
        context=context,
        runtime=runtime,
    )

    control_command_thread = None
    acquisition_command_thread = None

    try:
        while True:
            control_manager.reconnect_requested.clear()
            control_manager.stop_listening.clear()
            control_manager.clear_queues()

            acquisition_manager.reconnect_requested.clear()
            acquisition_manager.stop_listening.clear()
            acquisition_manager.clear_queues()

            if not control_manager.start_connection(port=args.control_port):
                logger.error("Failed to connect control socket")
                time.sleep(1)
                continue

            if not control_manager.handshake(max_retries=None):
                logger.error("Control-plane handshake failed")
                time.sleep(1)
                continue

            logger.info("Client control-plane handshake completed successfully")

            if not acquisition_manager.start_connection(port=args.acq_port):
                logger.error("Failed to connect acquisition socket")
                time.sleep(1)
                continue

            if not acquisition_manager.handshake(max_retries=None):
                logger.error("Acquisition-plane handshake failed")
                time.sleep(1)
                continue

            logger.info("Client acquisition-plane handshake completed successfully")

            if not control_manager.start_listener():
                logger.error("Failed to start control listener")
                time.sleep(1)
                continue

            if not acquisition_manager.start_listener():
                logger.error("Failed to start acquisition listener")
                time.sleep(1)
                continue

            control_command_thread = threading.Thread(
                target=control_manager.handle_commands,
                daemon=True,
            )
            control_command_thread.start()

            acquisition_command_thread = threading.Thread(
                target=acquisition_manager.handle_commands,
                daemon=True,
            )
            acquisition_command_thread.start()

            while (
                not control_manager.reconnect_requested.is_set()
                and not acquisition_manager.reconnect_requested.is_set()
            ):
                time.sleep(0.5)

            logger.info("Reconnect requested after server shutdown")

            control_manager.close_connection()
            acquisition_manager.close_connection()

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Client interrupted, shutting down")

    finally:
        acquisition_manager.close_connection()
        control_manager.close_connection()
        runtime.close()
        context.term()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())