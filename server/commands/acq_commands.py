import argparse
import cmd2
from typing import List
import time
import threading
from server.utils.logger import get_logger
from common.message_handler import Channel


logger = get_logger("acq_commands")

#######################
# ACQUISITION HELPERS #
#######################

def _hv_to_user_channels(channels: List[int]) -> List[int]:
    return [ch - 1 for ch in channels]


def _send_hv_command(self, client_id: bytes, command: str, payload: dict, timeout_s: float):
    hv_command = self.control_manager.message_handler.create_command(
        channel=Channel.HV,
        command=command,
        payload=payload,
        sender="server",
    )

    self.control_manager.queue_message(client_id, hv_command)

    return self.control_manager.wait_for_reply(
        client_id=client_id,
        in_reply_to=hv_command.request_id,
        timeout_s=timeout_s,
    )



def _send_rc_command(self, client_id: bytes, command: str, payload: dict, timeout_s: float):
    rc_command = self.control_manager.message_handler.create_command(
        channel=Channel.RC,
        command=command,
        payload=payload,
        sender="server",
    )

    self.control_manager.queue_message(client_id, rc_command)

    return self.control_manager.wait_for_reply(
        client_id=client_id,
        in_reply_to=rc_command.request_id,
        timeout_s=timeout_s,
    )


def _read_rc_register(self, client_id: bytes, address: int) -> int | None:
    client_name = client_id.decode(errors="ignore")

    reply, reason = _send_rc_command(
        self=self,
        client_id=client_id,
        command="rc_read_register",
        payload={"address": address},
        timeout_s=35.0,
    )

    if reply is None:
        logger.error(f"RC read register {address} failed for client {client_name}: {reason}")
        self.poutput(f"Client {client_name}: no reply while reading RC register {address} ({reason})")
        return None

    payload = reply.payload or {}
    status = payload.get("status")
    result = payload.get("result", {})
    error = payload.get("error")

    if status != "ok":
        logger.error(f"RC read register {address} failed for client {client_name}: {error}")
        self.poutput(f"Client {client_name}: failed to read RC register {address}")
        if error:
            self.poutput(f"Client {client_name}: error: {error}")
        return None

    return result.get("value")


def _write_rc_register(self, client_id: bytes, address: int, value: int) -> bool:
    client_name = client_id.decode(errors="ignore")

    reply, reason = _send_rc_command(
        self=self,
        client_id=client_id,
        command="rc_write_register",
        payload={
            "address": address,
            "value": value,
        },
        timeout_s=35.0,
    )

    if reply is None:
        logger.error(f"RC write register {address} failed for client {client_name}: {reason}")
        self.poutput(f"Client {client_name}: no reply while writing RC register {address} ({reason})")
        return False

    payload = reply.payload or {}
    status = payload.get("status")
    error = payload.get("error")

    if status != "ok":
        logger.error(f"RC write register {address} failed for client {client_name}: {error}")
        self.poutput(f"Client {client_name}: failed to write RC register {address}")
        if error:
            self.poutput(f"Client {client_name}: error: {error}")
        return False

    return True


def _enable_hv_channels(self) -> dict[bytes, List[int]]:
    """
    Synchronize HV status and switch ON all OK+OFF channels.

    Returns:
        dict[client_id, list[int]]
        Channels are returned in external numbering: 0..6.
        These are the channels that can be passed to RC register 19.
    """

    client_ids = self.control_manager.server_state.list_connected_clients()

    if not client_ids:
        self.poutput("No connected clients.")
        return {}

    enabled_channels_by_client = {}

    for client_id in client_ids:
        client_name = client_id.decode(errors="ignore")

        sync_reply, reason = _send_hv_command(
            self=self,
            client_id=client_id,
            command="set_hv_sync",
            payload={"channels": "all"},
            timeout_s=90.0,
        )

        if sync_reply is None:
            logger.error(f"HV sync failed for client {client_name}: {reason}")
            self.poutput(f"Client {client_name}: HV sync failed ({reason})")
            continue

        sync_payload = sync_reply.payload or {}
        sync_result = sync_payload.get("result", {})
        sync_error = sync_payload.get("error")

        if sync_error:
            logger.error(f"HV sync error from client {client_name}: {sync_error}")
            self.poutput(f"Client {client_name}: HV sync error: {sync_error}")
            continue

        ok_channels = set(sync_result.get("ok_channels", []))
        on_channels = set(sync_result.get("on_channels", []))
        off_channels = set(sync_result.get("off_channels", []))
        bad_channels = set(sync_result.get("bad_channels", []))

        ok_on_channels = sorted(ok_channels & on_channels)
        ok_off_channels = sorted(ok_channels & off_channels)

        if bad_channels:
            self.poutput(
                f"Client {client_name}: BAD HV channels excluded: "
                f"{_hv_to_user_channels(sorted(bad_channels))}"
            )

        if not ok_channels:
            self.poutput(f"Client {client_name}: no OK HV channels available.")
            continue

        if ok_off_channels:
            user_ok_off_channels = _hv_to_user_channels(ok_off_channels)

            self.poutput(
                f"Client {client_name}: switching ON HV channels "
                f"{user_ok_off_channels} and waiting for UP state..."
            )

            on_reply, reason = _send_hv_command(
                self=self,
                client_id=client_id,
                command="hv_on_and_wait",
                payload={"channels": user_ok_off_channels},
                timeout_s=300.0,
            )

            if on_reply is None:
                logger.error(
                    f"HV on_and_wait failed for client {client_name}: {reason}"
                )
                self.poutput(
                    f"Client {client_name}: HV on_and_wait failed ({reason})"
                )
                continue

            on_payload = on_reply.payload or {}
            on_result = on_payload.get("result", {})
            on_error = on_payload.get("error")

            if on_error:
                logger.error(
                    f"HV on_and_wait error from client {client_name}: {on_error}"
                )
                self.poutput(
                    f"Client {client_name}: HV on_and_wait error: {on_error}"
                )

            successful_on = set(
                on_result.get("successful_channels", [])
                or on_result.get("on_channels", [])
                or on_result.get("up_channels", [])
            )

            failed_on = set(on_result.get("failed_channels", []))
            bad_after_on = set(on_result.get("bad_channels", []))

            if successful_on:
                self.poutput(
                    f"Client {client_name}: HV channels UP: "
                    f"{_hv_to_user_channels(sorted(successful_on))}"
                )

            if failed_on:
                self.poutput(
                    f"Client {client_name}: HV channels failed to go UP: "
                    f"{_hv_to_user_channels(sorted(failed_on))}"
                )

            if bad_after_on:
                self.poutput(
                    f"Client {client_name}: HV channels moved to BAD: "
                    f"{_hv_to_user_channels(sorted(bad_after_on))}"
                )

            final_hv_channels = sorted(
                set(ok_on_channels) | successful_on
            )

        else:
            final_hv_channels = ok_on_channels
            self.poutput(
                f"Client {client_name}: all usable OK channels already ON."
            )

        final_user_channels = _hv_to_user_channels(final_hv_channels)

        if not final_user_channels:
            self.poutput(
                f"Client {client_name}: no HV channels available for acquisition."
            )
            continue

        enabled_channels_by_client[client_id] = final_user_channels

        self.poutput(
            f"Client {client_name}: HV channels ready for acquisition: "
            f"{final_user_channels}"
        )

    return enabled_channels_by_client

def _enable_rc_channels(self, client_id: bytes, channels: List[int]) -> bool:
    client_name = client_id.decode(errors="ignore")

    if not channels:
        self.poutput(f"Client {client_name}: no RC channels selected for acquisition.")
        logger.warning(f"Cannot enable RC channels for client {client_name}: empty channel list")
        return False

    register_address = 19
    register_value = 0

    for ch in channels:
        if ch < 0 or ch >= 7:
            self.poutput(f"Client {client_name}: invalid RC channel: {ch}")
            logger.error(f"Invalid RC channel requested for acquisition: {ch}")
            return False

        register_value |= 1 << ch

    ok = _write_rc_register(
        self=self,
        client_id=client_id,
        address=register_address,
        value=register_value,
    )

    if not ok:
        self.poutput(f"Client {client_name}: failed to enable RC channels.")
        return False

    self.poutput(
        f"Client {client_name}: RC register {register_address} written with "
        f"value {register_value} (enabled channels: {channels})"
    )

    return True


def _disable_rc_channels(self) -> None:
    client_ids = self.control_manager.server_state.list_connected_clients()

    for client_id in client_ids:
        client_name = client_id.decode(errors="ignore")

        ok = _write_rc_register(
            self=self,
            client_id=client_id,
            address=19,
            value=0,
        )

        if ok:
            self.poutput(f"Client {client_name}: RC acquisition channels disabled.")
        else:
            self.poutput(f"Client {client_name}: failed to disable RC acquisition channels.")



def _flush_client(self, client_id: bytes) -> bool:
    client_name = client_id.decode(errors="ignore")

    read_prev = _read_rc_register(
        self=self,
        client_id=client_id,
        address=15,
    )

    if read_prev is None:
        self.poutput(f"Client {client_name}: flush skipped, no valid read from register 15.")
        return False

    if not _write_rc_register(
        self=self,
        client_id=client_id,
        address=15,
        value=read_prev + 32,
    ):
        self.poutput(f"Client {client_name}: flush failed while writing register 15.")
        return False

    time.sleep(2.0)

    read_now = _read_rc_register(
        self=self,
        client_id=client_id,
        address=15,
    )

    if read_now is None:
        self.poutput(f"Client {client_name}: flush failed, missing final read.")
        return False

    if read_now - read_prev - 32 == 64:
        self.poutput(f"Client {client_name}: data flushing ended successfully.")

        _write_rc_register(
            self=self,
            client_id=client_id,
            address=15,
            value=read_prev,
        )

        return True

    self.poutput(f"Client {client_name}: flush error. Please check.")
    logger.error(
        f"Flush check failed for client {client_name}: "
        f"prev={read_prev}, now={read_now}"
    )

    return False


def _flush_clients(self, client_ids: List[bytes]) -> None:
    time.sleep(10.0)

    for client_id in client_ids:
        _flush_client(
            self=self,
            client_id=client_id,
        )
        
        
def _get_test_rc_channels(self, client_id: bytes) -> List[int]:
    """
    Return RC channels to enable in test mode.

    In test mode no HV power/configuration command is allowed.
    If HVService is available in monitor-only mode, use HV OK channels only.
    If HVService is unavailable, fall back to all RC channels for bench tests.
    """

    client_name = client_id.decode(errors="ignore")

    sync_reply, reason = _send_hv_command(
        self=self,
        client_id=client_id,
        command="set_hv_sync",
        payload={"channels": "all"},
        timeout_s=90.0,
    )

    if sync_reply is None:
        logger.warning(
            f"HV sync unavailable in test mode for client {client_name}: {reason}. "
            "Falling back to all RC channels."
        )
        self.poutput(
            f"Client {client_name}: HV sync unavailable in test mode; "
            "enabling all RC channels."
        )
        return list(range(7))

    sync_payload = sync_reply.payload or {}
    sync_result = sync_payload.get("result", {})
    sync_error = sync_payload.get("error")

    if sync_error:
        logger.warning(
            f"HV sync error in test mode for client {client_name}: {sync_error}. "
            "Falling back to all RC channels."
        )
        self.poutput(
            f"Client {client_name}: HV sync error in test mode; "
            "enabling all RC channels."
        )
        return list(range(7))

    ok_channels = sorted(set(sync_result.get("ok_channels", [])))
    bad_channels = sorted(set(sync_result.get("bad_channels", [])))

    if bad_channels:
        self.poutput(
            f"Client {client_name}: BAD HV/FEB channels excluded in test mode: "
            f"{_hv_to_user_channels(bad_channels)}"
        )

    rc_channels = _hv_to_user_channels(ok_channels)

    if not rc_channels:
        logger.warning(
            f"No OK HV/FEB channels found in test mode for client {client_name}. "
            "Falling back to all RC channels."
        )
        self.poutput(
            f"Client {client_name}: no OK HV/FEB channels found in test mode; "
            "enabling all RC channels."
        )
        return list(range(7))

    self.poutput(
        f"Client {client_name}: test mode RC channels from HV/FEB presence: "
        f"{rc_channels}"
    )

    return rc_channels


def _ensure_acquisition_state(self) -> None:
    if not hasattr(self, "_acq_finalize_lock"):
        self._acq_finalize_lock = threading.Lock()

    if not hasattr(self, "_acq_finalized"):
        self._acq_finalized = False


def _reset_acquisition_state(self) -> None:
    _ensure_acquisition_state(self=self)

    with self._acq_finalize_lock:
        self._acq_finalized = False


def _finalize_acquisition(self, client_ids: List[bytes], reason: str) -> None:
    _ensure_acquisition_state(self=self)

    with self._acq_finalize_lock:
        if self._acq_finalized:
            logger.info(f"Acquisition finalization already completed. Ignoring request: {reason}")
            return

        self._acq_finalized = True

        self.poutput(f"Finalizing acquisition: {reason}")
        logger.info(f"Finalizing acquisition: {reason}")

        if self.data_receiver_service.is_running():
            stopped = self.data_receiver_service.stop()

            if stopped:
                self.poutput("Acquisition stopped.")
            else:
                self.poutput("Failed to stop acquisition.")
                return

        else:
            self.poutput("Data receiver is not running. Continuing finalization.")

        _disable_rc_channels(self=self)

        if not client_ids:
            self.poutput("No connected clients. Final flush skipped.")
            return

        self.poutput("Starting final flush receiver...")

        flush_info = self.data_receiver_service.start_flush(
            duration=20.0,
        )

        if flush_info is None:
            self.poutput("Failed to start final flush receiver.")
            logger.error("Failed to start final flush receiver")
            return

        flush_thread = threading.Thread(
            target=_flush_clients,
            args=(self, client_ids),
            daemon=True,
        )

        flush_thread.start()

        while self.data_receiver_service.is_running():
            time.sleep(0.5)

        flush_thread.join(timeout=10.0)
        
        self.data_receiver_service.clear_finalizing()

        self.poutput("Final flush completed.")
        logger.info("Final flush completed")


def _watch_acquisition_completion(self, client_ids: List[bytes]) -> None:
    while self.data_receiver_service.is_running():
        time.sleep(0.5)

    _finalize_acquisition(
        self=self,
        client_ids=client_ids,
        reason="data receiver completed its configured duration",
    )

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
def do_acquisition(self, args: argparse.Namespace) -> None:
    """Acquisition commands: acquisition start, acquisition stop."""

    if args.command == "start":
        self.poutput("Acquisition start command received.")

        client_ids = self.control_manager.server_state.list_connected_clients()

        if not client_ids:
            self.poutput("No connected clients.")
            return

        if self.data_receiver_service.is_running():
            self.poutput("Data receiver is already running.")
            return

        _reset_acquisition_state(self=self)

        rc_ready_clients = []

        if self.mode == "test":
            self.poutput(
                "Server is in test mode: skipping HV power commands. "
                "RC channels will be enabled from HV/FEB presence if available."
            )

            for client_id in client_ids:
                client_name = client_id.decode(errors="ignore")
                channels = _get_test_rc_channels(
                    self=self,
                    client_id=client_id,
                )

                rc_ok = _enable_rc_channels(
                    self=self,
                    client_id=client_id,
                    channels=channels,
                )

                if not rc_ok:
                    self.poutput(
                        f"Client {client_name}: RC channel enable failed. "
                        "Skipping acquisition start."
                    )
                    continue

                rc_ready_clients.append(client_id)

        else:
            enabled_channels_by_client = _enable_hv_channels(self=self)

            if not enabled_channels_by_client:
                self.poutput("No HV channels available for acquisition.")
                return

            for client_id, channels in enabled_channels_by_client.items():
                client_name = client_id.decode(errors="ignore")

                if not channels:
                    self.poutput(
                        f"Client {client_name}: no channels available for RC enable."
                    )
                    continue

                rc_ok = _enable_rc_channels(
                    self=self,
                    client_id=client_id,
                    channels=channels,
                )

                if not rc_ok:
                    self.poutput(
                        f"Client {client_name}: RC channel enable failed. "
                        "Skipping acquisition start."
                    )
                    continue

                rc_ready_clients.append(client_id)

        if not rc_ready_clients:
            self.poutput("No clients ready for acquisition.")
            return

        receiver_info = self.data_receiver_service.start(
            duration=args.duration,
            suffix=args.suffix,
            acq_type=args.acq_type,
            run_id=args.run_id,
            batch_id=args.batch_id,
            force_compile=args.force_compile,
        )

        if receiver_info is None:
            self.poutput("Failed to start data receiver.")
            logger.error("Failed to start data receiver")
            return

        self.poutput(
            f"Data receiver started. "
            f"PID={receiver_info['pid']}, file={receiver_info['file']}"
        )

        if args.duration is not None and args.duration > 0:
            watcher_thread = threading.Thread(
                target=_watch_acquisition_completion,
                args=(self, rc_ready_clients),
                daemon=True,
            )
            watcher_thread.start()

            self.poutput(
                "Automatic finalization enabled: RC disable and final flush "
                "will run when the receiver duration elapses."
            )

        return

    if args.command == "stop":
        client_ids = self.control_manager.server_state.list_connected_clients()

        _finalize_acquisition(
            self=self,
            client_ids=client_ids,
            reason="manual stop command",
        )

        return