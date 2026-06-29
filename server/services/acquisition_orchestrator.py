from typing import List
import time
import threading

from server.utils.logger import get_logger



class AcquisitionOrchestrator:
    def __init__(
        self,
        control_manager,
        data_receiver_service,
        channel_selection_service,
        command_service,
        get_mode,
        output_func=None,
    ) -> None:
        self.control_manager = control_manager
        self.data_receiver_service = data_receiver_service
        self.channel_selection_service = channel_selection_service
        self.command_service = command_service
        self.get_mode = get_mode
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("acquisition_orchestrator")
        self.logger.debug("Acquisition Orchestrator initialized")

        self._finalize_lock = threading.Lock()
        self._finalized = False

    def _reset_acquisition_state(self) -> None:
        with self._finalize_lock:
            self._finalized = False

    def _enable_rc_channels(self, client_id: bytes, channels: List[int]) -> bool:
        client_name = client_id.decode(errors="ignore")

        if not channels:
            self.poutput(f"Client {client_name}: no RC channels selected for acquisition.")
            self.logger.warning(
                f"Cannot enable RC channels for client {client_name}: empty channel list"
            )
            return False

        register_address = 19
        register_value = 0

        for ch in channels:
            if ch < 0 or ch >= 7:
                self.poutput(f"Client {client_name}: invalid RC channel: {ch}")
                self.logger.error(
                    f"Invalid RC channel requested for acquisition: {ch}"
                )
                return False

            register_value |= 1 << ch

        ok = self.command_service.write_rc_register(
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

            ok = self.command_service.write_rc_register(
                client_id=client_id,
                address=19,
                value=0,
            )

            if ok:
                self.poutput(f"Client {client_name}: RC acquisition channels disabled.")
            else:
                self.poutput(
                    f"Client {client_name}: failed to disable RC acquisition channels."
                )

    def _flush_client(self, client_id: bytes) -> bool:
        client_name = client_id.decode(errors="ignore")

        read_prev = self.command_service.read_rc_register(
            client_id=client_id,
            address=15,
        )

        if read_prev is None:
            self.poutput(
                f"Client {client_name}: flush skipped, no valid read from register 15."
            )
            return False

        if not self.command_service.write_rc_register(
            client_id=client_id,
            address=15,
            value=read_prev + 32,
        ):
            self.poutput(
                f"Client {client_name}: flush failed while writing register 15."
            )
            return False

        time.sleep(2.0)

        read_now = self.command_service.read_rc_register(
            client_id=client_id,
            address=15,
        )

        if read_now is None:
            self.poutput(f"Client {client_name}: flush failed, missing final read.")
            return False

        if read_now - read_prev - 32 == 64:
            self.poutput(f"Client {client_name}: data flushing ended successfully.")

            self.command_service.write_rc_register(
                client_id=client_id,
                address=15,
                value=read_prev,
            )

            return True

        self.poutput(f"Client {client_name}: flush error. Please check.")
        self.logger.error(
            f"Flush check failed for client {client_name}: "
            f"prev={read_prev}, now={read_now}"
        )

        return False

    def _flush_clients(self, client_ids: List[bytes]) -> None:
        time.sleep(10.0)

        for client_id in client_ids:
            self._flush_client(client_id=client_id)

    def _finalize_acquisition(self, client_ids: List[bytes], reason: str) -> None:
        with self._finalize_lock:
            if self._finalized:
                self.logger.info(
                    f"Acquisition finalization already completed. Ignoring request: {reason}"
                )
                return

            self._finalized = True

        self.poutput(f"Finalizing acquisition: {reason}")
        self.logger.info(f"Finalizing acquisition: {reason}")

        self._disable_rc_channels()

        if self.data_receiver_service.is_running():
            stopped = self.data_receiver_service.stop()

            if stopped:
                self.poutput("Acquisition stopped.")
            else:
                self.poutput("Failed to stop acquisition.")
                return

        else:
            self.poutput("Data receiver is not running. Continuing finalization.")

        if not client_ids:
            self.poutput("No connected clients. Final flush skipped.")
            return

        self.poutput("Starting final flush receiver...")

        flush_info = self.data_receiver_service.start_flush(
            duration=30.0,
        )

        if flush_info is None:
            self.poutput("Failed to start final flush receiver.")
            self.logger.error("Failed to start final flush receiver")
            return

        flush_thread = threading.Thread(
            target=self._flush_clients,
            args=(client_ids,),
            daemon=True,
        )

        flush_thread.start()

        try:
            while self.data_receiver_service.is_running():
                time.sleep(0.5)

            flush_thread.join(timeout=15.0)

        finally:
            self.data_receiver_service.clear_finalizing()

        self.poutput("Final flush completed.")
        self.logger.info("Final flush completed")

    def _watch_acquisition_completion(self, client_ids: List[bytes]) -> None:
        while self.data_receiver_service.is_running():
            time.sleep(0.5)

        self._finalize_acquisition(
            client_ids=client_ids,
            reason="data receiver completed its configured duration",
        )

    
    def _resolve_batch_id(self, args, client_ids: List[bytes]) -> str | None:
        if args.batch_id is not None:
            return args.batch_id

        if not client_ids:
            return None

        client_id = client_ids[0]
        identity = self.control_manager.server_state.get_identity(client_id) or {}

        batch_id = identity.get("batch_id")
        if batch_id:
            self.poutput(f"Using batch_id from client identity: {batch_id}")
            return batch_id

        multipmt_id = identity.get("multipmt_id")
        if multipmt_id:
            self.poutput(
                f"No batch_id in client identity. Using multipmt_id as acquisition folder id: {multipmt_id}"
            )
            return multipmt_id

        return None

    def start(self, args) -> None:
        self.poutput("Acquisition start command received.")

        client_ids = self.control_manager.server_state.list_connected_clients()

        if not client_ids:
            self.poutput("No connected clients.")
            return

        if self.data_receiver_service.is_busy():
            self.poutput("Data receiver is already running or finalizing.")
            return

        self._reset_acquisition_state()

        rc_ready_clients = []
        mode = self.get_mode()
        

        if mode == "test":
            self.poutput(
                "Server is in test mode: skipping HV power commands. "
                "RC channels will be enabled from HV/FEB presence if available."
            )

            for client_id in client_ids:
                client_name = client_id.decode(errors="ignore")
                channels = self.channel_selection_service.get_test_rc_channels(
                    client_id=client_id,
                )

                rc_ok = self._enable_rc_channels(
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
            enabled_channels_by_client = (
                self.channel_selection_service.prepare_hv_channels_for_acquisition()
            )

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

                rc_ok = self._enable_rc_channels(
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
        
        resolved_batch_id = self._resolve_batch_id(args, rc_ready_clients)

        if resolved_batch_id is None:
            self.poutput("Cannot start acquisition: missing batch_id and multipmt_id.")
            self._disable_rc_channels()
            return

        receiver_info = self.data_receiver_service.start(
            duration=args.duration,
            suffix=args.suffix,
            acq_type=args.acq_type,
            run_id=args.run_id,
            batch_id=resolved_batch_id,
            force_compile=args.force_compile,
        )


        if receiver_info is None:
            self.poutput("Failed to start data receiver.")
            self.logger.error("Failed to start data receiver")
            self._disable_rc_channels()
            return

        self.poutput(
            f"Data receiver started. "
            f"PID={receiver_info['pid']}, file={receiver_info['file']}"
        )

        if args.duration is not None and args.duration > 0:
            watcher_thread = threading.Thread(
                target=self._watch_acquisition_completion,
                args=(rc_ready_clients,),
                daemon=True,
            )
            watcher_thread.start()

            self.poutput(
                "Automatic finalization enabled: RC disable and final flush "
                "will run when the receiver duration elapses."
            )

    def stop(self) -> None:
        client_ids = self.control_manager.server_state.list_connected_clients()

        self._finalize_acquisition(
            client_ids=client_ids,
            reason="manual stop command",
        )

    
    
