import time

from server.utils.logger import get_logger

class CalibrationOrchestrator:

    def __init__(self, acquisition_service, channel_selection_service, command_service, get_mode, output_func=None,) -> None:
        

        self.acquisition_service = acquisition_service
        self.channel_selection_service = channel_selection_service
        self.command_service = command_service

        self.poutput = output_func or (lambda message: None)

        self.get_mode = get_mode

        self.logger = get_logger("calibration_orchestrator")
        self.logger.debug("Calibration Orchestrator initialized")




    ####################################
    #Time-To-Peak Calibration Procedure#
    ####################################

    def _parse_ttp_values(self, args) -> list[int]:
        if args.values is not None:
            values = []

            for item in args.values.split(","):
                item = item.strip()

                if not item:
                    continue

                values.append(int(item))

            return values

        start, stop, step = args.range

        if step == 0:
            raise ValueError("TTP range step cannot be zero")

        if step > 0:
            return list(range(start, stop + 1, step))

        return list(range(start, stop - 1, step))   


    def _set_ttp_register(
        self,
        client_ids: list[bytes],
        ttp_value: int,
    ) -> list[bytes]:
        ready_clients = []

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            ok = self.command_service.write_rc_register(
                client_id=client_id,
                address=10,
                value=ttp_value,
            )

            if not ok:
                self.poutput(
                    f"Client {client_name}: failed to write TTP register 10 = {ttp_value}"
                )
                continue

            self.poutput(
                f"Client {client_name}: RC register 10 set to TTP value {ttp_value}"
            )

            ready_clients.append(client_id)

        return ready_clients
    
    def _run_single_ttp_test_acquisition(
        self,
        args,
        client_ids: list[bytes],
        ttp_value: int,
        scan_run_folder,
        resolved_batch_id,
    ) -> None:
        rc_ready_clients = []

        for client_id in client_ids:
            client_name = client_id.decode(errors="ignore")

            channels = self.channel_selection_service.get_test_rc_channels(
                client_id=client_id,
                requested_channels=args.channels,
            )
            
            if not channels:
                self.poutput(
                    f"Client {client_name}: no requested channels available "
                    f"for TTP={ttp_value}"
                )
                continue

            rc_ok = self.acquisition_service.enable_rc_channels(
                client_id=client_id,
                channels=channels,
            )

            if not rc_ok:
                self.poutput(
                    f"Client {client_name}: RC channel enable failed for TTP={ttp_value}"
                )
                continue

            rc_ready_clients.append(client_id)

        if not rc_ready_clients:
            self.poutput(f"No clients ready for TTP={ttp_value}. Skipping.")
            return

        base_suffix = args.suffix.strip() if args.suffix else ""

        if base_suffix in {"", "ttp"}:
            suffix = f"ttp_{ttp_value}"
        else:
            suffix = f"{base_suffix}_ttp_{ttp_value}"


        receiver_info = self.acquisition_service.acquisition_receiver_start(
            duration=args.duration,
            suffix=suffix,
            acq_type=args.acq_type,
            run_id=args.run_id,
            batch_id=resolved_batch_id,
            force_compile=args.force_compile,
            run_folder=scan_run_folder,
        )

        if receiver_info is None:
            self.poutput(f"Failed to start data receiver for TTP={ttp_value}.")
            self.acquisition_service.disable_rc_channels()
            return

        self.poutput(
            f"TTP={ttp_value}: data receiver started. "
            f"PID={receiver_info['pid']}, file={receiver_info['file']}"
        )

        while self.acquisition_service.check_acquisition_running():
            time.sleep(0.5)

        self.acquisition_service.finalize_acquisition(
            client_ids=rc_ready_clients,
            reason=f"TTP scan point completed, ttp={ttp_value}",
        ) 

    def scan_ttp(self, args) -> None:
        self.poutput("TTP scan command received.")

        mode = self.get_mode()

        if mode != "test":
            self.poutput(
                f"TTP scan currently implemented only in test mode. "
                f"Current mode is '{mode}'."
            )
            return

        client_ids = self.acquisition_service.get_connected_clients()


        if not client_ids:
            self.poutput("No connected clients.")
            return

        if self.acquisition_service.check_acquisition_busy():
            self.poutput("Data receiver is already running or finalizing.")
            return

        try:
            ttp_values = self._parse_ttp_values(args)
        except Exception as e:
            self.poutput(f"Invalid TTP scan values: {e}")
            self.logger.error(f"Invalid TTP scan values: {e}")
            return

        if not ttp_values:
            self.poutput("No TTP values selected.")
            return

        self.poutput(f"Starting TTP scan in test mode: values={ttp_values}")

        self.acquisition_service.reset_acquisition_state()
        
        resolved_batch_id = self.acquisition_service.resolve_batch_id(args, client_ids)

        if resolved_batch_id is None:
            self.poutput("Cannot start TTP scan: missing batch_id and multipmt_id.")
            return

        scan_run_folder = self.acquisition_service.get_acquisition_run_folder(resolved_batch_id = resolved_batch_id, args=args)

        for ttp_value in ttp_values:
            if self.acquisition_service.check_acquisition_busy():
                self.poutput(
                    f"Receiver busy before TTP={ttp_value}. Stopping scan."
                )
                return

            self.poutput(f"\nStarting TTP scan point: register 10 = {ttp_value}")

            ttp_ready_clients = self._set_ttp_register(
                client_ids=client_ids,
                ttp_value=ttp_value,
            )

            if not ttp_ready_clients:
                self.poutput(
                    f"No clients accepted TTP={ttp_value}. Skipping point."
                )
                continue

            self.acquisition_service.reset_acquisition_state()

            self._run_single_ttp_test_acquisition(
                args=args,
                client_ids=ttp_ready_clients,
                ttp_value=ttp_value,
                scan_run_folder=scan_run_folder,
                resolved_batch_id=resolved_batch_id,
            )

        self.poutput("TTP scan completed.")

    