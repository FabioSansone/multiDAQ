from typing import List
import time
import threading

from server.utils.logger import get_logger



class AcquisitionOrchestrator:
    def __init__(
        self,
        acquisition_service,
        channel_selection_service,
        get_mode,
        output_func=None,
    ) -> None:
        self.acquisition_service = acquisition_service
        self.channel_selection_service = channel_selection_service
        self.get_mode = get_mode
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("acquisition_orchestrator")
        self.logger.debug("Acquisition Orchestrator initialized")





    def start(self, args) -> None:
        self.poutput("Acquisition start command received.")

        client_ids = self.acquisition_service.get_connected_clients()

        if not client_ids:
            self.poutput("No connected clients.")
            return

        if self.acquisition_service.check_acquisition_busy():
            self.poutput("Data receiver is already running or finalizing.")
            return

        self.acquisition_service.reset_acquisition_state()

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

                rc_ok = self.acquisition_service.enable_rc_channels(
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

                rc_ok = self.acquisition_service.enable_rc_channels(
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
        
        resolved_batch_id = self.acquisition_service.resolve_batch_id(args, rc_ready_clients)

        if resolved_batch_id is None:
            self.poutput("Cannot start acquisition: missing batch_id and multipmt_id.")
            self.acquisition_service.disable_rc_channels()
            return

        receiver_info = self.acquisition_service.start_receiver(
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
            self.acquisition_service.disable_rc_channels()
            return

        self.poutput(
            f"Data receiver started. "
            f"PID={receiver_info['pid']}, file={receiver_info['file']}"
        )

        if args.duration is not None and args.duration > 0:
            watcher_thread = threading.Thread(
                target=self.acquisition_service.watch_acquisition_completion,
                args=(rc_ready_clients,),
                daemon=True,
            )
            watcher_thread.start()

            self.poutput(
                "Automatic finalization enabled: RC disable and final flush "
                "will run when the receiver duration elapses."
            )

    def stop(self) -> None:
        client_ids = self.acquisition_service.get_connected_clients()

        self.acquisition_service.finalize_acquisition(
            client_ids=client_ids,
            reason="manual stop command",
        )
        
    
    
    
