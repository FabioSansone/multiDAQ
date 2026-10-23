import threading

from server.utils.logger import get_logger
from server.services.client_command_service import CommandPlane
from server.core.server_state import ServerFSM, ServerFSMEvent


class AcquisitionOrchestrator:
    def __init__(
        self,
        acquisition_service,
        channel_selection_service,
        server_state,
        get_mode,
        output_func=None,
    ) -> None:
        self.acquisition_service = acquisition_service
        self.channel_selection_service = channel_selection_service
        self.get_mode = get_mode
        self.server_state = server_state
        self.poutput = output_func or (lambda message: None)
        self.logger = get_logger("acquisition_orchestrator")
        self.logger.debug("Acquisition Orchestrator initialized")

    def start(self, args) -> None:
        self.poutput("Acquisition start command received.")

        current_state = self.server_state.get_server_state()
        if current_state != ServerFSM.READY:
            self.poutput(f"Cannot start acquisition: server is '{current_state.value}', expected READY.")
            return

        trigger_config = self.acquisition_service.build_trigger_configuration(args=args)

        if trigger_config is None:
            self.poutput("Invalid trigger configuration. Acquisition not started.")
            return

        operational = set(self.server_state.get_operational_clients())
        client_ids = [
            cid for cid in self.acquisition_service.get_connected_clients(plane=CommandPlane.ACQUISITION)
            if cid in operational
        ]

        if not client_ids:
            self.poutput("No operational clients ready for acquisition.")
            return

        started = self.server_state.process_event(
            event=ServerFSMEvent.CONFIGURATION_STARTED,
            reason="Preparing RC/HV channels for acquisition",
            source="acquisition_orchestrator",
            metadata={"target_clients": client_ids},
        )

        if not started:
            self.poutput("Cannot start acquisition: invalid FSM transition (CONFIGURATION_STARTED).")
            self.logger.error("CONFIGURATION_STARTED rejected by FSM in acquisition start")
            return

        # Values the overridden channels had before, per client: put back at the end
        # of the run, or at once if the run does not start (also on an unexpected error).
        hv_previous: dict[bytes, dict[int, dict]] = {}

        try:
            self._prepare_and_start(args, client_ids, trigger_config, hv_previous)

        except Exception as exc:
            self.logger.exception("Unexpected error while preparing the acquisition")
            self.poutput(f"Acquisition not started: internal error while preparing it ({exc}).")

            try:
                self._restore_hv_overrides(hv_previous)
            except Exception:
                self.logger.exception("Restoring the HV override failed unexpectedly")

            state = self.server_state.get_server_state()

            if state == ServerFSM.CONFIGURING:
                self.server_state.process_event(
                    event=ServerFSMEvent.CONFIGURATION_FAILED,
                    reason=f"Unexpected error while preparing the acquisition: {exc}",
                    source="acquisition_orchestrator",
                )

            elif state == ServerFSM.ACQUIRING:
                # The session thread did not start: close the session that was opened.
                self.acquisition_service.close_session(success=False, reason="acquisition could not start")

    def _prepare_and_start(self, args, client_ids, trigger_config, hv_previous) -> None:
        """Prepare RC/HV channels step by step and start the session thread. The caller
        puts back the HV override recorded in hv_previous if this raises."""
        mode = self.get_mode()

        # The preparation is a sequence of steps. Each step removes the channels that
        # cannot take part (among those the previous step left) and drops the clients
        # left without channels or failing, recording every removal in the log.

        # Step 1: RC19 off on every target, before HV and channels are touched.
        active_clients = []

        for client_id in client_ids:
            if self.acquisition_service.disable_rc_channels(client_ids=[client_id]):
                active_clients.append(client_id)
            else:
                self._drop_client(client_id, "RC19 disable", "RC19 could not be disabled")

        # Step 2: channels usable according to the hardware (HV/FEB state).
        channels_by_client: dict[bytes, list[int]] = {}

        if mode == "test":
            self.poutput(
                "Server is in test mode: skipping HV power commands. "
                "RC channels will be enabled from HV/FEB presence if available."
            )

            for client_id in active_clients:
                selection = self.channel_selection_service.get_test_rc_channels(
                    client_id=client_id, plane=CommandPlane.ACQUISITION
                )

                self._print_notes(selection.notes)

                if not selection.channels:
                    self._drop_client(client_id, "channel selection", "no channels available")
                    continue

                channels_by_client[client_id] = selection.channels

        elif active_clients:
            enabled_channels_by_client = self.channel_selection_service.prepare_hv_channels_for_acquisition(
                plane=CommandPlane.ACQUISITION, target_client_ids=active_clients,
            )

            for client_id in active_clients:
                selection = enabled_channels_by_client.get(client_id)

                if selection is None:
                    self._drop_client(client_id, "HV preparation", "client not prepared")
                    continue

                self._print_notes(selection.notes)

                if not selection.channels:
                    self._drop_client(client_id, "HV preparation", "no HV channels available")
                    continue

                self.poutput(
                    f"Client {client_id.decode(errors='ignore')}: HV channels ready for acquisition: "
                    f"{selection.channels}"
                )
                channels_by_client[client_id] = selection.channels

        # Step 3 (multipmt): channels without a matching calibration.
        if mode == "multipmt":
            for client_id in list(channels_by_client):
                channels = self._exclude_channels(
                    client_id, channels_by_client[client_id],
                    self.server_state.get_calibration_excluded_channels(client_id),
                    "calibration match", "no matching calibration",
                )

                if not channels:
                    del channels_by_client[client_id]
                    self._drop_client(client_id, "calibration match", "no channel with a matching calibration")
                    continue

                channels_by_client[client_id] = channels

        # Step 4: voltage/threshold override requested by the user. It is also
        # applied in test mode: the HV policy of the client allows setting the
        # parameters there, while powering the channels remains forbidden.
        for client_id in list(channels_by_client):
            client_name = client_id.decode(errors="ignore")
            channels = channels_by_client[client_id]

            hv_override, override_excluded, override_error = self.acquisition_service.resolve_hv_overrides(
                client_id=client_id, args=args,
            )

            if override_error is not None:
                del channels_by_client[client_id]
                self._drop_client(client_id, "HV override", override_error)
                continue

            # A channel that cannot be overridden is left out alone, with its reason.
            for reason in sorted(set(override_excluded.values())):
                channels = self._exclude_channels(
                    client_id, channels,
                    [ch for ch, ch_reason in override_excluded.items() if ch_reason == reason],
                    "HV override", reason,
                )

            if not channels:
                del channels_by_client[client_id]
                self._drop_client(client_id, "HV override", "no channel left after the override")
                continue

            channels_by_client[client_id] = channels

            if not hv_override:
                continue

            if mode == "multipmt":
                excluded_now = sorted(set(hv_override.keys()) - set(channels))
                if excluded_now:
                    self.poutput(
                        f"Client {client_name}: HV override applies to channel(s) {excluded_now}, "
                        "currently excluded from this acquisition (no matching calibration). "
                        "Voltage/threshold will be stored, but these channels will NOT participate "
                        "now -- start acquisition again after the calibration mismatch is resolved "
                        "to include them."
                    )

            outcome = self.acquisition_service.apply_hv_override(
                client_id=client_id, acq_info=hv_override, wait_ramp=True,
            )

            if outcome is None:
                del channels_by_client[client_id]
                self._drop_client(client_id, "HV override", "voltage/threshold could not be applied")
                continue

            configured, previous = outcome

            if previous:
                hv_previous[client_id] = previous

            # Channels the user asked to override but the client did not configure
            # must not take part with an unknown voltage/threshold.
            channels = self._exclude_channels(
                client_id, channels, set(hv_override.keys()) - set(configured),
                "HV override", "voltage/threshold not applied",
            )

            if not channels:
                del channels_by_client[client_id]
                self._drop_client(client_id, "HV override", "no channel left after the override")
                self._restore_hv_overrides(hv_previous, [client_id])
                continue

            channels_by_client[client_id] = channels

        # Step 5: RC trigger configuration (RC19 stays off until the producer starts).
        rc_ready_clients = []

        for client_id in channels_by_client:
            rc_ok = self.acquisition_service.configure_acquisition_client(
                client_id=client_id, trigger_config=trigger_config,
                effective_channels=channels_by_client[client_id],
            )

            if not rc_ok:
                self._drop_client(client_id, "RC configuration", "RC acquisition registers not configured")
                self._restore_hv_overrides(hv_previous, [client_id])
                continue

            rc_ready_clients.append(client_id)

        failed_clients = [cid for cid in client_ids if cid not in rc_ready_clients]

        if not rc_ready_clients:
            self._restore_hv_overrides(hv_previous)
            self.server_state.process_event(
                event=ServerFSMEvent.CONFIGURATION_FAILED,
                reason="No clients ready for acquisition after RC/HV preparation",
                source="acquisition_orchestrator",
                metadata={"failed_clients": failed_clients},
            )
            self.poutput("No clients ready for acquisition.")
            return

        self.server_state.process_event(
            event=ServerFSMEvent.CONFIGURATION_SUCCEEDED,
            reason="RC/HV channels prepared for acquisition",
            source="acquisition_orchestrator",
            metadata={"successful_clients": rc_ready_clients, "failed_clients": failed_clients},
        )

        resolved_batch_id = self.acquisition_service.resolve_batch_id(rc_ready_clients)

        if resolved_batch_id is None:
            self.poutput("Cannot start acquisition: missing batch_id and multipmt_id.")
            self.acquisition_service.disable_rc_channels(client_ids=rc_ready_clients)
            self._restore_hv_overrides(hv_previous)
            return

        self.poutput(f"Acquisition folder id: {resolved_batch_id}")

        started_acq = self.server_state.process_event(
            event=ServerFSMEvent.ACQUISITION_STARTED,
            reason="Acquisition started",
            source="acquisition_orchestrator",
            metadata={"active_clients": rc_ready_clients},
        )

        if not started_acq:
            self.poutput("FSM rejected acquisition start.")
            self.logger.error("ACQUISITION_STARTED rejected by FSM")
            self.acquisition_service.disable_rc_channels(client_ids=rc_ready_clients)
            self._restore_hv_overrides(hv_previous)
            return

        self.acquisition_service.begin_session()

        owner_thread = threading.Thread(
            target=self._run_session,
            kwargs={
                "client_ids": rc_ready_clients,
                "acq_type": args.acq_type,
                "file_format": args.file_format,
                "suffix": args.suffix,
                "run_id": args.run_id,
                "duration": args.duration,
                "hv_previous": dict(hv_previous),
            },
            daemon=True,
        )
        owner_thread.start()

        # From now on the session thread owns the restore.
        hv_previous.clear()

        self.poutput(
            "Acquisition running in background. The server remains responsive; "
            "use 'acquisition stop' to stop it."
        )

    def _print_notes(self, notes: list[str]) -> None:
        """Tell the user what a service left out (the service only logs it)."""
        for note in notes:
            self.poutput(note)

    def _drop_client(self, client_id: bytes, step: str, reason: str) -> None:
        message = f"Client {client_id.decode(errors='ignore')}: removed from the acquisition at step '{step}': {reason}."
        self.logger.warning(message)
        self.poutput(message)

    def _exclude_channels(self, client_id: bytes, channels, excluded, step: str, reason: str) -> list[int]:
        removed = sorted(set(channels) & set(excluded))

        if removed:
            message = (
                f"Client {client_id.decode(errors='ignore')}: channels {removed} excluded "
                f"at step '{step}': {reason}."
            )
            self.logger.warning(message)
            self.poutput(message)

        return sorted(set(channels) - set(excluded))

    def _restore_hv_overrides(self, hv_previous: dict[bytes, dict[int, dict]], client_ids=None) -> list[bytes]:
        """Put back voltage/threshold of the overridden channels (all the clients in
        hv_previous, or only client_ids). Each client is restored once.

        Returns the clients whose restore failed on every channel."""
        unrestored = []

        for client_id in list(hv_previous):
            if client_ids is not None and client_id not in client_ids:
                continue

            previous = hv_previous.pop(client_id)
            client_name = client_id.decode(errors="ignore")

            self.poutput(f"Client {client_name}: restoring voltage/threshold on channels {sorted(previous)}.")

            restored, not_restored = self.acquisition_service.restore_hv_override(client_id=client_id, previous=previous)

            if not_restored:
                self.poutput(f"Client {client_name}: voltage/threshold NOT restored on channels {not_restored} (see log).")

            if previous and not restored:
                unrestored.append(client_id)

        return unrestored

    def _on_duration_elapsed(self) -> None:
        self.poutput("Acquisition duration elapsed: finalizing.")
        self.server_state.process_event(
            event=ServerFSMEvent.RECEIVER_COMPLETED,
            reason="Acquisition duration elapsed",
            source="acquisition_orchestrator",
        )

    def _run_session(self, client_ids, acq_type, file_format, suffix, run_id, duration, hv_previous) -> None:
        """Thread della sessione: esegue l'acquisizione e chiude sempre la
        sessione, anche in caso di eccezione."""
        reason = "acquisition session completed"
        success = False

        try:
            result = self.acquisition_service.run_acquisition_session(
                client_ids=client_ids,
                acq_type=acq_type,
                file_format=file_format,
                suffix=suffix,
                run_id=run_id,
                duration=duration,
                reason=reason,
                on_duration_elapsed=self._on_duration_elapsed,
            )
            success = result.success

            if success:
                self.poutput("Acquisition finalized.")

            for client_id in result.failed_clients:
                client_name = client_id.decode(errors="ignore")
                self.poutput(f"Client {client_name}: acquisition failed ({result.clients[client_id].error}).")

            if not success:
                self.poutput(
                    "Acquisition ended with errors. The server returns to READY: "
                    "check the log and the hardware state before starting a new run."
                )

        except Exception as exc:
            self.logger.exception("Acquisition session aborted by an unexpected error")
            self.poutput(
                f"Acquisition session aborted by an internal error: {exc}. "
                "The server returns to READY: check the hardware state before starting a new run."
            )

        finally:
            # Hardware closed and reset: voltage/threshold go back to what they were
            # before the override, while the FSM is still finalizing. The restore is
            # attempted even after an error and never prevents the session from closing.
            unrestored = []

            try:
                unrestored = self._restore_hv_overrides(hv_previous)
            except Exception:
                self.logger.exception("Restoring the HV override failed unexpectedly")

            self.acquisition_service.close_session(success=success, reason=reason)

            # A client that could not be restored on any channel is left out of the next
            # acquisitions (after the session, which brings the clients back to READY).
            for client_id in unrestored:
                self.server_state.remove_operational_client(client_id)
                self.poutput(
                    f"Client {client_id.decode(errors='ignore')}: voltage/threshold could not be restored "
                    "on any channel; the client is excluded from the next acquisitions."
                )

    def stop(self) -> None:

        current_state = self.server_state.get_server_state()
        
        if current_state == ServerFSM.FINALIZING:
            self.acquisition_service.request_stop()
            return
        

        if current_state == ServerFSM.ACQUIRING:
            stop_requested = self.server_state.process_event(
                event=ServerFSMEvent.STOP_REQUESTED,
                reason="Manual stop command",
                source="acquisition_orchestrator",
            )
            if not stop_requested:
                self.poutput("Cannot register stop in FSM.")
                self.logger.error("STOP_REQUESTED rejected by FSM")
                return
            self.poutput("Stop requested; finalization is running in the background.")
            
            self.acquisition_service.request_stop()
        else:
            self.poutput(f"Nothing to stop: server is '{current_state.value}'.")


   

    