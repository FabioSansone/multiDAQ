from server.utils.logger import get_logger


class MonitoringOrchestrator:

    def __init__(
        self,
        monitoring_service,
        server_state,
        output_func=None,
    ) -> None:

        self.monitoring_service = monitoring_service
        self.server_state = server_state

        self.poutput = output_func or (lambda message: None)

        self.logger = get_logger("monitoring_orchestrator")
        self.logger.debug("Monitoring Orchestrator initialized")
    
    @staticmethod
    def _resolve_sections(args) -> tuple[bool, bool, bool]:

        any_selected = (
            args.main
            or args.rc
            or args.hv
        )

        if not any_selected:
            return True, True, True

        return (
            args.main,
            args.rc,
            args.hv,
        )


    def _resolve_targets(self, args) -> list[bytes]:

        if args.multipmt_id is None and args.batch_id is None:
            return self.server_state.list_monitoring_clients()

        resolved = self.server_state.resolve_client_id(
            multipmt_id=args.multipmt_id,
            batch_id=args.batch_id,
        )

        if (
            resolved is None
            or not self.server_state.is_client_on_plane(
                resolved,
                "monitoring",
            )
        ):
            return []

        return [resolved]

    
    def collect_snapshot(
        self,
        *,
        client_ids: list[bytes],
        include_main: bool,
        include_rc: bool,
        include_hv: bool,
        channels="all",
    ) -> dict[bytes, dict]:

        snapshots = {}

        for client_id in client_ids:

            client_snapshot = {
                "identity": (
                    self.server_state.get_identity(client_id)
                    or {}
                )
            }

            if include_main:
                client_snapshot["main"] = (
                    self.monitoring_service.read_main_snapshot(
                        client_id=client_id,
                    )
                )

            if include_rc:
                client_snapshot["rc"] = (
                    self.monitoring_service.read_rc_snapshot(
                        client_id=client_id,
                        channels=channels,
                    )
                )

            if include_hv:
                client_snapshot["hv"] = (
                    self.monitoring_service.read_hv_snapshot(
                        client_id=client_id,
                        channels=channels,
                    )
                )

            snapshots[client_id] = client_snapshot

        return snapshots
    
    def _print_main(self, snapshot: dict) -> None:

        if not snapshot["success"]:
            self.poutput(
                f"  MAIN: ERROR - {snapshot['error']}"
            )
            return

        result = snapshot["result"]

        self.poutput("  MAIN")

        for group, values in result.items():
            self.poutput(
                f"    {group}: {values}"
            )
            
            
    def _print_rc(self, snapshot: dict) -> None:

        if not snapshot["success"]:
            self.poutput(
                f"  RC: snapshot completed with errors - "
                f"{snapshot['error']}"
            )

        result = snapshot["result"]

        free = result.get("free", {})
        trigger = result.get("trigger", {})

        self.poutput("  RC")

        self.poutput("    Free rates:")

        for channel, data in free.get(
            "channels", {}
        ).items():

            self.poutput(
                f"      ch {channel}: "
                f"{data.get('value')} "
                f"(enabled={data.get('enabled')})"
            )

        self.poutput("    Trigger rates:")

        for channel, data in trigger.get(
            "channels", {}
        ).items():

            self.poutput(
                f"      ch {channel}: "
                f"{data.get('value')} "
                f"(enabled={data.get('enabled')})"
            )

        self.poutput(
            "    external trigger rate: "
            f"{trigger.get('external_trigger_rate', {}).get('value')}"
        )

        self.poutput(
            "    auto trigger rate: "
            f"{trigger.get('auto_trigger_rate', {}).get('value')}"
        )
        
    def _print_hv(self, snapshot: dict) -> None:

        if not snapshot["success"]:
            self.poutput(
                f"  HV: snapshot completed with errors - "
                f"{snapshot['error']}"
            )

        result = snapshot["result"]

        electrical = (
            result
            .get("electrical", {})
            .get("channels", {})
        )

        status_alarm = (
            result
            .get("status_alarm", {})
            .get("channels", {})
        )

        self.poutput("  HV")

        channels = sorted(
            {
                int(key)
                for key in (
                    list(electrical.keys())
                    + list(status_alarm.keys())
                )
            }
        )

        for hv_channel in channels:

            electrical_data = (
                electrical.get(hv_channel)
                or electrical.get(str(hv_channel))
                or {}
            )

            status_data = (
                status_alarm.get(hv_channel)
                or status_alarm.get(str(hv_channel))
                or {}
            )

            user_channel = hv_channel - 1

            self.poutput(
                f"    ch {user_channel}: "
                f"V={electrical_data.get('voltage')} "
                f"I={electrical_data.get('current')} "
                f"T={electrical_data.get('temperature')} "
                f"state={electrical_data.get('channel_state')} "
                f"power={electrical_data.get('power_state')} "
                f"status={status_data.get('hw_status')} "
                f"alarm={status_data.get('hw_alarm')}"
            )
            
    def _print_snapshots(
        self,
        snapshots: dict[bytes, dict],
    ) -> None:

        for client_id, snapshot in snapshots.items():

            client_name = client_id.decode(
                errors="ignore"
            )

            identity = snapshot.get(
                "identity", {}
            )

            self.poutput("")
            self.poutput(
                f"Client {client_name} "
                f"(multipmt_id="
                f"{identity.get('multipmt_id')}, "
                f"batch_id="
                f"{identity.get('batch_id')})"
            )

            if "main" in snapshot:
                self._print_main(
                    snapshot["main"]
                )

            if "rc" in snapshot:
                self._print_rc(
                    snapshot["rc"]
                )

            if "hv" in snapshot:
                self._print_hv(
                    snapshot["hv"]
                )
    
    
    def status(self, args) -> None:

        if args.interval is not None:
            self.poutput(
                "Repeated monitoring is not enabled yet. "
                "Use status without --interval/--duration."
            )
            return

        client_ids = self._resolve_targets(args)

        if not client_ids:
            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )
            return

        (
            include_main,
            include_rc,
            include_hv,
        ) = self._resolve_sections(args)

        snapshots = self.collect_snapshot(
            client_ids=client_ids,
            include_main=include_main,
            include_rc=include_rc,
            include_hv=include_hv,
            channels=args.channels,
        )

        self._print_snapshots(snapshots)