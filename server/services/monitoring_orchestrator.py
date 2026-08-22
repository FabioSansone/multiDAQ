from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import time

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
        
        self.console = Console()

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
        
    @staticmethod
    def _fmt(value, digits: int = 2) -> str:
        if value is None:
            return "—"

        if isinstance(value, float):
            return f"{value:.{digits}f}"

        return str(value)

    def _print_client_header(
        self,
        client_id: bytes,
        identity: dict,
    ) -> None:

        client_name = client_id.decode(errors="ignore")

        multipmt_id = identity.get("multipmt_id", "—")
        batch_id = identity.get("batch_id", "—")

        text = Text()
        text.append(client_name, style="bold")
        text.append("\n")
        text.append(f"multiPMT ID: {multipmt_id}")
        text.append("    ")
        text.append(f"Batch ID: {batch_id}")

        self.console.print(
            Panel(
                text,
                title="multiDAQ Monitoring Snapshot",
                expand=False,
            )
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

    @staticmethod
    def _build_hv_state_lookup(
        hv_snapshot: dict | None,
    ) -> dict[int, dict]:

        if not hv_snapshot:
            return {}

        result = hv_snapshot.get(
            "result", {}
        )

        electrical = (
            result
            .get("electrical", {})
            .get("channels", {})
        )

        lookup: dict[int, dict] = {}

        for hv_key, data in electrical.items():
            try:
                hv_channel = int(hv_key)
            except (TypeError, ValueError):
                continue

            rc_channel = hv_channel - 1

            if rc_channel < 0 or rc_channel >= 7:
                continue

            lookup[rc_channel] = {
                "channel_state": data.get(
                    "channel_state"
                ),
                "power_state": data.get(
                    "power_state"
                ),
            }

        return lookup
    
    
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
                    self.server_state.get_identity(
                        client_id
                    )
                    or {}
                )
            }

            if include_main:
                client_snapshot["main"] = (
                    self.monitoring_service.read_main_snapshot(
                        client_id=client_id,
                    )
                )

            hv_snapshot = None

            if include_hv or include_rc:
                hv_snapshot = (
                    self.monitoring_service.read_hv_snapshot(
                        client_id=client_id,
                        channels=channels,
                    )
                )

                if include_hv:
                    client_snapshot["hv"] = hv_snapshot

            if include_rc:
                rc_snapshot = (
                    self.monitoring_service.read_rc_snapshot(
                        client_id=client_id,
                        channels=channels,
                    )
                )

                rc_snapshot["hv_state_by_channel"] = (
                    self._build_hv_state_lookup(
                        hv_snapshot
                    )
                )

                client_snapshot["rc"] = rc_snapshot

            snapshots[client_id] = client_snapshot

        return snapshots
    
    def _print_main(self, snapshot: dict) -> None:

        if not snapshot["success"]:
            self.console.print(
                f"[bold red]MAIN snapshot error:[/bold red] "
                f"{snapshot['error']}"
            )
            return

        result = snapshot["result"]

        #
        # Environment
        #
        env = result.get("env", {})

        env_table = Table(
            title="MAIN — Environment",
            show_header=True,
            header_style="bold",
        )

        env_table.add_column("Quantity")
        env_table.add_column("Value", justify="right")
        env_table.add_column("Unit")

        env_table.add_row(
            "Temperature",
            self._fmt(env.get("temperature_c")),
            "°C",
        )

        env_table.add_row(
            "Pressure",
            self._fmt(env.get("pressure_hpa")),
            "hPa",
        )

        env_table.add_row(
            "Humidity",
            self._fmt(env.get("humidity_pct")),
            "%",
        )

        self.console.print(env_table)

        #
        # Power
        #
        power = result.get("power", {})

        power_table = Table(
            title="MAIN — Power",
            show_header=True,
            header_style="bold",
        )

        power_table.add_column("Quantity")
        power_table.add_column("Value", justify="right")
        power_table.add_column("Unit")

        power_table.add_row(
            "Rail AIN0",
            self._fmt(power.get("rail_ain0_v"), 3),
            "V",
        )

        power_table.add_row(
            "I MON 1",
            self._fmt(power.get("i_mon_1_a"), 3),
            "A",
        )

        power_table.add_row(
            "Rail AIN2",
            self._fmt(power.get("rail_ain2_v"), 3),
            "V",
        )

        self.console.print(power_table)

        #
        # Magnetic field
        #
        mag = result.get("mag", {})

        mag_table = Table(
            title="MAIN — Magnetic Field",
            show_header=True,
            header_style="bold",
        )

        mag_table.add_column("Axis")
        mag_table.add_column("Field", justify="right")
        mag_table.add_column("Unit")

        for axis in ("x", "y", "z"):
            mag_table.add_row(
                axis.upper(),
                self._fmt(mag.get(f"mag_{axis}_ut"), 3),
                "µT",
            )

        self.console.print(mag_table)

        #
        # Motion
        #
        motion = result.get("motion", {})

        motion_table = Table(
            title="MAIN — Motion",
            show_header=True,
            header_style="bold",
        )

        motion_table.add_column("Axis")
        motion_table.add_column(
            "Acceleration [g]",
            justify="right",
        )
        motion_table.add_column(
            "Gyroscope [°/s]",
            justify="right",
        )

        for axis in ("x", "y", "z"):
            motion_table.add_row(
                axis.upper(),
                self._fmt(
                    motion.get(f"acc_{axis}_g"),
                    4,
                ),
                self._fmt(
                    motion.get(f"gyr_{axis}_dps"),
                    3,
                ),
            )

        self.console.print(motion_table)

        die_temperature = motion.get(
            "die_temperature_c"
        )

        self.console.print(
            f"  BMI270 die temperature: "
            f"[bold]{self._fmt(die_temperature)} °C[/bold]"
        )


        ######
        #XADC#
        ######

        fpga = result.get("fpga", {})

        fpga_table = Table(
            title="MAIN — FPGA",
            show_header=True,
            header_style="bold",
        )

        fpga_table.add_column("Quantity")
        fpga_table.add_column(
            "Value",
            justify="right",
        )
        fpga_table.add_column("Unit")

        fpga_table.add_row(
            "FPGA temperature",
            self._fmt(
                fpga.get("temperature_c"),
                2,
            ),
            "°C",
        )

        self.console.print(fpga_table)
            
            
    def _print_rc(self, snapshot: dict) -> None:

        if not snapshot["success"]:
            self.console.print(
                f"[bold yellow]RC snapshot completed with errors:[/bold yellow] "
                f"{snapshot['error']}"
            )

        result = snapshot["result"]

        free = result.get("free", {})
        trigger = result.get("trigger", {})

        free_channels = free.get("channels", {})
        trigger_channels = trigger.get(
            "channels", {}
        )

        hv_state_by_channel = snapshot.get(
            "hv_state_by_channel", {}
        )

        table = Table(
            title="RC Channel Rates",
            show_header=True,
            header_style="bold",
        )

        table.add_column("Ch", justify="right")
        table.add_column(
            "Free rate [Hz]",
            justify="right",
        )
        table.add_column(
            "Triggered rate [Hz]",
            justify="right",
        )
        table.add_column("Rate enable")
        table.add_column("HV state")
        table.add_column("HV power")

        all_channels = sorted(
            {
                int(ch)
                for ch in (
                    list(free_channels.keys())
                    + list(trigger_channels.keys())
                )
            }
        )

        for channel in all_channels:

            free_data = (
                free_channels.get(channel)
                or free_channels.get(str(channel))
                or {}
            )

            trigger_data = (
                trigger_channels.get(channel)
                or trigger_channels.get(str(channel))
                or {}
            )

            hv_info = hv_state_by_channel.get(
                channel, {}
            )

            enabled = bool(
                free_data.get("enabled")
            )

            if enabled:
                enabled_text = Text(
                    "ENABLED",
                    style="green",
                )
            else:
                enabled_text = Text(
                    "DISABLED",
                    style="dim",
                )

            hv_state = hv_info.get(
                "channel_state"
            )

            if hv_state == "ok":
                hv_state_text = Text(
                    "OK",
                    style="green",
                )
            elif hv_state == "bad":
                hv_state_text = Text(
                    "BAD",
                    style="red",
                )
            elif hv_state == "fixed_bad":
                hv_state_text = Text(
                    "FIXED BAD",
                    style="bold red",
                )
            else:
                hv_state_text = Text(
                    str(hv_state or "—")
                )

            table.add_row(
                str(channel),
                self._fmt(
                    free_data.get("value"),
                    0,
                ),
                self._fmt(
                    trigger_data.get("value"),
                    0,
                ),
                enabled_text,
                hv_state_text,
                str(
                    hv_info.get("power_state")
                    or "—"
                ).upper(),
            )

        self.console.print(table)
        
        trigger_table = Table(
            title="RC Trigger Monitoring",
            show_header=True,
            header_style="bold",
        )

        trigger_table.add_column("Counter")
        trigger_table.add_column(
            "Rate [Hz]",
            justify="right",
        )
        trigger_table.add_column("Configuration")
        
        
        external_data = trigger.get(
            "external_trigger_rate", {}
        )

        auto_data = trigger.get(
            "auto_trigger_rate", {}
        )

        auto_config = trigger.get(
            "auto_trigger_config"
        )
        
        
        auto_description = "unavailable"

        if auto_config is not None:

            mode = auto_config.get("mode")

            if mode == "majority":
                multiplicity = auto_config.get(
                    "majority_threshold"
                )

                if multiplicity == 0:
                    auto_description = (
                        "not configured"
                    )
                else:
                    auto_description = (
                        f"majority "
                        f"(multiplicity={multiplicity})"
                    )

            elif mode == "exact":
                channels = auto_config.get(
                    "exact_channels", []
                )

                auto_description = (
                    "exact channels "
                    f"{channels}"
                )
            
        trigger_table.add_row(
            "External trigger",
            self._fmt(
                external_data.get("value"),
                0,
            ),
            "—",
        )

        trigger_table.add_row(
            "Auto trigger",
            self._fmt(
                auto_data.get("value"),
                0,
            ),
            auto_description,
        )

        self.console.print(trigger_table)
            
        
    def _print_hv(self, snapshot: dict) -> None:

        if not snapshot["success"]:
            self.console.print(
                f"[bold yellow]HV snapshot completed with errors:[/bold yellow] "
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

        table = Table(
            title="HV Channels",
            show_header=True,
            header_style="bold",
        )

        table.add_column("Ch", justify="right")
        table.add_column("State")
        table.add_column("Power")
        table.add_column("Voltage [V]", justify="right")
        table.add_column("Current [µA]", justify="right")
        table.add_column("Temp. [°C]", justify="right")
        table.add_column("HW status")
        table.add_column("Alarm")

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

            state = electrical_data.get(
                "channel_state"
            )

            power = electrical_data.get(
                "power_state"
            )

            if state == "ok":
                state_text = Text("OK", style="green")
            elif state == "bad":
                state_text = Text("BAD", style="red")
            elif state == "fixed_bad":
                state_text = Text(
                    "FIXED BAD",
                    style="bold red",
                )
            else:
                state_text = Text(
                    str(state or "—")
                )

            if power == "on":
                power_text = Text("ON", style="green")
            elif power == "off":
                power_text = Text("OFF", style="dim")
            else:
                power_text = Text("—")

            alarm = status_data.get("hw_alarm")

            if alarm and alarm != "none":
                alarm_text = Text(
                    str(alarm),
                    style="bold red",
                )
            else:
                alarm_text = Text(
                    str(alarm or "—")
                )

            table.add_row(
                str(user_channel),
                state_text,
                power_text,
                self._fmt(
                    electrical_data.get("voltage"),
                    3,
                ),
                self._fmt(
                    electrical_data.get("current"),
                    3,
                ),
                self._fmt(
                    electrical_data.get("temperature"),
                    2,
                ),
                str(
                    status_data.get("hw_status")
                    or "—"
                ),
                alarm_text,
            )

        self.console.print(table)
        
        
        
            
    def _print_snapshots(
        self,
        snapshots: dict[bytes, dict],
    ) -> None:

        for client_id, snapshot in snapshots.items():

            identity = snapshot.get(
                "identity", {}
            )

            self._print_client_header(
                client_id=client_id,
                identity=identity,
            )

            if "main" in snapshot:
                self._print_main(
                    snapshot["main"]
                )

            if "hv" in snapshot:
                self._print_hv(
                    snapshot["hv"]
                )

            if "rc" in snapshot:
                self._print_rc(
                    snapshot["rc"]
                )

            self.console.print()

            
    
    
    def snapshot(self, args) -> None:

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

        self._print_snapshots(
            snapshots
        )


    def poll(self, args) -> None:

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

        interval_s = args.interval
        duration_s = args.duration

        start_time = time.monotonic()
        next_poll_time = start_time

        self.logger.info(
            "Monitoring poll started: "
            f"clients={len(client_ids)}, "
            f"interval={interval_s}s, "
            f"duration={duration_s}s"
        )

        try:

            while True:

                now = time.monotonic()

                if now - start_time >= duration_s:
                    break

                if now < next_poll_time:
                    time.sleep(
                        next_poll_time - now
                    )

                now = time.monotonic()

                if now - start_time >= duration_s:
                    break

                snapshots = self.collect_snapshot(
                    client_ids=client_ids,
                    include_main=include_main,
                    include_rc=include_rc,
                    include_hv=include_hv,
                    channels=args.channels,
                )

                elapsed_s = (
                    time.monotonic()
                    - start_time
                )

                self.console.print(
                    f"[bold cyan]"
                    f"Monitoring poll — "
                    f"t={elapsed_s:.1f}s"
                    f"[/bold cyan]"
                )

                self._print_snapshots(
                    snapshots
                )

                next_poll_time += interval_s

                now = time.monotonic()

                while next_poll_time <= now:
                    next_poll_time += interval_s

        except KeyboardInterrupt:

            self.console.print(
                "\n[yellow]"
                "Monitoring poll interrupted by user."
                "[/yellow]"
            )

            self.logger.info(
                "Monitoring poll interrupted by user"
            )

            return

        self.logger.info(
            "Monitoring poll completed"
        )

        self.console.print(
            "[green]"
            "Monitoring poll completed."
            "[/green]"
        )