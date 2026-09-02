from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
import copy
import time

from server.utils.logger import get_logger
from common.message_handler import Channel
from server.services.client_command_service import CommandPlane


class MonitoringOrchestrator:

    def __init__(
        self,
        monitoring_service,
        monitor_stream_service,
        monitor_persistence_service,
        server_state,
        output_func=None,
    ) -> None:

        self.monitoring_service = monitoring_service
        self.monitor_stream_service = monitor_stream_service
        self.monitor_persistence_service  = monitor_persistence_service
        
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
    def _resolve_save_selection(
        args,
    ) -> tuple[list[Channel], bool]:

        sample_sections = []

        if args.main:
            sample_sections.append(
                Channel.MAIN
            )

        if args.rc:
            sample_sections.append(
                Channel.RC
            )

        if args.hv:
            sample_sections.append(
                Channel.HV
            )

        events_selected = bool(
            args.events
        )

        #
        # Preserve previous default:
        # no explicit section means all periodic
        # sample streams, but not EVENTS.
        #
        if (
            not sample_sections
            and not events_selected
        ):

            sample_sections = [
                Channel.MAIN,
                Channel.RC,
                Channel.HV,
            ]

        return (
            sample_sections,
            events_selected,
        )
        
    @classmethod
    def _resolve_stream_sections(
        cls,
        args,
    ) -> list[Channel]:

        (
            include_main,
            include_rc,
            include_hv,
        ) = cls._resolve_sections(
            args
        )

        sections = []

        if include_main:
            sections.append(
                Channel.MAIN
            )

        if include_rc:
            sections.append(
                Channel.RC
            )

        if include_hv:
            sections.append(
                Channel.HV
            )

        return sections
        
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
    
    
    def _build_persistence_metadata(
        self,
        client_id: bytes,
    ) -> dict | None:
        
        mode = self.server_state.get_mode()

        schemas = (
            self.monitor_persistence_service
            .get_dataset_schemas()
        )

        main_columns = schemas.get(
            "main",
            []
        )

        rc_columns = schemas.get(
            "rc",
            []
        )

        hv_columns = schemas.get(
            "hv",
            []
        )

        metadata_schema_version = schemas.get(
            "mon_metadata_version"
        )
        
        event_columns = schemas.get(
            "events",
            [],
        )

        session = (
            self.monitor_persistence_service
            .get_session()
        )

        if session is None:

            self.logger.error(
                f"Cannot build persistence metadata: "
                f"no active monitoring persistence session "
                f"for client={client_id!r}"
            )

            return None

        identity = (
            self.server_state.get_identity(
                client_id
            )
            or {}
        )

        command_service = (
            self.monitoring_service
            .command_service
        )

        
        rc31 = command_service.read_rc_register(
            client_id=client_id,
            address=31,
            plane=CommandPlane.MONITORING,
        )

        rc39 = command_service.read_rc_register(
            client_id=client_id,
            address=39,
            plane=CommandPlane.MONITORING,
        )

        
        if mode == "test":
            pmt_serial_map = {}
        else:
            pmt_serial_map = (
                command_service
                .get_pmt_serial_map_clients(
                    client_id=client_id,
                    requested_channels="all",
                    plane=CommandPlane.MONITORING,
                )
            )

            if pmt_serial_map is None:
                pmt_serial_map = {}

        metadata = {
            "schema_version": (
                metadata_schema_version
            ),

            "session": {
                "session_id": (
                    session.session_id
                ),
                "started_at_utc_ns": (
                    session.started_at_utc_ns
                ),
                "acquisition_mode": mode,
            },

            "client": {
                "client_id": client_id.decode(
                    errors="ignore"
                ),
                "identity": copy.deepcopy(
                    identity
                ),
            },

            "hardware": {
                "pmt_serial_map": {
                    str(channel): serial
                    for channel, serial
                    in pmt_serial_map.items()
                },
            },

            "configuration": {
                "initial": {
                    "31": rc31,
                    "39": rc39,
                },
                "current": {
                    "31": rc31,
                    "39": rc39,
                },
            },

            "persistence": {
                "sections": {},
            },

            "datasets": {
                "main": {
                    "columns": list(
                        main_columns
                    ),
                },
                "rc": {
                    "columns": list(
                        rc_columns
                    ),
                },
                "hv": {
                    "columns": list(
                        hv_columns
                    ),
                },
                "events": {
                    "columns": list(
                        event_columns
                    ),
                },
            },

            "configuration_history": [],
        }

        return metadata
        
    
    
    
    
    
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

    
    def _print_sensor_status(
        self,
        client_id: bytes,
        status: dict,
    ) -> None:

        identity = (
            self.server_state.get_identity(
                client_id
            )
            or {}
        )

        self._print_client_header(
            client_id=client_id,
            identity=identity,
        )

        if not status.get(
            "success",
            False,
        ):

            self.console.print(
                "[bold red]"
                "MAIN sensor status error:"
                "[/bold red] "
                f"{status.get('error') or 'unknown error'}"
            )

            self.console.print()
            return

        result = (
            status.get(
                "result",
                {},
            )
            or {}
        )

        sensors = (
            result.get(
                "sensors",
                {},
            )
            or {}
        )

        devices = (
            result.get(
                "devices",
                {},
            )
            or {}
        )

        summary = (
            result.get(
                "summary",
                {},
            )
            or {}
        )

        # ================================================================
        # Monitored quantities
        # ================================================================

        sensor_table = Table(
            title="MAIN — Sensor Threshold Status",
            show_header=True,
            header_style="bold",
        )

        sensor_table.add_column(
            "Quantity"
        )

        sensor_table.add_column(
            "Value",
            justify="right",
        )

        sensor_table.add_column(
            "Min",
            justify="right",
        )

        sensor_table.add_column(
            "Max",
            justify="right",
        )

        sensor_table.add_column(
            "Status"
        )

        for sensor_name, data in (
            sensors.items()
        ):

            available = bool(
                data.get(
                    "available"
                )
            )

            alarm = bool(
                data.get(
                    "alarm"
                )
            )

            direction = data.get(
                "direction"
            )

            value = data.get(
                "value"
            )

            min_value = data.get(
                "min"
            )

            max_value = data.get(
                "max"
            )

            if not available:

                status_text = Text(
                    "UNAVAILABLE",
                    style="bold yellow",
                )

            elif alarm:

                if direction == "low":
                    alarm_label = "ALARM LOW"

                elif direction == "high":
                    alarm_label = "ALARM HIGH"

                else:
                    alarm_label = "ALARM"

                status_text = Text(
                    alarm_label,
                    style="bold red",
                )

            else:

                status_text = Text(
                    "OK",
                    style="green",
                )

            sensor_table.add_row(
                sensor_name,
                self._fmt(
                    value,
                    3,
                ),
                self._fmt(
                    min_value,
                    3,
                ),
                self._fmt(
                    max_value,
                    3,
                ),
                status_text,
            )

        self.console.print(
            sensor_table
        )

        # ================================================================
        # Physical devices
        # ================================================================

        device_table = Table(
            title="MAIN — Sensor Devices",
            show_header=True,
            header_style="bold",
        )

        device_table.add_column(
            "Device"
        )

        device_table.add_column(
            "Availability"
        )

        device_table.add_column(
            "I2C bus",
            justify="right",
        )

        for device_name, data in (
            devices.items()
        ):

            available = bool(
                data.get(
                    "available"
                )
            )

            if available:

                availability_text = Text(
                    "AVAILABLE",
                    style="green",
                )

            else:

                availability_text = Text(
                    "UNAVAILABLE",
                    style="bold yellow",
                )

            bus = data.get(
                "bus"
            )

            device_table.add_row(
                device_name.upper(),
                availability_text,
                (
                    str(bus)
                    if bus is not None
                    else "—"
                ),
            )

        self.console.print(
            device_table
        )

        # ================================================================
        # Summary
        # ================================================================

        has_alarm = bool(
            summary.get(
                "alarm"
            )
        )

        unavailable_quantity = bool(
            summary.get(
                "unavailable_quantity"
            )
        )

        unavailable_device = bool(
            summary.get(
                "unavailable_device"
            )
        )

        if has_alarm:

            overall_text = Text(
                "ALARM",
                style="bold red",
            )

        elif (
            unavailable_quantity
            or unavailable_device
        ):

            overall_text = Text(
                "DEGRADED",
                style="bold yellow",
            )

        else:

            overall_text = Text(
                "OK",
                style="bold green",
            )

        summary_text = Text()

        summary_text.append(
            "Overall sensor status: "
        )

        summary_text.append_text(
            overall_text
        )

        summary_text.append(
            "\nThreshold alarm: "
        )

        summary_text.append(
            "YES" if has_alarm else "NO",
            style=(
                "bold red"
                if has_alarm
                else "green"
            ),
        )

        summary_text.append(
            "\nUnavailable quantities: "
        )

        summary_text.append(
            (
                "YES"
                if unavailable_quantity
                else "NO"
            ),
            style=(
                "bold yellow"
                if unavailable_quantity
                else "green"
            ),
        )

        summary_text.append(
            "\nUnavailable devices: "
        )

        summary_text.append(
            (
                "YES"
                if unavailable_device
                else "NO"
            ),
            style=(
                "bold yellow"
                if unavailable_device
                else "green"
            ),
        )

        self.console.print(
            Panel(
                summary_text,
                title="MAIN — Sensor Summary",
                expand=False,
            )
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
        
    
    def handle_configuration_event(
        self,
        client_id: bytes,
        message,
    ) -> bool:

        payload = message.payload or {}

        if payload.get("event") != (
            "rc_configuration_changed"
        ):
            return True

        details = payload.get(
            "details",
            {},
        )

        register = details.get(
            "register"
        )

        new_value = details.get(
            "new_value"
        )

        if register not in {
            31,
            39,
        }:
            return True

        timestamp_utc_ns = payload.get(
            "timestamp_utc_ns"
        )

        if timestamp_utc_ns is None:

            self.logger.warning(
                "Cannot record RC configuration "
                "history without synchronized timestamp: "
                f"client={client_id!r}, "
                f"register={register}"
            )

            return False

        return (
            self.monitor_persistence_service
            .record_configuration_change(
                client_id=client_id,
                register=register,
                new_value=new_value,
                timestamp_utc_ns=(
                    timestamp_utc_ns
                ),
            )
        )

    def save_start(self, args) -> None:

        client_ids = self._resolve_targets(
            args
        )

        if not client_ids:

            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )

            return

        #
        # One monitoring persistence session is shared
        # by samples and events.
        #
        if not (
            self.monitor_persistence_service
            .ensure_session()
        ):

            self.poutput(
                "Failed to create monitoring "
                "persistence session."
            )

            return

        (
            sample_sections,
            events_selected,
        ) = self._resolve_save_selection(
            args
        )

        interval_s = args.interval

        requested_interval_ns = int(
            interval_s
            * 1_000_000_000
        )

        for client_id in client_ids:

            client_name = client_id.decode(
                errors="ignore"
            )

            # ============================================================
            # Initialize metadata once per client/session
            # ============================================================

            existing_metadata = (
                self.monitor_persistence_service
                .get_client_metadata(
                    client_id=client_id
                )
            )

            if existing_metadata is None:

                metadata = (
                    self._build_persistence_metadata(
                        client_id
                    )
                )

                if metadata is None:

                    self.poutput(
                        f"{client_name}: failed to build "
                        "monitoring persistence metadata"
                    )

                    continue

                metadata_ok = (
                    self.monitor_persistence_service
                    .initialize_client_metadata(
                        client_id=client_id,
                        metadata=metadata,
                    )
                )

                if not metadata_ok:

                    self.poutput(
                        f"{client_name}: failed to initialize "
                        "monitoring persistence metadata"
                    )

                    continue

            # ============================================================
            # Periodic sample persistence
            # ============================================================

            for section in sample_sections:

                prepared = (
                    self.monitor_persistence_service
                    ._prepare_stream(
                        client_id=client_id,
                        section=section,
                        save_format=args.save_format,
                        requested_interval_ns=(
                            requested_interval_ns
                        ),
                    )
                )

                if not prepared:

                    self.poutput(
                        f"{client_name}: failed to prepare "
                        f"{section.value.upper()} persistence"
                    )

                    continue

                activated = (
                    self.monitor_persistence_service
                    .activate_stream(
                        client_id=client_id,
                        section=section,
                    )
                )

                if not activated:

                    self.poutput(
                        f"{client_name}: failed to activate "
                        f"{section.value.upper()} persistence"
                    )

                    continue

                #
                # Samples require producer orchestration.
                #
                subscribed = (
                    self.monitor_stream_service
                    .subscribe(
                        client_id=client_id,
                        section=section,
                        consumer="persistence",
                        requested_interval_ns=(
                            requested_interval_ns
                        ),
                    )
                )

                if not subscribed:

                    self.monitor_persistence_service.deactivate_stream(
                        client_id=client_id,
                        section=section,
                    )

                    self.poutput(
                        f"{client_name}: failed to enable "
                        f"{section.value.upper()} "
                        "monitoring persistence"
                    )

                    continue

                metadata_updated = (
                    self.monitor_persistence_service
                    .update_stream_metadata(
                        client_id=client_id,
                        section=section,
                        enabled=True,
                        save_format=args.save_format,
                        requested_interval_ns=(
                            requested_interval_ns
                        ),
                    )
                )

                if not metadata_updated:

                    #
                    # Full rollback.
                    #
                    self.monitor_stream_service.unsubscribe(
                        client_id=client_id,
                        section=section,
                        consumer="persistence",
                    )

                    self.monitor_persistence_service.deactivate_stream(
                        client_id=client_id,
                        section=section,
                    )

                    self.poutput(
                        f"{client_name}: failed to update "
                        f"{section.value.upper()} metadata"
                    )

                    continue

                self.poutput(
                    f"{client_name}: "
                    f"{section.value.upper()} monitoring "
                    f"persistence enabled "
                    f"(interval={interval_s}s)"
                )

            # ============================================================
            # Event persistence
            # ============================================================

            if not events_selected:
                continue

            prepared = (
                self.monitor_persistence_service
                .prepare_events(
                    client_id=client_id,
                    save_format=args.save_format,
                )
            )

            if not prepared:

                self.poutput(
                    f"{client_name}: failed to prepare "
                    "EVENT persistence"
                )

                continue

            activated = (
                self.monitor_persistence_service
                .activate_events(
                    client_id=client_id
                )
            )

            if not activated:

                self.poutput(
                    f"{client_name}: failed to activate "
                    "EVENT persistence"
                )

                continue

            metadata_updated = (
                self.monitor_persistence_service
                .update_event_metadata(
                    client_id=client_id,
                    enabled=True,
                    save_format=args.save_format,
                )
            )

            if not metadata_updated:

                self.monitor_persistence_service.deactivate_events(
                    client_id=client_id
                )

                self.poutput(
                    f"{client_name}: failed to update "
                    "EVENT persistence metadata"
                )

                continue

            self.poutput(
                f"{client_name}: "
                "EVENT monitoring persistence enabled"
            )


    def save_stop(self, args) -> None:

        client_ids = self._resolve_targets(
            args
        )

        if not client_ids:

            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )

            return

        (
            sample_sections,
            events_selected,
        ) = self._resolve_save_selection(
            args
        )

        for client_id in client_ids:

            client_name = client_id.decode(
                errors="ignore"
            )

            # ============================================================
            # Periodic sample streams
            # ============================================================

            for section in sample_sections:

                stream_state = (
                    self.monitor_persistence_service
                    .get_stream(
                        client_id=client_id,
                        section=section,
                    )
                )

                if stream_state is None:

                    self.poutput(
                        f"{client_name}: "
                        f"{section.value.upper()} persistence "
                        "is not configured"
                    )

                    continue

                #
                # First stop future delivery to persistence.
                #
                unsubscribed = (
                    self.monitor_stream_service
                    .unsubscribe(
                        client_id=client_id,
                        section=section,
                        consumer="persistence",
                    )
                )

                if not unsubscribed:

                    self.poutput(
                        f"{client_name}: failed to disable "
                        f"{section.value.upper()} monitoring "
                        "subscription"
                    )

                    continue

                #
                # Once unsubscribed, no new persistence samples
                # should be selected for this stream.
                #
                deactivated = (
                    self.monitor_persistence_service
                    .deactivate_stream(
                        client_id=client_id,
                        section=section,
                    )
                )

                if not deactivated:

                    self.poutput(
                        f"{client_name}: failed to deactivate "
                        f"{section.value.upper()} persistence"
                    )

                    continue

                #
                # Wait for all previously accepted items.
                #
                idle = (
                    self.monitor_persistence_service
                    .wait_stream_idle(
                        client_id=client_id,
                        section=section,
                        timeout_s=30.0,
                    )
                )

                if not idle:

                    self.poutput(
                        f"{client_name}: timeout waiting for "
                        f"{section.value.upper()} persistence "
                        "queue to drain"
                    )

                    continue

                #
                # Flush residual partial batch and wait for any
                # write already in flight.
                #
                flushed = (
                    self.monitor_persistence_service
                    .flush_stream(
                        client_id=client_id,
                        section=section,
                    )
                )

                if not flushed:

                    self.poutput(
                        f"{client_name}: failed to flush "
                        f"{section.value.upper()} persistence"
                    )

                    continue

                metadata_ok = (
                    self.monitor_persistence_service
                    .update_stream_metadata(
                        client_id=client_id,
                        section=section,
                        enabled=False,
                        save_format=(
                            stream_state.save_format
                        ),
                        requested_interval_ns=(
                            stream_state
                            .requested_interval_ns
                        ),
                    )
                )

                if not metadata_ok:

                    self.poutput(
                        f"{client_name}: "
                        f"{section.value.upper()} stopped, "
                        "but metadata update failed"
                    )

                    continue

                self.poutput(
                    f"{client_name}: "
                    f"{section.value.upper()} monitoring "
                    "persistence disabled"
                )

            # ============================================================
            # Events
            # ============================================================

            if not events_selected:
                continue

            event_state = (
                self.monitor_persistence_service
                .get_event_state(
                    client_id=client_id
                )
            )

            if event_state is None:

                self.poutput(
                    f"{client_name}: "
                    "EVENT persistence is not configured"
                )

                continue

            #
            # Prevent new EVENT items from being accepted.
            #
            deactivated = (
                self.monitor_persistence_service
                .deactivate_events(
                    client_id=client_id
                )
            )

            if not deactivated:

                self.poutput(
                    f"{client_name}: failed to deactivate "
                    "EVENT persistence"
                )

                continue

            #
            # Wait until every EVENT accepted before deactivation
            # has completed process_event_item().
            #
            idle = (
                self.monitor_persistence_service
                .wait_events_idle(
                    client_id=client_id,
                    timeout_s=30.0,
                )
            )

            if not idle:

                self.poutput(
                    f"{client_name}: timeout waiting for "
                    "EVENT persistence queue to drain"
                )

                continue

            flushed = (
                self.monitor_persistence_service
                .flush_events(
                    client_id=client_id
                )
            )

            if not flushed:

                self.poutput(
                    f"{client_name}: failed to flush "
                    "EVENT persistence"
                )

                continue

            metadata_ok = (
                self.monitor_persistence_service
                .update_event_metadata(
                    client_id=client_id,
                    enabled=False,
                    save_format=(
                        event_state.save_format
                    ),
                )
            )

            if not metadata_ok:

                self.poutput(
                    f"{client_name}: EVENT persistence "
                    "stopped, but metadata update failed"
                )

                continue

            self.poutput(
                f"{client_name}: "
                "EVENT monitoring persistence disabled"
            )
    
    def save_status(self, args) -> None:

        client_ids = self._resolve_targets(
            args
        )

        if not client_ids:

            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )

            return

        session = (
            self.monitor_persistence_service
            .get_session()
        )

        queue_status = (
            self.monitor_persistence_service
            .get_queue_status()
        )

        # ================================================================
        # Global persistence state
        # ================================================================

        if session is None:

            self.poutput(
                "Monitoring persistence session: none"
            )

        else:

            self.poutput(
                f"Monitoring persistence session: "
                f"{session.session_id}\n"
                f"Folder: {session.root_folder}"
            )

        self.poutput(
            f"Persistence queue: "
            f"{queue_status['size']}/"
            f"{queue_status['maxsize']}"
        )

        # ================================================================
        # Client state
        # ================================================================

        for client_id in client_ids:

            client_name = client_id.decode(
                errors="ignore"
            )

            streams = (
                self.monitor_persistence_service
                .list_client_streams(
                    client_id
                )
            )

            event_state = (
                self.monitor_persistence_service
                .get_event_state(
                    client_id=client_id
                )
            )

            self.poutput(
                f"\n{client_name}:"
            )

            if not streams and event_state is None:

                self.poutput(
                    "  no persistence streams configured"
                )

                continue

            # ============================================================
            # Periodic sample streams
            # ============================================================

            for stream in streams:

                self.poutput(
                    f"  {stream.section.value.upper()}: "
                    f"{'ACTIVE' if stream.enabled else 'inactive'}, "
                    f"format={stream.save_format}, "
                    f"interval="
                    f"{stream.requested_interval_ns / 1e9:.3f}s, "
                    f"enqueued={stream.samples_enqueued}, "
                    f"written={stream.samples_written}, "
                    f"dropped={stream.samples_dropped}, "
                    f"pending={stream.queue_pending}, "
                    f"rows={stream.rows_written}, "
                    f"error={stream.last_error or '-'}"
                )

            # ============================================================
            # Events
            # ============================================================

            if event_state is not None:

                self.poutput(
                    "  EVENTS: "
                    f"{'ACTIVE' if event_state.enabled else 'inactive'}, "
                    f"format={event_state.save_format}, "
                    f"enqueued={event_state.events_enqueued}, "
                    f"written={event_state.events_written}, "
                    f"dropped={event_state.events_dropped}, "
                    f"pending={event_state.queue_pending}, "
                    f"rows={event_state.rows_written}, "
                    f"error={event_state.last_error or '-'}"
                )
    
    def sensors_status(
        self,
        args,
    ) -> None:

        client_ids = (
            self._resolve_targets(
                args
            )
        )

        if not client_ids:

            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )

            return

        for client_id in client_ids:

            try:

                status = (
                    self.monitoring_service
                    .read_sensor_status(
                        client_id=client_id,
                    )
                )

            except Exception as exc:

                self.logger.exception(
                    "Failed to retrieve MAIN "
                    "sensor status: "
                    f"client={client_id!r}, "
                    f"error={exc}"
                )

                status = {
                    "success": False,
                    "result": {},
                    "error": str(exc),
                }

            self._print_sensor_status(
                client_id=client_id,
                status=status,
            )
    
    
    def show_start(
        self,
        args,
    ) -> None:

        client_ids = (
            self._resolve_targets(
                args
            )
        )

        if not client_ids:

            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )

            return

        sections = (
            self._resolve_stream_sections(
                args
            )
        )

        requested_interval_ns = int(
            args.interval
            * 1_000_000_000
        )

        for client_id in client_ids:

            client_name = client_id.decode(
                errors="ignore"
            )

            for section in sections:

                subscribed = (
                    self.monitor_stream_service
                    .subscribe(
                        client_id=client_id,
                        section=section,
                        consumer="prometheus",
                        requested_interval_ns=(
                            requested_interval_ns
                        ),
                    )
                )

                if not subscribed:

                    self.poutput(
                        f"{client_name}: failed to enable "
                        f"{section.value.upper()} "
                        "visualization"
                    )

                    continue

                self.poutput(
                    f"{client_name}: "
                    f"{section.value.upper()} "
                    "visualization enabled "
                    f"(requested interval="
                    f"{args.interval:.3f}s)"
                )
    
    
    def show_stop(
        self,
        args,
    ) -> None:

        client_ids = (
            self._resolve_targets(
                args
            )
        )

        if not client_ids:

            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )

            return

        sections = (
            self._resolve_stream_sections(
                args
            )
        )

        for client_id in client_ids:

            client_name = client_id.decode(
                errors="ignore"
            )

            for section in sections:

                stream_state = (
                    self.monitor_stream_service
                    .get_stream(
                        client_id=client_id,
                        section=section,
                    )
                )

                if stream_state is None:

                    self.poutput(
                        f"{client_name}: "
                        f"{section.value.upper()} "
                        "visualization is not active"
                    )

                    continue

                subscription = (
                    stream_state.consumers.get(
                        "prometheus"
                    )
                )

                if subscription is None:

                    self.poutput(
                        f"{client_name}: "
                        f"{section.value.upper()} "
                        "visualization is not active"
                    )

                    continue

                unsubscribed = (
                    self.monitor_stream_service
                    .unsubscribe(
                        client_id=client_id,
                        section=section,
                        consumer="prometheus",
                    )
                )

                if not unsubscribed:

                    self.poutput(
                        f"{client_name}: failed to disable "
                        f"{section.value.upper()} "
                        "visualization"
                    )

                    continue

                self.poutput(
                    f"{client_name}: "
                    f"{section.value.upper()} "
                    "visualization disabled"
                )
    
    def show_status(
        self,
        args,
    ) -> None:

        client_ids = (
            self._resolve_targets(
                args
            )
        )

        if not client_ids:

            self.poutput(
                "No Monitoring Plane clients match "
                "the requested target."
            )

            return

        sections = (
            Channel.MAIN,
            Channel.RC,
            Channel.HV,
        )

        for client_id in client_ids:

            client_name = client_id.decode(
                errors="ignore"
            )

            self.poutput(
                f"\n{client_name}:"
            )

            for section in sections:

                stream_state = (
                    self.monitor_stream_service
                    .get_stream(
                        client_id=client_id,
                        section=section,
                    )
                )

                if stream_state is None:

                    self.poutput(
                        f"  {section.value.upper()}: inactive"
                    )

                    continue

                subscription = (
                    stream_state.consumers.get(
                        "prometheus"
                    )
                )

                if subscription is None:

                    self.poutput(
                        f"  {section.value.upper()}: inactive"
                    )

                    continue

                requested_interval_s = (
                    subscription.requested_interval_ns
                    / 1_000_000_000
                )

                producer_interval_ns = (
                    stream_state.active_producer_interval_ns
                )

                if producer_interval_ns is None:

                    producer_interval_text = (
                        "inactive"
                    )

                else:

                    producer_interval_text = (
                        f"{producer_interval_ns / 1e9:.3f}s"
                    )

                self.poutput(
                    f"  {section.value.upper()}: "
                    "ACTIVE, "
                    f"requested_interval="
                    f"{requested_interval_s:.3f}s, "
                    f"producer_interval="
                    f"{producer_interval_text}"
                )