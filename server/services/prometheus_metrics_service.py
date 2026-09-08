import time

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    start_http_server,
)

from prometheus_client.core import (
    GaugeMetricFamily,
)

from common.message_handler import (
    Channel,
    ProtocolMessage,
)

from server.core.server_state import (
    ServerState,
    ServerFSM,
    ClientFSM,
)

from server.services.monitor_stream_service import (
    StreamSubscription,
)

from server.utils.logger import get_logger


# =====================================================================
# Prometheus runtime collector
#
# These metrics are evaluated when Prometheus scrapes /metrics.
#
# IMPORTANT:
# - no hardware access
# - no commands are sent to clients
# - no additional polling thread
# - only thread-safe/read-only service state is inspected
# =====================================================================


class PrometheusRuntimeCollector:

    CLIENT_LABEL_NAMES = (
        "client_id",
        "multipmt_id",
        "batch_id",
    )

    def __init__(
        self,
        *,
        server_state,
        time_sync_service,
        monitor_stream_service,
        monitor_persistence_service,
        data_receiver_service,
    ) -> None:

        self.server_state = server_state
        self.time_sync_service = (
            time_sync_service
        )
        self.monitor_stream_service = (
            monitor_stream_service
        )
        self.monitor_persistence_service = (
            monitor_persistence_service
        )
        self.data_receiver_service = (
            data_receiver_service
        )

        self.logger = get_logger(
            "prometheus_runtime_collector"
        )


    # ================================================================
    # Generic helpers
    # ================================================================

    @staticmethod
    def _client_id_to_string(
        client_id: bytes,
    ) -> str:

        try:

            return client_id.decode(
                "utf-8",
                errors="strict",
            )

        except UnicodeDecodeError:

            return client_id.hex()


    def _get_client_label_values(
        self,
        client_id: bytes,
    ) -> list[str]:

        identity = (
            self.server_state.get_identity(
                client_id
            )
            or {}
        )

        return [
            self._client_id_to_string(
                client_id
            ),
            str(
                identity.get(
                    "multipmt_id",
                    "",
                )
            ),
            str(
                identity.get(
                    "batch_id",
                    "",
                )
            ),
        ]


    def _get_known_clients(
        self,
    ) -> list[bytes]:

        
        states = (
            self.server_state
            .get_client_states()
        )

        return list(
            states.keys()
        )


    # ================================================================
    # Server state
    # ================================================================

    def _collect_server_state(
        self,
    ):

        metric = GaugeMetricFamily(
            "multidaq_server_state",
            (
                "Current multiDAQ server FSM state. "
                "Exactly one state has value 1."
            ),
            labels=[
                "state",
            ],
        )

        current_state = (
            self.server_state
            .get_server_state()
        )

        for state in ServerFSM:

            metric.add_metric(
                [
                    state.value,
                ],
                (
                    1
                    if state == current_state
                    else 0
                ),
            )

        yield metric


    # ================================================================
    # Connected client counts
    # ================================================================

    def _collect_client_counts(
        self,
    ):

        control_clients = (
            self.server_state
            .list_connected_clients()
        )

        acquisition_clients = (
            self.server_state
            .list_acquisition_clients()
        )

        monitoring_clients = (
            self.server_state
            .list_monitoring_clients()
        )

        control_metric = GaugeMetricFamily(
            "multidaq_clients_control_connected",
            (
                "Number of clients currently connected "
                "to the Control Plane."
            ),
        )

        control_metric.add_metric(
            [],
            len(control_clients),
        )

        yield control_metric


        acquisition_metric = (
            GaugeMetricFamily(
                "multidaq_clients_acquisition_connected",
                (
                    "Number of clients currently connected "
                    "to the Acquisition Plane."
                ),
            )
        )

        acquisition_metric.add_metric(
            [],
            len(acquisition_clients),
        )

        yield acquisition_metric


        monitoring_metric = (
            GaugeMetricFamily(
                "multidaq_clients_monitoring_connected",
                (
                    "Number of clients currently connected "
                    "to the Monitoring Plane."
                ),
            )
        )

        monitoring_metric.add_metric(
            [],
            len(monitoring_clients),
        )

        yield monitoring_metric


    # ================================================================
    # Per-client connectivity
    # ================================================================

    def _collect_client_connectivity(
        self,
    ):

        label_names = list(
            self.CLIENT_LABEL_NAMES
        )

        control_metric = GaugeMetricFamily(
            "multidaq_client_control_connected",
            (
                "Whether the client is connected "
                "to the Control Plane."
            ),
            labels=label_names,
        )

        acquisition_metric = GaugeMetricFamily(
            "multidaq_client_acquisition_connected",
            (
                "Whether the client is connected "
                "to the Acquisition Plane."
            ),
            labels=label_names,
        )

        monitoring_metric = GaugeMetricFamily(
            "multidaq_client_monitoring_connected",
            (
                "Whether the client is connected "
                "to the Monitoring Plane."
            ),
            labels=label_names,
        )

        operational_metric = GaugeMetricFamily(
            "multidaq_client_operational",
            (
                "Whether the client belongs to the "
                "currently operational configured set."
            ),
            labels=label_names,
        )

        client_states = (
            self.server_state
            .get_client_states()
        )

        for client_id, client_state in (
            client_states.items()
        ):

            labels = (
                self._get_client_label_values(
                    client_id
                )
            )

            control_metric.add_metric(
                labels,
                int(
                    self.server_state
                    .is_client_on_plane(
                        client_id,
                        "control",
                    )
                ),
            )

            acquisition_metric.add_metric(
                labels,
                int(
                    self.server_state
                    .is_client_on_plane(
                        client_id,
                        "acquisition",
                    )
                ),
            )

            monitoring_metric.add_metric(
                labels,
                int(
                    self.server_state
                    .is_client_on_plane(
                        client_id,
                        "monitoring",
                    )
                ),
            )

            

            operational_metric.add_metric(
                labels,
                int(self.server_state.is_client_operational(client_id)),
            )

        yield control_metric
        yield acquisition_metric
        yield monitoring_metric
        yield operational_metric


    # ================================================================
    # Client FSM state
    # ================================================================

    def _collect_client_states(
        self,
    ):

        metric = GaugeMetricFamily(
            "multidaq_client_state",
            (
                "Current client FSM state. "
                "Exactly one state has value 1 "
                "for each registered client."
            ),
            labels=[
                *self.CLIENT_LABEL_NAMES,
                "state",
            ],
        )

        client_states = (
            self.server_state
            .get_client_states()
        )

        for client_id, current_state in (
            client_states.items()
        ):

            client_labels = (
                self._get_client_label_values(
                    client_id
                )
            )

            for state in ClientFSM:

                metric.add_metric(
                    [
                        *client_labels,
                        state.value,
                    ],
                    (
                        1
                        if state == current_state
                        else 0
                    ),
                )

        yield metric


    # ================================================================
    # Time synchronization
    # ================================================================

    def _collect_time_sync(
        self,
    ):

        label_names = list(
            self.CLIENT_LABEL_NAMES
        )

        sync_ok_metric = GaugeMetricFamily(
            "multidaq_time_sync_ok",
            (
                "Whether a valid client/server time "
                "synchronization state exists."
            ),
            labels=label_names,
        )

        rtt_metric = GaugeMetricFamily(
            "multidaq_time_sync_rtt_seconds",
            (
                "Network RTT of the selected "
                "time synchronization measurement."
            ),
            labels=label_names,
        )

        uncertainty_metric = GaugeMetricFamily(
            "multidaq_time_sync_uncertainty_seconds",
            (
                "Estimated time synchronization "
                "uncertainty."
            ),
            labels=label_names,
        )

        age_metric = GaugeMetricFamily(
            "multidaq_time_sync_age_seconds",
            (
                "Age of the current client time "
                "synchronization state."
            ),
            labels=label_names,
        )

        now_ns = time.monotonic_ns()

        for client_id in (
            self._get_known_clients()
        ):

            labels = (
                self._get_client_label_values(
                    client_id
                )
            )

            state = (
                self.time_sync_service
                .get_state(
                    client_id
                )
            )

            if state is None:

                sync_ok_metric.add_metric(
                    labels,
                    0,
                )

                continue

            sync_ok_metric.add_metric(
                labels,
                1,
            )

            rtt_metric.add_metric(
                labels,
                (
                    state.network_rtt_ns
                    / 1_000_000_000
                ),
            )

            uncertainty_metric.add_metric(
                labels,
                (
                    state.uncertainty_ns
                    / 1_000_000_000
                ),
            )

            age_ns = max(
                0,
                (
                    now_ns
                    - state
                    .synced_at_server_monotonic_ns
                ),
            )

            age_metric.add_metric(
                labels,
                (
                    age_ns
                    / 1_000_000_000
                ),
            )

        yield sync_ok_metric
        yield rtt_metric
        yield uncertainty_metric
        yield age_metric


    # ================================================================
    # Monitoring producer streams
    # ================================================================

    def _collect_monitor_streams(
        self,
    ):

        labels = [
            *self.CLIENT_LABEL_NAMES,
            "section",
        ]

        active_metric = GaugeMetricFamily(
            "multidaq_monitor_stream_active",
            (
                "Whether the monitoring sample producer "
                "is currently active."
            ),
            labels=labels,
        )

        interval_metric = GaugeMetricFamily(
            "multidaq_monitor_stream_interval_seconds",
            (
                "Actual active monitoring producer "
                "interval in seconds."
            ),
            labels=labels,
        )

        streams = (
            self.monitor_stream_service
            .list_streams()
        )

        for stream in streams:

            client_labels = (
                self._get_client_label_values(
                    stream.client_id
                )
            )

            metric_labels = [
                *client_labels,
                stream.section.value,
            ]

            active_metric.add_metric(
                metric_labels,
                int(stream.active),
            )

            if (
                stream.active
                and stream
                .active_producer_interval_ns
                is not None
            ):

                interval_metric.add_metric(
                    metric_labels,
                    (
                        stream
                        .active_producer_interval_ns
                        / 1_000_000_000
                    ),
                )

        yield active_metric
        yield interval_metric


    # ================================================================
    # Persistence
    # ================================================================

    def _collect_persistence(
        self,
    ):

        status = (
            self.monitor_persistence_service
            .get_status()
        )

        running_metric = GaugeMetricFamily(
            "multidaq_persistence_running",
            (
                "Whether the monitoring persistence "
                "runtime is active."
            ),
        )

        running_metric.add_metric(
            [],
            int(
                bool(
                    status.get(
                        "running",
                        False,
                    )
                )
            ),
        )

        yield running_metric


        queue_status = (
            status.get(
                "queue",
                {},
            )
            or {}
        )

        queue_size = int(
            queue_status.get(
                "size",
                0,
            )
        )

        queue_capacity = int(
            queue_status.get(
                "maxsize",
                0,
            )
        )

        queue_size_metric = (
            GaugeMetricFamily(
                "multidaq_persistence_queue_size",
                (
                    "Current number of items in the "
                    "monitoring persistence queue."
                ),
            )
        )

        queue_size_metric.add_metric(
            [],
            queue_size,
        )

        yield queue_size_metric


        queue_capacity_metric = (
            GaugeMetricFamily(
                "multidaq_persistence_queue_capacity",
                (
                    "Maximum monitoring persistence "
                    "queue capacity."
                ),
            )
        )

        queue_capacity_metric.add_metric(
            [],
            queue_capacity,
        )

        yield queue_capacity_metric


        queue_fill_metric = (
            GaugeMetricFamily(
                "multidaq_persistence_queue_fill_ratio",
                (
                    "Fraction of monitoring persistence "
                    "queue capacity currently used."
                ),
            )
        )

        queue_fill_ratio = (
            queue_size / queue_capacity
            if queue_capacity > 0
            else 0.0
        )

        queue_fill_metric.add_metric(
            [],
            queue_fill_ratio,
        )

        yield queue_fill_metric


        # ------------------------------------------------------------
        # Periodic sample persistence streams
        # ------------------------------------------------------------

        stream_labels = [
            *self.CLIENT_LABEL_NAMES,
            "section",
        ]

        stream_active_metric = (
            GaugeMetricFamily(
                "multidaq_persistence_stream_active",
                (
                    "Whether persistence is active "
                    "for the client monitoring stream."
                ),
                labels=stream_labels,
            )
        )

        samples_dropped_metric = (
            GaugeMetricFamily(
                "multidaq_persistence_samples_dropped",
                (
                    "Current persistence sample drop "
                    "counter for the client stream."
                ),
                labels=stream_labels,
            )
        )

        streams = (
            status.get(
                "streams",
                [],
            )
            or []
        )

        for stream in streams:

            client_labels = (
                self._get_client_label_values(
                    stream.client_id
                )
            )

            labels = [
                *client_labels,
                stream.section.value,
            ]

            stream_active_metric.add_metric(
                labels,
                int(
                    bool(
                        stream.enabled
                    )
                ),
            )

            samples_dropped_metric.add_metric(
                labels,
                stream.samples_dropped,
            )

        yield stream_active_metric
        yield samples_dropped_metric


        # ------------------------------------------------------------
        # EVENT persistence
        # ------------------------------------------------------------

        event_dropped_metric = (
            GaugeMetricFamily(
                "multidaq_persistence_events_dropped",
                (
                    "Current persistence EVENT drop "
                    "counter for the client."
                ),
                labels=list(
                    self.CLIENT_LABEL_NAMES
                ),
            )
        )

        events = (
            status.get(
                "events",
                [],
            )
            or []
        )

        for event_state in events:

            labels = (
                self._get_client_label_values(
                    event_state.client_id
                )
            )

            event_dropped_metric.add_metric(
                labels,
                event_state.events_dropped,
            )

        yield event_dropped_metric


    # ================================================================
    # Acquisition receiver
    # ================================================================

    def _collect_receiver(
        self,
    ):

        receiver_metric = GaugeMetricFamily(
            "multidaq_receiver_running",
            (
                "Whether the persistent evreceiver "
                "process is running."
            ),
        )

        receiver_metric.add_metric(
            [],
            int(
                self.data_receiver_service
                .is_running()
            ),
        )

        yield receiver_metric


        acquisition_metric = (
            GaugeMetricFamily(
                "multidaq_acquisition_active",
                (
                    "Whether the server FSM is currently "
                    "in the ACQUIRING state."
                ),
            )
        )

        acquisition_metric.add_metric(
            [],
            int(
                self.server_state
                .get_server_state()
                == ServerFSM.ACQUIRING
            ),
        )

        yield acquisition_metric


    # ================================================================
    # Main collect entry point
    # ================================================================

    def collect(
        self,
    ):

        collectors = (
            self._collect_server_state,
            self._collect_client_counts,
            self._collect_client_connectivity,
            self._collect_client_states,
            self._collect_time_sync,
            self._collect_monitor_streams,
            self._collect_persistence,
            self._collect_receiver,
        )

        for collector in collectors:

            try:

                yield from collector()

            except Exception as exc:

                #
                # One faulty runtime metric group must
                # not make the whole /metrics endpoint
                # unusable.
                #
                self.logger.exception(
                    "Failed to collect Prometheus "
                    f"runtime metrics from "
                    f"{collector.__name__}: "
                    f"{exc}"
                )


# =====================================================================
# Prometheus Metrics Service
# =====================================================================


class PrometheusMetricsService:

    def __init__(
        self,
        *,
        server_state: ServerState,
        time_sync_service,
        monitor_stream_service,
        monitor_persistence_service,
        data_receiver_service,
        mac_identity_registry,
    ) -> None:

        self.server_state = server_state
        self.mac_identity_registry = mac_identity_registry

        self.registry = CollectorRegistry()
        self._acquisition_counter_state = {}
        self._acquisition_rate_state = {}

        self._http_server = None
        self._http_thread = None

        self.logger = get_logger(
            "prometheus_metrics_service"
        )

        # ============================================================
        # SAMPLE-driven metrics
        # ============================================================

        self._create_main_metrics()
        self._create_hv_metrics()
        self._create_rc_metrics()
        self._create_acquisition_metrics()

        # ============================================================
        # Scrape-driven runtime metrics
        # ============================================================

        self.runtime_collector = (
            PrometheusRuntimeCollector(
                server_state=server_state,
                time_sync_service=(
                    time_sync_service
                ),
                monitor_stream_service=(
                    monitor_stream_service
                ),
                monitor_persistence_service=(
                    monitor_persistence_service
                ),
                data_receiver_service=(
                    data_receiver_service
                ),
            )
        )

        self.registry.register(
            self.runtime_collector
        )

        self.logger.debug(
            "Prometheus Metrics Service initialized"
        )


    # ============================================================
    # Labels
    # ============================================================

    def _get_client_labels(
        self,
        client_id: bytes,
    ) -> dict[str, str]:

        try:

            client_label = client_id.decode(
                "utf-8",
                errors="strict",
            )

        except UnicodeDecodeError:

            client_label = client_id.hex()

        identity = (
            self.server_state.get_identity(
                client_id=client_id
            )
            or {}
        )

        return {
            "client_id": client_label,
            "multipmt_id": str(
                identity.get(
                    "multipmt_id",
                    "",
                )
            ),
            "batch_id": str(
                identity.get(
                    "batch_id",
                    "",
                )
            ),
        }


    @staticmethod
    def _with_channel_label(
        labels: dict[str, str],
        channel: int,
    ) -> dict[str, str]:

        return {
            **labels,
            "channel": str(channel),
        }


    @staticmethod
    def _with_register_label(
        labels: dict[str, str],
        register: int,
    ) -> dict[str, str]:

        return {
            **labels,
            "register": str(register),
        }


    def _set_gauge(
        self,
        gauge: Gauge,
        labels: dict[str, str],
        value,
    ) -> None:

        if value is None:
            return

        try:

            numeric_value = float(
                value
            )

        except (
            TypeError,
            ValueError,
        ):

            self.logger.debug(
                "Ignoring non-numeric Prometheus "
                f"value={value!r}"
            )

            return

        gauge.labels(
            **labels
        ).set(
            numeric_value
        )


    def _get_acquisition_client_labels(
        self,
        source_id: str,
    ) -> dict[str, str] | None:

        mac = (
            self.mac_identity_registry
            .get_mac_from_id(
                source_id
            )
        )

        if mac is None:

            self.logger.warning(
                "Cannot resolve acquisition "
                f"source_id={source_id!r}: "
                "numeric id is not present in "
                "the MAC registry"
            )

            return None


        client_id = (
            self.server_state
            .get_client_id_by_mac(
                mac
            )
        )

        if client_id is None:

            self.logger.warning(
                "Cannot resolve acquisition "
                f"source_id={source_id!r}: "
                f"MAC {mac!r} is not associated "
                "with a registered client"
            )

            return None


        return self._get_client_labels(
            client_id
        )


    # ============================================================
    # MAIN metrics
    # ============================================================

    def _create_main_metrics(
        self,
    ) -> None:

        labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
        )

        self.main_temperature = Gauge(
            "multidaq_main_temperature_celsius",
            "Main board environmental temperature",
            labels,
            registry=self.registry,
        )

        self.main_humidity = Gauge(
            "multidaq_main_humidity_percent",
            "Main board relative humidity",
            labels,
            registry=self.registry,
        )

        self.main_pressure = Gauge(
            "multidaq_main_pressure_hectopascals",
            "Main board atmospheric pressure",
            labels,
            registry=self.registry,
        )

        self.main_voltage_5v = Gauge(
            "multidaq_main_voltage_5v_volts",
            "Main board 5 V rail voltage",
            labels,
            registry=self.registry,
        )

        self.main_voltage_3v3 = Gauge(
            "multidaq_main_voltage_3v3_volts",
            "Main board 3.3 V rail voltage",
            labels,
            registry=self.registry,
        )

        self.main_current = Gauge(
            "multidaq_main_current_amperes",
            "Main board monitored current",
            labels,
            registry=self.registry,
        )

        self.main_fpga_temperature = Gauge(
            "multidaq_main_fpga_temperature_celsius",
            "FPGA temperature",
            labels,
            registry=self.registry,
        )
        
        self.main_sample_timestamp = Gauge(
            "multidaq_main_sample_timestamp_seconds",
            "UTC timestamp of the latest MAIN monitoring sample",
            labels,
            registry=self.registry
        )


    def _handle_main(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        payload = (
            message.payload
            or {}
        )

        data = (
            payload.get(
                "data"
            )
            or {}
        )

        env = (
            data.get(
                "env"
            )
            or {}
        )

        power = (
            data.get(
                "power"
            )
            or {}
        )

        fpga = (
            data.get(
                "fpga"
            )
            or {}
        )
        
        timestamp = payload.get("timestamp_utc_ns")
        timestamp_seconds = timestamp / 1_000_000_000 if timestamp is not None else None

        labels = (
            self._get_client_labels(
                client_id
            )
        )

        self._set_gauge(
            self.main_temperature,
            labels,
            env.get(
                "temperature_c"
            ),
        )

        self._set_gauge(
            self.main_humidity,
            labels,
            env.get(
                "humidity_pct"
            ),
        )

        self._set_gauge(
            self.main_pressure,
            labels,
            env.get(
                "pressure_hpa"
            ),
        )

        self._set_gauge(
            self.main_voltage_5v,
            labels,
            power.get(
                "rail_ain0_v"
            ),
        )

        self._set_gauge(
            self.main_voltage_3v3,
            labels,
            power.get(
                "rail_ain2_v"
            ),
        )

        self._set_gauge(
            self.main_current,
            labels,
            power.get(
                "i_mon_1_a"
            ),
        )

        self._set_gauge(
            self.main_fpga_temperature,
            labels,
            fpga.get(
                "temperature_c"
            ),
        )
        
        self._set_gauge(
            self.main_sample_timestamp,
            labels,
            timestamp_seconds
        )

        return True


    # ============================================================
    # HV metrics
    # ============================================================

    def _create_hv_metrics(
        self,
    ) -> None:

        channel_labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
            "channel",
        )

        client_labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
        )

        self.hv_voltage = Gauge(
            "multidaq_hv_voltage_volts",
            "HV channel measured voltage",
            channel_labels,
            registry=self.registry,
        )

        self.hv_current = Gauge(
            "multidaq_hv_current_amperes",
            "HV channel measured current",
            channel_labels,
            registry=self.registry,
        )

        self.hv_temperature = Gauge(
            "multidaq_hv_temperature_celsius",
            "HV channel temperature",
            channel_labels,
            registry=self.registry,
        )

        self.hv_channel_enabled = Gauge(
            "multidaq_hv_channel_enabled",
            (
                "Whether the HV channel is "
                "available for operation"
            ),
            channel_labels,
            registry=self.registry,
        )

        self.hv_power_enabled = Gauge(
            "multidaq_hv_power_enabled",
            (
                "Whether HV power is enabled "
                "for the channel"
            ),
            channel_labels,
            registry=self.registry,
        )

        #
        # Aggregate client-level status.
        #
        # Deliberately based only on channel_state:
        # HV power being OFF may be perfectly intentional.
        #
        self.hv_ok = Gauge(
            "multidaq_hv_ok",
            (
                "Whether all seven HV channels "
                "are operationally available"
            ),
            client_labels,
            registry=self.registry,
        )
        
        
        self.hv_sample_timestamp = Gauge(
            "multidaq_hv_sample_timestamp_seconds",
            "UTC timestamp of the latest HV monitoring sample",
            client_labels,
            registry=self.registry
        )


    def _handle_hv(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        payload = (
            message.payload
            or {}
        )

        data = (
            payload.get(
                "data"
            )
            or {}
        )

        electrical = (
            data.get(
                "electrical"
            )
            or {}
        )

        channels = (
            electrical.get(
                "channels"
            )
            or {}
        )

        client_labels = (
            self._get_client_labels(
                client_id
            )
        )

        all_channels_ok = True
        
        timestamp = payload.get("timestamp_utc_ns")
        timestamp_seconds = timestamp / 1_000_000_000 if timestamp is not None else None
        

        #
        # Hardware HV channels are 1..7.
        # User-facing multiDAQ channels are 0..6.
        #
        for raw_channel in range(
            1,
            8,
        ):

            channel_data = (
                channels.get(
                    raw_channel
                )
                or channels.get(
                    str(raw_channel)
                )
                or {}
            )

            user_channel = (
                raw_channel
                - 1
            )

            labels = (
                self._with_channel_label(
                    client_labels,
                    user_channel,
                )
            )

            self._set_gauge(
                self.hv_voltage,
                labels,
                channel_data.get(
                    "voltage"
                ),
            )

            self._set_gauge(
                self.hv_current,
                labels,
                channel_data.get(
                    "current"
                ),
            )

            self._set_gauge(
                self.hv_temperature,
                labels,
                channel_data.get(
                    "temperature"
                ),
            )

            channel_state = (
                channel_data.get(
                    "channel_state"
                )
            )

            power_state = (
                channel_data.get(
                    "power_state"
                )
            )

            #
            # Simple operational view.
            #
            channel_enabled = (
                1
                if channel_state == "ok"
                else 0
            )

            power_enabled = (
                1
                if power_state == "on"
                else 0
            )

            if channel_state != "ok":

                all_channels_ok = False

            self._set_gauge(
                self.hv_channel_enabled,
                labels,
                channel_enabled,
            )

            self._set_gauge(
                self.hv_power_enabled,
                labels,
                power_enabled,
            )

        self._set_gauge(
            self.hv_ok,
            client_labels,
            int(
                all_channels_ok
            ),
        )
        
        self._set_gauge(
            self.hv_sample_timestamp,
            client_labels,
            timestamp_seconds
        )

        return True


    # ============================================================
    # RC metrics
    # ============================================================

    def _create_rc_metrics(
        self,
    ) -> None:

        register_labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
            "register",
        )

        client_labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
        )

        self.rc_register_value = Gauge(
            "multidaq_rc_rate_hertz",
            "RC monitoring register value",
            register_labels,
            registry=self.registry,
        )

        self.rc_sample_timestamp = Gauge(
            "multidaq_rc_sample_timestamp_seconds",
            "UTC timestamp of the latest RC monitoring sample",
            client_labels,
            registry=self.registry,
        )


    def _set_rc_register(
        self,
        client_labels: dict[str, str],
        register: int,
        value,
    ) -> None:

        labels = (
            self._with_register_label(
                client_labels,
                register,
            )
        )

        self._set_gauge(
            self.rc_register_value,
            labels,
            value,
        )


    def _handle_rc(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        payload = (
            message.payload
            or {}
        )

        data = (
            payload.get(
                "data"
            )
            or {}
        )

        free = (
            data.get(
                "free"
            )
            or {}
        )

        trigger = (
            data.get(
                "trigger"
            )
            or {}
        )

        free_channels = (
            free.get(
                "channels"
            )
            or {}
        )

        trigger_channels = (
            trigger.get(
                "channels"
            )
            or {}
        )

        labels = (
            self._get_client_labels(
                client_id
            )
        )
        
        
        timestamp = payload.get("timestamp_utc_ns")
        timestamp_seconds = timestamp / 1_000_000_000 if timestamp is not None else None
                

        #
        # Free rates:
        # RC registers 20..26
        #
        for channel in range(
            7
        ):

            channel_data = (
                free_channels.get(
                    channel
                )
                or free_channels.get(
                    str(channel)
                )
                or {}
            )

            self._set_rc_register(
                labels,
                20 + channel,
                channel_data.get(
                    "value"
                ),
            )

        #
        # External trigger rate:
        # RC register 27
        #
        external_trigger = (
            trigger.get(
                "external_trigger_rate"
            )
            or {}
        )

        self._set_rc_register(
            labels,
            27,
            external_trigger.get(
                "value"
            ),
        )

        #
        # Auto trigger rate:
        # RC register 28
        #
        auto_trigger = (
            trigger.get(
                "auto_trigger_rate"
            )
            or {}
        )

        self._set_rc_register(
            labels,
            28,
            auto_trigger.get(
                "value"
            ),
        )

        #
        # Trigger channel rates:
        # RC registers 32..38
        #
        for channel in range(
            7
        ):

            channel_data = (
                trigger_channels.get(
                    channel
                )
                or trigger_channels.get(
                    str(channel)
                )
                or {}
            )

            self._set_rc_register(
                labels,
                32 + channel,
                channel_data.get(
                    "value"
                ),
            )
        
        
        self._set_gauge(
            self.rc_sample_timestamp,
            labels,
            timestamp_seconds
        )

        return True


    # ============================================================
    # ACQUISITION METRICS
    # ============================================================

    def _create_acquisition_metrics(
        self,
    ) -> None:

        source_labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
        )

        worker_labels = (
            "worker",
        )


        self.acquisition_events_received = Counter(
            "multidaq_acquisition_events_received_total",
            (
                "Total number of raw acquisition "
                "events received from the source"
            ),
            source_labels,
            registry=self.registry,
        )


        self.acquisition_events_written = Counter(
            "multidaq_acquisition_events_written_total",
            (
                "Total number of valid acquisition "
                "events accepted for persistence"
            ),
            source_labels,
            registry=self.registry,
        )


        self.acquisition_bytes_received = Counter(
            "multidaq_acquisition_bytes_received_total",
            (
                "Total number of raw acquisition "
                "bytes received from the source"
            ),
            source_labels,
            registry=self.registry,
        )


        self.acquisition_queue_size = Gauge(
            "multidaq_acquisition_queue_size",
            (
                "Current number of DMA batches "
                "queued for an evreceiver worker"
            ),
            worker_labels,
            registry=self.registry,
        )


        self.acquisition_queue_capacity = Gauge(
            "multidaq_acquisition_queue_capacity",
            (
                "Maximum number of DMA batches "
                "that can be queued for an "
                "evreceiver worker"
            ),
            worker_labels,
            registry=self.registry,
        )


        self.acquisition_queue_fill_ratio = Gauge(
            "multidaq_acquisition_queue_fill_ratio",
            (
                "Fraction of evreceiver worker "
                "queue capacity currently occupied"
            ),
            worker_labels,
            registry=self.registry,
        )


        self.acquisition_metrics_last_update = Gauge(
            (
                "multidaq_acquisition_metrics_"
                "last_update_timestamp_seconds"
            ),
            (
                "Unix timestamp of the most recent "
                "acquisition metrics update for "
                "the source"
            ),
            source_labels,
            registry=self.registry,
        )
        
        self.acquisition_event_rate = Gauge(
            "multidaq_acquisition_event_rate_hertz",
            (
                "Acquisition raw event rate calculated "
                "from consecutive receiver snapshots"
            ),
            source_labels,
            registry=self.registry,
        )

        self.acquisition_valid_event_rate = Gauge(
            "multidaq_acquisition_valid_event_rate_hertz",
            (
                "Acquisition valid event rate calculated "
                "from consecutive receiver snapshots"
            ),
            source_labels,
            registry=self.registry,
        )

        self.acquisition_data_rate = Gauge(
            "multidaq_acquisition_data_rate_bytes_per_second",
            (
                "Acquisition raw data rate calculated "
                "from consecutive receiver snapshots"
            ),
            source_labels,
            registry=self.registry,
        )

    def _update_acquisition_counter(
        self,
        *,
        counter: Counter,
        counter_name: str,
        labels: dict[str, str],
        source_id: str,
        value: int,
    ) -> None:

        if value < 0:
            return

        #
        # Keep the raw receiver source_id only
        # as an internal key for tracking the
        # process-local C counter.
        #
        key = (
            counter_name,
            source_id,
        )

        previous = (
            self._acquisition_counter_state
            .get(
                key
            )
        )

        if previous is None:

            delta = value

        elif value >= previous:

            delta = (
                value
                - previous
            )

        else:

            #
            # evreceiver restarted.
            #
            delta = value


        if delta > 0:

            counter.labels(
                **labels
            ).inc(
                delta
            )


        self._acquisition_counter_state[
            key
        ] = value


    def handle_acquisition_metrics(
        self,
        metric: dict,
    ) -> bool:

        metric_type = (
            metric.get(
                "type"
            )
        )


        try:

            if metric_type == "source":

                source_id = str(
                    metric.get(
                        "source_id",
                        ""
                    )
                )

                labels = (
                    self._get_acquisition_client_labels(
                        source_id
                    )
                )

                if labels is None:
                    return False


                events_received = int(
                    metric.get(
                        "events_received",
                        0,
                    )
                )

                events_written = int(
                    metric.get(
                        "events_written",
                        0,
                    )
                )

                bytes_received = int(
                    metric.get(
                        "bytes_received",
                        0,
                    )
                )
                
                timestamp_monotonic_ns = int(
                    metric.get(
                        "timestamp_monotonic_ns",
                        0,
                    )
                )


                self._update_acquisition_counter(
                    counter=(
                        self
                        .acquisition_events_received
                    ),
                    counter_name=(
                        "events_received"
                    ),
                    labels=labels,
                    source_id=source_id,
                    value=events_received,
                )


                self._update_acquisition_counter(
                    counter=(
                        self
                        .acquisition_events_written
                    ),
                    counter_name=(
                        "events_written"
                    ),
                    labels=labels,
                    source_id=source_id,
                    value=events_written,
                )


                self._update_acquisition_counter(
                    counter=(
                        self
                        .acquisition_bytes_received
                    ),
                    counter_name=(
                        "bytes_received"
                    ),
                    labels=labels,
                    source_id=source_id,
                    value=bytes_received,
                )
                
                previous = (
                    self._acquisition_rate_state
                    .get(source_id)
                )
                
                if previous is not None:
                    previous_timestamp_ns = (
                        previous[
                            "timestamp_monotonic_ns"
                        ]
                    )

                    dt_ns = (
                        timestamp_monotonic_ns
                        - previous_timestamp_ns
                    )

                    counters_valid = (
                        events_received
                        >= previous["events_received"]
                        and events_written
                        >= previous["events_written"]
                        and bytes_received
                        >= previous["bytes_received"]
                    )

                    if (
                        dt_ns > 0
                        and counters_valid
                    ):

                        dt_s = (
                            dt_ns
                            / 1_000_000_000
                        )

                        event_rate = (
                            events_received
                            - previous["events_received"]
                        ) / dt_s

                        valid_event_rate = (
                            events_written
                            - previous["events_written"]
                        ) / dt_s

                        data_rate = (
                            bytes_received
                            - previous["bytes_received"]
                        ) / dt_s

                        self.acquisition_event_rate.labels(
                            **labels
                        ).set(
                            event_rate
                        )

                        self.acquisition_valid_event_rate.labels(
                            **labels
                        ).set(
                            valid_event_rate
                        )

                        self.acquisition_data_rate.labels(
                            **labels
                        ).set(
                            data_rate
                        )
                        
                        
                self._acquisition_rate_state[
                    source_id
                ] = {
                    "events_received": events_received,
                    "events_written": events_written,
                    "bytes_received": bytes_received,
                    "timestamp_monotonic_ns": (
                        timestamp_monotonic_ns
                    ),
                }


                self.acquisition_metrics_last_update.labels(
                    **labels
                ).set(
                    time.time()
                )

                return True


            if metric_type == "worker":

                worker = str(
                    metric.get(
                        "worker"
                    )
                )

                queue_size = int(
                    metric.get(
                        "queue_size",
                        0,
                    )
                )

                queue_capacity = int(
                    metric.get(
                        "queue_capacity",
                        0,
                    )
                )


                self.acquisition_queue_size.labels(
                    worker=worker
                ).set(
                    queue_size
                )


                self.acquisition_queue_capacity.labels(
                    worker=worker
                ).set(
                    queue_capacity
                )


                fill_ratio = (
                    queue_size
                    / queue_capacity
                    if queue_capacity > 0
                    else 0.0
                )


                self.acquisition_queue_fill_ratio.labels(
                    worker=worker
                ).set(
                    fill_ratio
                )

                return True

            if metric_type == "source_stop":
                source_id = metric.get("source_id", "")
                labels = self._get_acquisition_client_labels(source_id)
                if labels is None:
                    return False
                self.acquisition_event_rate.labels(**labels).set(0.0)
                self.acquisition_valid_event_rate.labels(**labels).set(0.0)
                self.acquisition_data_rate.labels(**labels).set(0.0)
                self._acquisition_rate_state.pop(source_id,  None)
                
                return True

            self.logger.warning(
                "Unsupported acquisition "
                f"metric type={metric_type!r}"
            )

            return False


        except Exception as exc:

            self.logger.exception(
                "Failed to update acquisition "
                f"Prometheus metrics: {exc}"
            )

            return False


    # ============================================================
    # SAMPLE dispatcher entry point
    # ============================================================

    def handle_sample(
        self,
        client_id: bytes,
        message: ProtocolMessage,
        subscription: StreamSubscription,
    ) -> bool:

        section = (
            message.channel
        )

        try:

            if section == Channel.MAIN:

                return self._handle_main(
                    client_id,
                    message,
                )

            if section == Channel.HV:

                return self._handle_hv(
                    client_id,
                    message,
                )

            if section == Channel.RC:

                return self._handle_rc(
                    client_id,
                    message,
                )

            self.logger.warning(
                "Unsupported Prometheus monitoring "
                f"section={section}"
            )

            return False

        except Exception as exc:

            self.logger.exception(
                "Failed to update Prometheus metrics: "
                f"client={client_id!r}, "
                f"section={section}: "
                f"{exc}"
            )

            return False


    # ============================================================
    # HTTP exporter
    # ============================================================

    def start_exporter(
        self,
        host: str,
        port: int,
    ) -> bool:

        if self._http_server is not None:

            self.logger.debug(
                "Prometheus exporter already running"
            )

            return True

        try:

            (
                http_server,
                http_thread,
            ) = start_http_server(
                port=port,
                addr=host,
                registry=self.registry,
            )

        except Exception as exc:

            self.logger.exception(
                "Failed to start Prometheus exporter: "
                f"host={host}, "
                f"port={port}, "
                f"error={exc}"
            )

            return False

        self._http_server = (
            http_server
        )

        self._http_thread = (
            http_thread
        )

        self.logger.info(
            "Prometheus exporter started: "
            f"host={host}, "
            f"port={port}"
        )

        return True


    def stop_exporter(
        self,
    ) -> bool:

        http_server = (
            self._http_server
        )

        http_thread = (
            self._http_thread
        )

        if http_server is None:
            return True

        success = True

        try:

            http_server.shutdown()
            http_server.server_close()

        except Exception as exc:

            self.logger.exception(
                "Failed to stop Prometheus exporter: "
                f"{exc}"
            )

            success = False

        if (
            http_thread is not None
            and http_thread.is_alive()
        ):

            try:

                http_thread.join(
                    timeout=5.0
                )

            except Exception as exc:

                self.logger.exception(
                    "Failed while waiting for "
                    "Prometheus exporter thread: "
                    f"{exc}"
                )

                success = False

        self._http_server = None
        self._http_thread = None

        if success:

            self.logger.info(
                "Prometheus exporter stopped"
            )

        return success