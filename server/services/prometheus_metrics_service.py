from prometheus_client import CollectorRegistry, Gauge, start_http_server

from common.message_handler import Channel, ProtocolMessage
from server.core.server_state import ServerState
from server.services.monitor_stream_service import StreamSubscription
from server.utils.logger import get_logger


class PrometheusMetricsService:

    def __init__(
        self,
        server_state: ServerState,
    ):

        self.server_state = server_state

        self.registry = CollectorRegistry()
        
        self._http_server = None
        self._http_thread = None

        self._create_main_metrics()
        self._create_hv_metrics()
        self._create_rc_metrics()

        self.logger = get_logger(
            "prometheus_metrics_service"
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
            numeric_value = float(value)

        except (TypeError, ValueError):

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


    def _handle_main(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        payload = message.payload or {}
        data = payload.get("data") or {}

        env = data.get("env") or {}
        power = data.get("power") or {}
        fpga = data.get("fpga") or {}

        labels = self._get_client_labels(
            client_id
        )

        self._set_gauge(
            self.main_temperature,
            labels,
            env.get("temperature_c"),
        )

        self._set_gauge(
            self.main_humidity,
            labels,
            env.get("humidity_pct"),
        )

        self._set_gauge(
            self.main_pressure,
            labels,
            env.get("pressure_hpa"),
        )

        self._set_gauge(
            self.main_voltage_5v,
            labels,
            power.get("rail_ain0_v"),
        )

        self._set_gauge(
            self.main_voltage_3v3,
            labels,
            power.get("rail_ain2_v"),
        )

        self._set_gauge(
            self.main_current,
            labels,
            power.get("i_mon_1_a"),
        )

        self._set_gauge(
            self.main_fpga_temperature,
            labels,
            fpga.get("temperature_c"),
        )

        return True


    # ============================================================
    # HV metrics
    # ============================================================

    def _create_hv_metrics(
        self,
    ) -> None:

        labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
            "channel",
        )

        self.hv_voltage = Gauge(
            "multidaq_hv_voltage_volts",
            "HV channel measured voltage",
            labels,
            registry=self.registry,
        )

        self.hv_current = Gauge(
            "multidaq_hv_current_amperes",
            "HV channel measured current",
            labels,
            registry=self.registry,
        )

        self.hv_temperature = Gauge(
            "multidaq_hv_temperature_celsius",
            "HV channel temperature",
            labels,
            registry=self.registry,
        )

        self.hv_channel_enabled = Gauge(
            "multidaq_hv_channel_enabled",
            "Whether the HV channel is available for operation",
            labels,
            registry=self.registry,
        )

        self.hv_power_enabled = Gauge(
            "multidaq_hv_power_enabled",
            "Whether HV power is enabled for the channel",
            labels,
            registry=self.registry,
        )


    def _handle_hv(
        self,
        client_id: bytes,
        message: ProtocolMessage,
    ) -> bool:

        payload = message.payload or {}
        data = payload.get("data") or {}

        electrical = (
            data.get("electrical")
            or {}
        )

        channels = (
            electrical.get("channels")
            or {}
        )

        client_labels = (
            self._get_client_labels(
                client_id
            )
        )

        #
        # Hardware HV channels are 1..7.
        # User-facing multiDAQ channels are 0..6.
        #
        for raw_channel in range(1, 8):

            channel_data = (
                channels.get(raw_channel)
                or channels.get(str(raw_channel))
                or {}
            )

            user_channel = (
                raw_channel - 1
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
            # Grafana/Prometheus deliberately expose
            # only the simple operational view.
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

        return True


    # ============================================================
    # RC metrics
    # ============================================================

    def _create_rc_metrics(
        self,
    ) -> None:

        labels = (
            "client_id",
            "multipmt_id",
            "batch_id",
            "register",
        )

        self.rc_register_value = Gauge(
            "multidaq_rc_rate_hertz",
            "RC monitoring register value",
            labels,
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

        payload = message.payload or {}
        data = payload.get("data") or {}

        free = (
            data.get("free")
            or {}
        )

        trigger = (
            data.get("trigger")
            or {}
        )

        free_channels = (
            free.get("channels")
            or {}
        )

        trigger_channels = (
            trigger.get("channels")
            or {}
        )

        labels = (
            self._get_client_labels(
                client_id
            )
        )

        #
        # Free rates:
        # RC registers 20..26
        #
        for channel in range(7):

            channel_data = (
                free_channels.get(channel)
                or free_channels.get(str(channel))
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
        # Trigger rates:
        # RC registers 32..38
        #
        for channel in range(7):

            channel_data = (
                trigger_channels.get(channel)
                or trigger_channels.get(str(channel))
                or {}
            )

            self._set_rc_register(
                labels,
                32 + channel,
                channel_data.get(
                    "value"
                ),
            )

        return True



    
    
    

    def handle_sample(
        self,
        client_id: bytes,
        message: ProtocolMessage,
        subscription: StreamSubscription,
    ) -> bool:

        section = message.channel

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
    
    def start_exporter(self, host: str, port: int) -> bool:
        
        if self._http_server is not None:
            self.logger.debug("Prometheus exporter already running")
            return True
        
        try:
            http_server, http_thread = start_http_server(port=port, addr=host, registry=self.registry)
        except Exception as e:
            self.logger.exception(
                "Failed to start Prometheus exporter: "
                f"host={host}, "
                f"port={port}, "
                f"error={e}"
            )
            
            return False

        self._http_server = http_server
        self._http_thread = http_thread
        
        
        self.logger.info(
            "Prometheus exporter started: "
            f"host={host}, "
            f"port={port}"
        )

        return True
    
    
    
    def stop_exporter(
        self,
    ) -> bool:

        http_server = self._http_server
        http_thread = self._http_thread

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