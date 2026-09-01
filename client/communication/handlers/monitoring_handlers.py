from common.message_handler import Channel, MessageStatus


def _send_monitoring_reply(
        manager,
        message,
        *,
        status: MessageStatus,
        result: dict | None = None,
        error: str | None = None,
) -> None:

    reply = manager.message_handler.create_reply(
        channel=Channel.MONITORING,
        in_reply_to=message.request_id,
        payload={
            "result": result or {},
            "error": error,
        },
        sender="client",
        status=status,
    )

    manager.queue_message(reply)



def handle_sample_start(manager, message) -> None:

    payload = message.payload or {}

    section_raw = payload.get("section")
    interval_s = payload.get("interval_s")

    if section_raw is None:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error="Missing sample section",
        )
        return

    try:
        channel = Channel(str(section_raw).lower())
    except ValueError:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error=f"Invalid sample section: {section_raw}",
        )
        return

    sample_service = manager.runtime.monitor_sample_service

    if channel not in sample_service.SUPPORTED_CHANNELS:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error=(
                f"Unsupported sample section: "
                f"{channel.value}"
            ),
        )
        return

    if interval_s is None:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error="Missing sample interval",
        )
        return

    if channel == Channel.HV:
        if not manager.runtime.ensure_hv_service():
            _send_monitoring_reply(
                manager,
                message,
                status=MessageStatus.ERROR,
                error="HVService unavailable",
            )
            return

    success = sample_service.start_section(
        channel=channel,
        interval_s=interval_s,
    )

    if not success:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error=(
                f"Failed to start "
                f"{channel.value} sample stream"
            ),
        )
        return

    _send_monitoring_reply(
        manager,
        message,
        status=MessageStatus.OK,
        result={
            "section": channel.value,
            "enabled": True,
            "interval_s": float(interval_s),
        },
    )


def handle_sample_stop(manager, message) -> None:

    payload = message.payload or {}

    section_raw = payload.get("section")

    if section_raw is None:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error="Missing sample section",
        )
        return

    try:
        channel = Channel(str(section_raw).lower())
    except ValueError:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error=f"Invalid sample section: {section_raw}",
        )
        return

    sample_service = manager.runtime.monitor_sample_service

    if channel not in sample_service.SUPPORTED_CHANNELS:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error=(
                f"Unsupported sample section: "
                f"{channel.value}"
            ),
        )
        return

    success = sample_service.stop_section(channel=channel)

    if not success:
        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error=(
                f"Failed to stop "
                f"{channel.value} sample stream"
            ),
        )
        return

    _send_monitoring_reply(
        manager,
        message,
        status=MessageStatus.OK,
        result={
            "section": channel.value,
            "enabled": False,
            "interval_s": None,
        },
    )
    

def handle_main_sensor_status(
    manager,
    message,
) -> None:

    main_service = (
        manager.runtime.main_service
    )

    if main_service is None:

        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error="MainService unavailable",
        )

        return

    response = (
        main_service._submit_command(
            command="main_sensor_status",
            payload={},
            sender="monitoring_main_sensor_status",
        )
    )

    if response.status != MessageStatus.OK:

        _send_monitoring_reply(
            manager,
            message,
            status=MessageStatus.ERROR,
            error=(
                response.error
                or "Failed to read MAIN sensor status"
            ),
        )

        return

    _send_monitoring_reply(
        manager,
        message,
        status=MessageStatus.OK,
        result=(
            response.result
            or {}
        ),
    )