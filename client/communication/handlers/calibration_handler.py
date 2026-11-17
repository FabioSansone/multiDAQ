from common.message_handler import MessageStatus, Channel


def _reply(manager, message, error: str | None) -> None:
    reply = manager.message_handler.create_reply(
        channel=Channel.ACQUISITION,
        in_reply_to=message.request_id,
        payload={
            "status": "ok" if error is None else "error",
            "error": error,
        },
        sender="client",
        status=MessageStatus.OK if error is None else MessageStatus.ERROR,
    )

    manager.queue_message(reply)


def handle_pedestal_calibration(manager, message):
    payload = message.payload or {}

    channels = payload.get("channels")

    if not isinstance(channels, list) or not channels:
        manager.logger.error(f"Missing or invalid channels in pedestal calibration command: {channels!r}")
        _reply(manager, message, "Missing channels")
        return

    manager.logger.info(f"Received pedestal calibration request for the following channels: {channels}")

    if manager.runtime.calibration_service.prepare_pedestal(channels=channels):
        manager.logger.info("Pedestal calibration preparation ended successfully")
        _reply(manager, message, None)
    else:
        manager.logger.error("Failed to prepare for pedestal calibration")
        _reply(manager, message, "Failed to prepare for pedestal calibration")


def _reply_hv_point(manager, message, result: dict) -> None:
    """Reply of a calibration point with HV: the outcome per channel travels to the server."""
    success = bool(result.get("success"))

    reply = manager.message_handler.create_reply(
        channel=Channel.ACQUISITION,
        in_reply_to=message.request_id,
        payload={
            "status": "ok" if success else "error",
            "successful_channels": list(result.get("successful_channels") or []),
            "excluded_channels": list(result.get("excluded_channels") or []),
            "forced_off_channels": list(result.get("forced_off_channels") or []),
            "not_off_channels": list(result.get("not_off_channels") or []),
            "error": None if success else (result.get("error") or "preparation failed"),
        },
        sender="client",
        status=MessageStatus.OK if success else MessageStatus.ERROR,
    )

    manager.queue_message(reply)


def _handle_hv_point_calibration(manager, message, calibration_name: str, prepare) -> None:
    """Command of a calibration point with common voltage and threshold ('<name>_voltage',
    '<name>_threshold' in the payload): the HV preparation is done by `prepare`."""

    payload = message.payload or {}

    channels = payload.get("channels")
    voltage = payload.get(f"{calibration_name}_voltage")
    threshold = payload.get(f"{calibration_name}_threshold")

    if not isinstance(channels, list) or not channels or voltage is None or threshold is None:
        manager.logger.error(f"Missing or invalid channels, voltage or threshold in {calibration_name} calibration command: {payload!r}")
        _reply_hv_point(manager, message, {"success": False, "error": "Missing channels or common voltage/threshold"})
        return

    manager.logger.info(f"Received {calibration_name} calibration request: channels={channels}, voltage={voltage}, threshold={threshold}")

    result = prepare(channels=channels, voltage=voltage, threshold=threshold)

    if result.get("success"):
        manager.logger.info(f"{calibration_name.upper()} calibration preparation ended successfully")
    else:
        manager.logger.error(f"Failed to prepare for {calibration_name.upper()} calibration: {result.get('error')}")

    _reply_hv_point(manager, message, result)


def handle_spe_calibration(manager, message):
    _handle_hv_point_calibration(manager, message, "spe", manager.runtime.calibration_service.prepare_spe)


def handle_gain_calibration(manager, message):
    _handle_hv_point_calibration(manager, message, "gain", manager.runtime.calibration_service.prepare_gain)


def handle_polarizer_calibration(manager, message):
    _handle_hv_point_calibration(manager=manager, message=message, calibration_name="polarizer", prepare=manager.runtime.calibration_service.prepare_polarizer)


def handle_ttp_calibration(manager, message):
    """TTP: voltage and threshold of each channel. The keys of 'ttp_configuration_by_ch' arrive
    as strings (JSON): they are converted here to logical channels."""

    payload = message.payload or {}

    channels = payload.get("channels")
    raw_configuration = payload.get("ttp_configuration_by_ch")

    if not isinstance(channels, list) or not channels or not isinstance(raw_configuration, dict) or not raw_configuration:
        manager.logger.error(f"Missing or invalid channels or configuration in ttp calibration command: {payload!r}")
        _reply_hv_point(manager, message, {"success": False, "error": "Missing channels or per-channel voltage/threshold"})
        return

    try:
        configuration_by_channel = {int(channel): values for channel, values in raw_configuration.items()}
    except (TypeError, ValueError):
        manager.logger.error(f"Invalid channel keys in ttp calibration configuration: {list(raw_configuration)!r}")
        _reply_hv_point(manager, message, {"success": False, "error": "Invalid channel keys in the configuration"})
        return

    manager.logger.info(f"Received ttp calibration request: channels={channels}, configuration={configuration_by_channel}")

    result = manager.runtime.calibration_service.prepare_ttp(
        channels=channels, configuration_by_channel=configuration_by_channel,
    )

    if result.get("success"):
        manager.logger.info("TTP calibration preparation ended successfully")
    else:
        manager.logger.error(f"Failed to prepare for TTP calibration: {result.get('error')}")

    _reply_hv_point(manager, message, result)
