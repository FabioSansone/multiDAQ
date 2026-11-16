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
            "error": None if success else (result.get("error") or "preparation failed"),
        },
        sender="client",
        status=MessageStatus.OK if success else MessageStatus.ERROR,
    )

    manager.queue_message(reply)


def handle_spe_calibration(manager, message):
    payload = message.payload or {}

    channels = payload.get("channels")
    voltage = payload.get("spe_voltage")
    threshold = payload.get("spe_threshold")

    if not isinstance(channels, list) or not channels or voltage is None or threshold is None:
        manager.logger.error(f"Missing or invalid channels, voltage or threshold in spe calibration command: {payload!r}")
        _reply_hv_point(manager, message, {"success": False, "error": "Missing channels or common voltage/threshold"})
        return

    manager.logger.info(f"Received spe calibration request: channels={channels}, voltage={voltage}, threshold={threshold}")

    result = manager.runtime.calibration_service.prepare_spe(channels=channels, voltage=voltage, threshold=threshold)

    if result.get("success"):
        manager.logger.info("SPE calibration preparation ended successfully")
    else:
        manager.logger.error(f"Failed to prepare for SPE calibration: {result.get('error')}")

    _reply_hv_point(manager, message, result)
