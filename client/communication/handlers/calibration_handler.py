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
