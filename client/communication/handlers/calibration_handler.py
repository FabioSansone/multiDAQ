from common.message_handler import MessageStatus, Channel

def handle_pedestal_calibration(manager, message):
    payload = message.payload or {}

    channels = payload.get("channels")

    if channels is None:
        manager.logger.error("Missing channels in pedestal calibration command")
        
        reply = manager.message_handler.create_reply(
            channel=Channel.ACQUISITION,
            in_reply_to=message.request_id,
            payload={
                "status": "error",
                "error": "Missing channels",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    manager.logger.info(
        f"Received pedestal calibration request for the following channels: {channels}"
    )

    result = manager.runtime.calibration_service.prepare_pedestal(
        channels=channels
    )

    if result:
        manager.logger.info(
            f"Pedestal calibration preparation ended successfully"
        )
        error = None
        status = MessageStatus.OK
        status_text = "ok"
    else:
        manager.logger.error(
            f"Failed to prepare for pedestal calibration"
        )
        error = f"Failed to prepare for pedestal calibration"
        status = MessageStatus.ERROR
        status_text = "error"

    reply = manager.message_handler.create_reply(
        channel=Channel.ACQUISITION,
        in_reply_to=message.request_id,
        payload={
            "status": status_text,
            "error": error,
        },
        sender="client",
        status=status,
    )

    manager.queue_message(reply)


def handle_spe_calibration(manager, message):
    payload = message.payload or {}

    channels = payload.get("channels")
    comm_voltage = payload.get("spe_voltage")
    comm_thr = payload.get("spe_threshold")

    if channels is None or comm_voltage is None or comm_thr is None:
        manager.logger.error("Missing channels or common voltage/threshold in spe calibration command")
        
        reply = manager.message_handler.create_reply(
            channel=Channel.ACQUISITION,
            in_reply_to=message.request_id,
            payload={
                "status": "error",
                "error": "Missing channels or common voltage/threshold",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    manager.logger.info(
        f"Received spe calibration request for the following channels: {channels}"
    )

    result = manager.runtime.calibration_service.prepare_spe(
        channels=channels,
        voltage=comm_voltage,
        threshold=comm_thr,
    )

    if result:
        manager.logger.info(
            f"SPE calibration preparation ended successfully"
        )
        error = None
        status = MessageStatus.OK
        status_text = "ok"
    else:
        manager.logger.error(
            f"Failed to prepare for SPE calibration"
        )
        error = f"Failed to prepare for SPE calibration"
        status = MessageStatus.ERROR
        status_text = "error"

    reply = manager.message_handler.create_reply(
        channel=Channel.ACQUISITION,
        in_reply_to=message.request_id,
        payload={
            "status": status_text,
            "error": error,
        },
        sender="client",
        status=status,
    )

    manager.queue_message(reply)