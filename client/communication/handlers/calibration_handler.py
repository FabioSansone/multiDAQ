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


def handle_gain_calibration(manager, message):
    payload = message.payload or {}

    channels = payload.get("channels")
    comm_voltage = payload.get("gain_voltage")
    comm_thr = payload.get("gain_threshold")

    if channels is None or comm_voltage is None or comm_thr is None:
        manager.logger.error("Missing channels or common voltage/threshold in gain calibration command")
        
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
        f"Received gain calibration request for the following channels: {channels}"
    )

    result = manager.runtime.calibration_service.prepare_gain(
        channels=channels,
        voltage=comm_voltage,
        threshold=comm_thr,
    )

    if result:
        manager.logger.info(
            f"GAIN calibration preparation ended successfully"
        )
        error = None
        status = MessageStatus.OK
        status_text = "ok"
    else:
        manager.logger.error(
            f"Failed to prepare for GAIN calibration"
        )
        error = f"Failed to prepare for GAIN calibration"
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


def handle_threshdol_calibration(manager, message):
    payload = message.payload or {}

    channels = payload.get("channels")
    voltage_by_channel = payload.get("thr_voltage_by_ch")
    comm_thr = payload.get("thr_threshold")

    if channels is None or voltage_by_channel is None or comm_thr is None:
        manager.logger.error("Missing channels or common voltage/threshold in threshold calibration command")
        
        reply = manager.message_handler.create_reply(
            channel=Channel.ACQUISITION,
            in_reply_to=message.request_id,
            payload={
                "success": False,
                "successful_channels": [],
                "excluded_channels": channels if channels is not None else [],
                "status": "error",
                "error": "Missing channels or common voltage/threshold",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    manager.logger.info(
        f"Received threshold calibration request for the following channels: {channels}"
    )

    try:
        if not isinstance(voltage_by_channel, dict):
            raise TypeError("Voltage configuration must be a dictionary")

        voltage_by_channel = {
            int(ch): voltage
            for ch, voltage in voltage_by_channel.items()
        }

    except (TypeError, ValueError) as exc:
        error = f"Invalid thr_voltage_by_ch: {exc}"
        manager.logger.error(error)

        reply = manager.message_handler.create_reply(
            channel=Channel.ACQUISITION,
            in_reply_to=message.request_id,
            payload={
                "status": "error",
                "success": False,
                "successful_channels": [],
                "excluded_channels": channels,
                "error": error,
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    result = manager.runtime.calibration_service.prepare_threshold_calibration(
        channels=channels,
        voltage_by_channel=voltage_by_channel,
        threshold=comm_thr,
    )

    success = result["success"]
    successful_channels = result["successful_channels"]
    excluded_channels = result["excluded_channels"]
    error = result["error"]

    status = MessageStatus.OK if success else MessageStatus.ERROR
    status_text = "ok" if success else "error"

    if success:
        manager.logger.info(
            "THR calibration preparation completed: "
            f"successful_channels={successful_channels}, "
            f"excluded_channels={excluded_channels}"
        )
    else:
        manager.logger.error(
            f"THR calibration preparation failed: {error}"
        )

    reply = manager.message_handler.create_reply(
        channel=Channel.ACQUISITION,
        in_reply_to=message.request_id,
        payload={
            "status": status_text,
            "success": success,
            "successful_channels": successful_channels,
            "excluded_channels": excluded_channels,
            "error": error,
        },
        sender="client",
        status=status,
    )

    manager.queue_message(reply)