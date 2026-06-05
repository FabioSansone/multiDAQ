from common.message_handler import MessageStatus, Channel


def handle_set_acq_mode_sync(manager, message):
    payload = message.payload or {}

    new_mode = payload.get("acq_mode")
    acq_info = payload.get("acquisition_configuration")
    pe_thr = payload.get("pe_thr")

    if not new_mode:
        manager.logger.error("Missing acq_mode in set_acq_mode_sync command")

        reply = manager.message_handler.create_reply(
            channel=Channel.ACQUISITION,
            in_reply_to=message.request_id,
            payload={
                "status": "error",
                "acq_mode": None,
                "error": "Missing acq_mode",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    manager.logger.info(
        f"Received acquisition mode sync request: {new_mode}"
    )

    success = manager.acquisition_service.apply_acquisition_mode(
        new_mode=new_mode,
        acq_info=acq_info,
        pe_thr=pe_thr,
    )

    if success:
        manager.logger.info(
            f"Acquisition mode synchronized successfully: {new_mode}"
        )
        error = None
        status = MessageStatus.OK
        status_text = "ok"
    else:
        manager.logger.error(
            f"Failed to synchronize acquisition mode: {new_mode}"
        )
        error = f"Failed to apply acquisition mode {new_mode}"
        status = MessageStatus.ERROR
        status_text = "error"

    reply = manager.message_handler.create_reply(
        channel=Channel.ACQUISITION,
        in_reply_to=message.request_id,
        payload={
            "status": status_text,
            "acq_mode": new_mode,
            "error": error,
        },
        sender="client",
        status=status,
    )

    manager.queue_message(reply)