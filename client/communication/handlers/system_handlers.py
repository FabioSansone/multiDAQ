from common.message_handler import Channel, MessageStatus



def handle_server_shutdown(manager, message):
    manager.logger.warning(
        f"Server shutdown received: {message.payload}"
    )
    
    manager.runtime.evproducer.stop()

    manager.reconnect_requested.set()
    manager.stop_listening.set()

    manager.logger.info("Reconnect requested after server shutdown")


def handle_get_multipmt_configuration(manager, message):
    payload = message.payload or {}

    try:
        snapshot = manager.runtime.get_multipmt_configuration(
            channels=payload.get("channels", "all"),
            rc_flag=bool(payload.get("rc", False)),
            hv_flag=bool(payload.get("hv", False)),
            power_flag=bool(payload.get("power", False)),
            serial_flag=bool(payload.get("serial", False)),
        )
    except (ValueError, TypeError) as e:
        manager.logger.error(f"Invalid payload for get_multipmt_configuration: {e}")

        reply = manager.message_handler.create_reply(
            channel=Channel.SYSTEM,
            in_reply_to=message.request_id,
            payload={
                "status": "error",
                "result": {},
                "error": f"Invalid channels payload: {e}",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    # Even when a section failed the values read are sent: the server decides what to do.
    if snapshot["success"]:
        status = MessageStatus.OK
        status_text = "ok"
        error = None
    else:
        status = MessageStatus.ERROR
        status_text = "error"
        error = f"Some values could not be read: {snapshot['errors']}"

    reply = manager.message_handler.create_reply(
        channel=Channel.SYSTEM,
        in_reply_to=message.request_id,
        payload={
            "status": status_text,
            "result": snapshot,
            "error": error,
        },
        sender="client",
        status=status,
    )

    manager.queue_message(reply)
