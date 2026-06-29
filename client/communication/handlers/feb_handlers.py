from common.message_handler import Channel, MessageStatus


def handle_feb_program(manager, message):
    payload = message.payload or {}

    channels = payload.get("channels", "all")
    baud = payload.get("baud", 115200)
    firmware = payload.get("firmware")
    port = payload.get("port", "/dev/ttyPS0")
    standard_addr = payload.get("standard_addr")

    if firmware is None:
        reply = manager.message_handler.create_reply(
            channel=Channel.RC,
            in_reply_to=message.request_id,
            payload={
                "status": "error",
                "result": {},
                "error": "Missing firmware path",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )
        manager.queue_message(reply)
        return

    result = manager.runtime.feb_service.program(
        channels=channels,
        baud=baud,
        firmware=firmware,
        port=port,
        standard_addr=standard_addr,
    )

    status = MessageStatus.OK if result.get("success") else MessageStatus.ERROR

    reply = manager.message_handler.create_reply(
        channel=Channel.RC,
        in_reply_to=message.request_id,
        payload={
            "status": status.value,
            "result": result,
            "error": None if result.get("success") else result,
        },
        sender="client",
        status=status,
    )

    manager.queue_message(reply)