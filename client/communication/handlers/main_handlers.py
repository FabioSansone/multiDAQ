import time
from common.message_handler import Channel, MessageStatus
from client.hardware.main.main_messages import MainRequest, MainMessagePriority

from client.communication.priority_utils import resolve_priority_value


def _handle_main_command(manager, message, *, main_command: str, timeout_s: float = 30.0):
    
    if manager.runtime.main_service is None:
        manager.logger.error(
            f"Cannot execute Main command {main_command}: MainService unavailable"
        )

        reply = manager.message_handler.create_reply(
            channel=Channel.MAIN,
            in_reply_to=message.request_id,
            payload={
                "main_request_id": message.request_id,
                "status": "error",
                "result": {},
                "error": "MainService unavailable",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return
    
    main_request = MainRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender=f"{manager.plane_name}_manager",
        command=main_command,
        payload=message.payload,
        status=message.status,
        deadline_s=time.time() + timeout_s
    )

    priority = MainMessagePriority(resolve_priority_value(manager, message))

    main_response = manager.runtime.main_service.request(
        main_request=main_request,
        priority=priority,
        timeout_s=timeout_s,
    )

    reply = manager.message_handler.create_reply(
        channel=Channel.MAIN,
        in_reply_to=message.request_id,
        payload={
            "main_request_id": main_response.request_id,
            "status": main_response.status.value,
            "result": main_response.result,
            "error": main_response.error,
        },
        sender="client",
        status=main_response.status,
    )

    manager.queue_message(reply)
    
def handle_main_read_snapshot(manager, message):
    _handle_main_command(
        manager,
        message,
        main_command="main_read_snapshot",
        timeout_s=30.0,
    )