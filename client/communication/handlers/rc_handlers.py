from common.message_handler import Channel
from client.hardware.rc.rc_messages import RCRequest, RCMessagePriority


def handle_rc_start_acquisition_mode(manager, message):

    timeout_s = 30.0

    rc_request = RCRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender="control_manager",
        command="rc_acq_start",
        payload=message.payload,
        status=message.status,
    )

    rc_response = manager.rc_service.execute_response(
        rc_request=rc_request,
        priority=RCMessagePriority.CONTROL,
        timeout_s=timeout_s,
    )

    reply = manager.message_handler.create_reply(
        channel=Channel.RC,
        in_reply_to=message.request_id,
        payload={
            "rc_request_id": rc_response.request_id,
            "status": rc_response.status.value,
            "result": rc_response.result,
            "error": rc_response.error,
        },
        sender="client",
        status=rc_response.status,
    )

    manager.queue_message(reply)