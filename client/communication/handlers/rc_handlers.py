from common.message_handler import Channel
from client.hardware.rc.rc_messages import RCRequest, RCMessagePriority


def _handle_rc_command(
    manager,
    message,
    *,
    rc_command: str,
    timeout_s: float = 30.0,
):
    rc_request = RCRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender="control_manager",
        command=rc_command,
        payload=message.payload,
        status=message.status,
    )

    rc_response = manager.runtime.rc_service.execute_response(
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


def handle_rc_start_acquisition_mode(manager, message):
    _handle_rc_command(
        manager,
        message,
        rc_command="rc_acq_start",
        timeout_s=30.0,
    )


def handle_rc_boot_mode(manager, message):
    _handle_rc_command(
        manager,
        message,
        rc_command="rc_boot",
        timeout_s=30.0,
    )


def handle_rc_reset(manager, message):
    _handle_rc_command(
        manager,
        message,
        rc_command="rc_reset",
        timeout_s=30.0,
    )


def handle_rc_read_register(manager, message):
    _handle_rc_command(
        manager,
        message,
        rc_command="rc_read_register",
        timeout_s=30.0,
    )


def handle_rc_write_register(manager, message):
    _handle_rc_command(
        manager,
        message,
        rc_command="rc_write_register",
        timeout_s=30.0,
    )