from common.message_handler import Channel
from client.hardware.hv_service import HVRequest, HVMessagePriority


def handle_hv_set_common_voltage(manager, message):
    hv_request = HVRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender="control_manager",
        command="set_common_voltage",
        payload=message.payload,
        status=message.status,
    )

    hv_response = manager.hv_service.request(
        hv_request=hv_request,
        priority=HVMessagePriority.CONTROL,
        timeout_s=5.0,
    )

    reply = manager.message_handler.create_reply(
        channel=Channel.HV,
        in_reply_to=message.request_id,
        payload={
            "hv_request_id": hv_response.request_id,
            "status": hv_response.status.value,
            "result": hv_response.result,
            "error": hv_response.error,
        },
        sender="client",
        status=hv_response.status,
    )

    manager.queue_message(reply)