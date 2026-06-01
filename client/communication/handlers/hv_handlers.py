import time

from common.message_handler import Channel
from client.hardware.hv.hv_messages import HVRequest, HVMessagePriority
from client.utils.channels import channels_definition

def handle_hv_set_common_voltage(manager, message):
    timeout_s = 35.0

    payload = dict(message.payload)
    payload["channels"] = channels_definition(
        payload["channels"],
        hv_channels=True,
    )

    hv_request = HVRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender="control_manager",
        command="set_common_voltage",
        payload=payload,
        status=message.status,
        deadline_s=time.time() + timeout_s,
    )

    hv_response = manager.hv_service.request(
        hv_request=hv_request,
        priority=HVMessagePriority.CONTROL,
        timeout_s=timeout_s,
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

def handle_hv_set_hv_sync(manager, message):
    timeout_s = 90.0

    payload = dict(message.payload)
    payload["channels"] = channels_definition(
        payload["channels"],
        hv_channels=True,
    )

    hv_request = HVRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender="control_manager",
        command="set_hv_sync",
        payload=payload,
        status=message.status,
        deadline_s=time.time() + timeout_s,
    )

    hv_response = manager.hv_service.request(
        hv_request=hv_request,
        priority=HVMessagePriority.CONTROL,
        timeout_s=timeout_s,
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