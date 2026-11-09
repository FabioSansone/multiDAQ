import time

from common.message_handler import Channel, MessageStatus
from client.hardware.hv.hv_messages import HVRequest, HVMessagePriority
from client.utils.channels import channels_definition
from client.communication.priority_utils import resolve_priority_value
from common.timeouts import client_timeout


# Read-only commands: the server may ask them before the mode sync that creates the HVService
READ_ONLY_HV_COMMANDS = {
    "get_serial_map",
    "get_voltage_channels",
    "get_threshold_channels",
    "get_volt_thr_channels",
    "get_power_state_channels",
}


def _handle_hv_command(manager, message, *, hv_command: str, timeout_s: float = 35.0):
    if manager.runtime.hv_service is None and hv_command in READ_ONLY_HV_COMMANDS:
        # monitor_only until the mode sync sets the policy it needs
        if manager.runtime.ensure_hv_service(hv_policy="monitor_only"):
            manager.runtime.hv_service.start()

    if manager.runtime.hv_service is None:
        manager.logger.error(f"Cannot execute HV command {hv_command}: HVService unavailable")
        reply = manager.message_handler.create_reply(
            channel=Channel.HV, in_reply_to=message.request_id,
            payload={"hv_request_id": message.request_id, "status": "error",
                     "result": {}, "error": "HVService unavailable"},
            sender="client", status=MessageStatus.ERROR,
        )
        manager.queue_message(reply)
        return

    try:
        payload = dict(message.payload)
        payload["channels"] = channels_definition(payload["channels"], hv_channels=True)
    except (ValueError, TypeError, KeyError, Exception) as e:
        manager.logger.error(f"Invalid payload for HV command {hv_command}: {e}")
        reply = manager.message_handler.create_reply(
            channel=Channel.HV, in_reply_to=message.request_id,
            payload={"hv_request_id": message.request_id, "status": "error",
                     "result": {}, "error": f"Invalid channels payload: {e}"},
            sender="client", status=MessageStatus.ERROR,
        )
        manager.queue_message(reply)
        return

    hv_request = HVRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender=f"{manager.plane_name}_manager",
        command=hv_command,
        payload=payload,
        status=message.status,
        deadline_s=time.time() + timeout_s,
    )

    priority = HVMessagePriority(resolve_priority_value(manager, message))

    hv_response = manager.runtime.hv_service.request(
        hv_request=hv_request,
        priority=priority,
        timeout_s=timeout_s,
    )

    reply = manager.message_handler.create_reply(
        channel=Channel.HV, in_reply_to=message.request_id,
        payload={"hv_request_id": hv_response.request_id, "status": hv_response.status.value,
                 "result": hv_response.result, "error": hv_response.error},
        sender="client", status=hv_response.status,
    )
    manager.queue_message(reply)


def handle_hv_set_common_voltage(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="set_common_voltage",
        timeout_s=client_timeout("set_common_voltage"),
    )


def handle_hv_set_common_threshold(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="set_common_threshold",
        timeout_s=client_timeout("set_common_threshold"),
    )


def handle_hv_set_acquisition_configuration(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="set_acquisition_configuration",
        timeout_s=client_timeout("set_acquisition_configuration"),
    )


def handle_hv_get_voltage_channels(manager, message):
    _handle_hv_command(manager=manager, message=message, hv_command="get_voltage_channels", timeout_s=client_timeout("get_voltage_channels"))


def handle_hv_get_threshold_channels(manager, message):
    _handle_hv_command(manager=manager, message=message, hv_command="get_threshold_channels", timeout_s=client_timeout("get_threshold_channels"))

def handle_hv_get_volt_thr_channels(manager, message):
    _handle_hv_command(manager=manager, message=message, hv_command="get_volt_thr_channels", timeout_s=client_timeout("get_volt_thr_channels"))


def handle_hv_get_power_state_channels(manager, message):
    _handle_hv_command(
        manager=manager,
        message=message,
        hv_command="get_power_state_channels",
        timeout_s=client_timeout("get_power_state_channels"),
    )


def handle_hv_on(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="hv_on",
        timeout_s=client_timeout("hv_on"),
    )


def handle_hv_off(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="hv_off",
        timeout_s=client_timeout("hv_off"),
    )


def handle_hv_set_hv_sync(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="set_hv_sync",
        timeout_s=client_timeout("set_hv_sync"),
    )

def handle_hv_set_on_and_wait(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="hv_on_and_wait",
        timeout_s=client_timeout("hv_on_and_wait"),
    )
    
def handle_hv_off_and_wait(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="hv_off_and_wait",
        timeout_s=client_timeout("hv_off_and_wait"),
    )

def handle_hv_set_user_bad(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="hv_set_user_bad",
        timeout_s=client_timeout("mark_bad"),
    )

def handle_hv_unset_user_bad(manager, message):
    _handle_hv_command(
        manager,
        message,
        hv_command="hv_unset_user_bad",
        timeout_s=client_timeout("unmark_bad"),
    )
        
def handle_hv_set_pmt_serials(manager, message):
    if manager.runtime.hv_service is None:
        manager.logger.error("Cannot execute HV command set_pmt_serials: HVService unavailable")
        reply = manager.message_handler.create_reply(
            channel=Channel.HV, in_reply_to=message.request_id,
            payload={"hv_request_id": message.request_id, "status": "error",
                     "result": {}, "error": "HVService unavailable"},
            sender="client", status=MessageStatus.ERROR,
        )
        manager.queue_message(reply)
        return

    try:
        raw_serials = message.payload.get("serials", {})
        converted_serials = {int(ch) + 1: serial for ch, serial in raw_serials.items()}
    except (TypeError, ValueError) as e:
        manager.logger.error(f"Problems in retrieving pmt serial codes: {e}")
        reply = manager.message_handler.create_reply(
            channel=Channel.HV, in_reply_to=message.request_id,
            payload={"hv_request_id": message.request_id, "status": "error",
                     "result": {}, "error": f"Invalid serials payload: {e}"},
            sender="client", status=MessageStatus.ERROR,
        )
        manager.queue_message(reply)
        return   

    hv_request = HVRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender=f"{manager.plane_name}_manager",
        command="set_pmt_serials",
        payload={"serials": converted_serials},
        status=message.status,
        deadline_s=time.time() + client_timeout("set_pmt_serials"),
    )

    priority = HVMessagePriority(resolve_priority_value(manager, message))

    hv_response = manager.runtime.hv_service.request(
        hv_request=hv_request, priority=priority, timeout_s=client_timeout("set_pmt_serials"),
    )

    reply = manager.message_handler.create_reply(
        channel=Channel.HV, in_reply_to=message.request_id,
        payload={"hv_request_id": hv_response.request_id, "status": hv_response.status.value,
                 "result": hv_response.result, "error": hv_response.error},
        sender="client", status=hv_response.status,
    )
    manager.queue_message(reply)


def handle_get_serial_map(manager, message):
    _handle_hv_command(manager, message, hv_command="get_serial_map", timeout_s=client_timeout("get_serial_map"))
    
    
def handle_hv_monitor_snapshot(manager, message):
    timeout_s = client_timeout("hv_monitor_snapshot")

    if manager.runtime.hv_service is None:
        manager.logger.error(
            "Cannot execute HV monitoring snapshot: HVService unavailable"
        )

        reply = manager.message_handler.create_reply(
            channel=Channel.HV,
            in_reply_to=message.request_id,
            payload={
                "hv_request_id": message.request_id,
                "status": "error",
                "result": {},
                "error": "HVService unavailable",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    payload = dict(message.payload or {})

    try:
        if "channels" in payload:
            payload["channels"] = channels_definition(
                payload["channels"],
                hv_channels=True,
            )
    except Exception as e:
        manager.logger.error(
            f"Invalid payload for HV monitoring snapshot: {e}"
        )

        reply = manager.message_handler.create_reply(
            channel=Channel.HV,
            in_reply_to=message.request_id,
            payload={
                "hv_request_id": message.request_id,
                "status": "error",
                "result": {},
                "error": f"Invalid channels payload: {e}",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    hv_request = HVRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender=f"{manager.plane_name}_manager",
        command="hv_monitor_snapshot",
        payload=payload,
        status=message.status,
        deadline_s=time.time() + timeout_s,
    )

    priority = HVMessagePriority(
        resolve_priority_value(manager, message)
    )

    hv_response = manager.runtime.hv_service.request(
        hv_request=hv_request,
        priority=priority,
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
              


def handle_hv_restore_configuration(manager, message):

    timeout_s = client_timeout("restore_hv_configuration")

    if manager.runtime.hv_service is None:
        manager.logger.error(
            "Cannot restore HV configuration: HVService unavailable"
        )

        reply = manager.message_handler.create_reply(
            channel=Channel.HV,
            in_reply_to=message.request_id,
            payload={
                "hv_request_id": message.request_id,
                "status": "error",
                "result": {},
                "error": "HVService unavailable",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    try:
        raw_configuration = (
            message.payload.get("configuration", {})
        )

        hv_configuration = {
            int(channel) + 1: dict(parameters)
            for channel, parameters
            in raw_configuration.items()
        }

    except Exception as e:
        manager.logger.error(
            f"Invalid HV restore configuration: {e}"
        )

        reply = manager.message_handler.create_reply(
            channel=Channel.HV,
            in_reply_to=message.request_id,
            payload={
                "hv_request_id": message.request_id,
                "status": "error",
                "result": {},
                "error": f"Invalid restore configuration: {e}",
            },
            sender="client",
            status=MessageStatus.ERROR,
        )

        manager.queue_message(reply)
        return

    hv_request = HVRequest(
        protocol_version=message.protocol_version,
        request_id=message.request_id,
        sender=f"{manager.plane_name}_manager",
        command="restore_hv_configuration",
        payload={
            "configuration": hv_configuration,
        },
        status=message.status,
        deadline_s=time.time() + timeout_s,
    )

    priority = HVMessagePriority(
        resolve_priority_value(manager, message)
    )

    hv_response = manager.runtime.hv_service.request(
        hv_request=hv_request,
        priority=priority,
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
