from client.hardware.hv.hv_interface import HV
from client.hardware.hv.hv_messages import HVRequest, HVResponse
from common.message_handler import MessageStatus


POSSIBLE_ALARMS = ["OV", "UV", "OC", "UC"]


def command_common_voltage(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    result = hv_interface.set_common_voltage(
        channels=hv_request.payload["channels"],
        common_voltage=hv_request.payload["common_voltage"],
    )

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=MessageStatus.OK,
        result=result or {},
    )


def command_check_channel_safety(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    chs = hv_request.payload["channels"]

    status_result = hv_interface.get_ch_status(channels=chs)
    alarm_result = hv_interface.get_ch_alarm(channels=chs)

    unsafe_channels = []
    checked_channels = []

    failed_status = set(status_result.get("failed_channels", []))
    failed_alarm = set(alarm_result.get("failed_channels", []))

    for ch in chs:
        status = status_result["status"].get(ch)
        alarm = alarm_result["alarm"].get(ch)

        read_failed = ch in failed_status or ch in failed_alarm

        checked_channels.append(
            {
                "channel": ch,
                "status": status,
                "alarm": alarm,
                "read_failed": read_failed,
            }
        )

        unsafe = (
            read_failed
            or status == "TRIP"
            or alarm in POSSIBLE_ALARMS
        )

        if unsafe:
            try:
                hv_interface.reset(channels=ch)
                hv_interface.off(channels=ch)
            finally:
                hv_interface.moveToBad(channel=ch)

            unsafe_channels.append(
                {
                    "channel": ch,
                    "status": status,
                    "alarm": alarm,
                    "read_failed": read_failed,
                    "action": "reset_off_moved_to_bad",
                }
            )

    if unsafe_channels:
        return HVResponse(
            protocol_version=protocol_version,
            request_id=hv_request.request_id,
            in_reply_to=hv_request.request_id,
            status=MessageStatus.ERROR,
            result={
                "checked_channels": checked_channels,
                "unsafe_channels": unsafe_channels,
            },
            error=f"Unsafe HV channels detected: {unsafe_channels}",
        )

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=MessageStatus.OK,
        result={
            "checked_channels": checked_channels,
            "unsafe_channels": [],
            "action": "none",
        },
    )


def command_check_recovery_bad(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    result = hv_interface.recover_bad_channels()

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=MessageStatus.OK,
        result=result or {},
    )


COMMAND_HANDLERS = {
    "set_common_voltage": command_common_voltage,
    "check_channel_safety": command_check_channel_safety,
    "check_recovery_bad": command_check_recovery_bad,
}