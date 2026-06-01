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

    failed = result.get("failed_channels", [])
    not_responding = result.get("not_responding_channels", [])
    successful = result.get("successful_channels", [])

    if failed or not_responding:
        status = MessageStatus.ERROR
        error = (
            "Some HV channels failed or did not respond. "
            f"Failed: {failed}, not responding: {not_responding}"
        )
    else:
        status = MessageStatus.OK
        error = None

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=status,
        result=result or {},
        error=error,
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
            action = None

            if read_failed:
                hv_interface.moveToBad(channel=ch)
                action = "moved_to_bad_read_failed"

            else:
                try:
                    hv_interface.force_reset(channels=ch)
                    hv_interface.force_off(channels=ch)
                    action = "reset_off_moved_to_bad"
                finally:
                    hv_interface.moveToBad(channel=ch)

            unsafe_channels.append(
                {
                    "channel": ch,
                    "status": status,
                    "alarm": alarm,
                    "read_failed": read_failed,
                    "action": action,
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

def command_check_channel_power(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    chs = hv_request.payload["channels"]

    checked_channels = []
    moved_to_on = []
    moved_to_off = []
    failed_channels = []

    for ch in chs:
        try:
            status = hv_interface.hv.getStatus(slave=ch)

            checked_channels.append({
                "channel": ch,
                "status": status,
                "read_failed": False,
            })

            if status == "UP" and ch in hv_interface.getOffChannels():
                hv_interface.moveToOn(channel=ch)
                moved_to_on.append(ch)

            elif status == "DOWN" and ch in hv_interface.getOnChannels():
                hv_interface.moveToOff(channel=ch)
                moved_to_off.append(ch)

        except Exception as e:
            failed_channels.append(ch)

            checked_channels.append({
                "channel": ch,
                "status": None,
                "read_failed": True,
                "error": str(e),
            })

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=MessageStatus.OK,
        result={
            "checked_channels": checked_channels,
            "moved_to_on_channels": moved_to_on,
            "moved_to_off_channels": moved_to_off,
            "failed_channels": failed_channels,
            "ok_channels": hv_interface.getOkChannels(),
            "bad_channels": hv_interface.getBadChannels(),
            "on_channels": hv_interface.getOnChannels(),
            "off_channels": hv_interface.getOffChannels(),
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
    "check_channel_power": command_check_channel_power,
    "check_recovery_bad": command_check_recovery_bad,
}