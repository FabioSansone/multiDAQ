from client.hardware.hv.hv_interface import HV
from client.hardware.hv.hv_messages import HVRequest, HVResponse
from common.message_handler import MessageStatus


POSSIBLE_ALARMS = ["OV", "UV", "OC", "UC"]

def _wrap_hv_action(
    protocol_version: int,
    hv_request: HVRequest,
    result: dict,
) -> HVResponse:
    failed = result.get("failed_channels", [])
    not_responding = result.get("not_responding_channels", [])

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
    

def command_common_threshold(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    result = hv_interface.set_common_threshold(
        channels=hv_request.payload["channels"],
        common_threshold=hv_request.payload["common_threshold"],
    )

    return _wrap_hv_action(protocol_version, hv_request, result)


def command_hv_on(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    result = hv_interface.on(
        channels=hv_request.payload["channels"],
    )

    return _wrap_hv_action(protocol_version, hv_request, result)


def command_hv_off(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    result = hv_interface.off(
        channels=hv_request.payload["channels"],
    )

    return _wrap_hv_action(protocol_version, hv_request, result)


def command_common_voltage(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    result = hv_interface.set_common_voltage(
        channels=hv_request.payload["channels"],
        common_voltage=hv_request.payload["common_voltage"],
    )

    return _wrap_hv_action(protocol_version, hv_request, result)


def command_acquisition_configuration(
        protocol_version: int,
        hv_interface: HV,
        hv_request: HVRequest,
) -> HVResponse:
    
    result = hv_interface.set_acquisition_configuration(
        channels=hv_request.payload["channels"],
        acq_configuration=hv_request.payload["acquisition_configuration"],
    )

    return _wrap_hv_action(protocol_version, hv_request, result)


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

def command_hv_on_and_wait(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    result = hv_interface.on_and_wait(
        channels=hv_request.payload["channels"],
    )

    return _wrap_hv_action(protocol_version, hv_request, result)

def command_hv_off_and_wait(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:

    result = hv_interface.off_and_wait(
        channels=hv_request.payload["channels"],
        timeout_s=hv_request.payload.get("timeout_s", 240.0),
        poll_s=hv_request.payload.get("poll_s", 2.0),
    )
    
    return _wrap_hv_action(protocol_version, hv_request, result)


def command_check_channel_presence(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:
    chs = hv_request.payload["channels"]

    checked_channels = []
    became_bad = []

    for ch in chs:
        try:
            if not hv_interface.hv.checkAddressBoundary(ch):
                hv_interface.moveToBad(ch)
                became_bad.append({
                    "channel": ch,
                    "reason": "out_of_boundary",
                })
                continue

            if not hv_interface.hv.checkAddress(ch):
                hv_interface.moveToBad(ch)
                became_bad.append({
                    "channel": ch,
                    "reason": "not_responding",
                })
                continue

            checked_channels.append({
                "channel": ch,
                "reachable": True,
            })

        except Exception as e:
            hv_interface.moveToBad(ch)
            became_bad.append({
                "channel": ch,
                "reason": str(e),
            })

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=MessageStatus.OK,
        result={
            "checked_channels": checked_channels,
            "became_bad_channels": became_bad,
            "ok_channels": hv_interface.getOkChannels(),
            "bad_channels": hv_interface.getBadChannels(),
            "on_channels": hv_interface.getOnChannels(),
            "off_channels": hv_interface.getOffChannels(),
        },
    )

def command_hv_sync(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:

    channels = hv_request.payload.get("channels", "all")

    recovery_request = HVRequest(
        protocol_version=hv_request.protocol_version,
        request_id=f"{hv_request.request_id}:recovery",
        command="check_recovery_bad",
        payload={},
        sender=hv_request.sender,
        status=hv_request.status,
    )

    presence_request = HVRequest(
        protocol_version=hv_request.protocol_version,
        request_id=f"{hv_request.request_id}:presence",
        command="check_channel_presence",
        payload={"channels": channels},
        sender=hv_request.sender,
        status=hv_request.status,
    )

    power_request = HVRequest(
        protocol_version=hv_request.protocol_version,
        request_id=f"{hv_request.request_id}:power",
        command="check_channel_power",
        payload={"channels": channels},
        sender=hv_request.sender,
        status=hv_request.status,
    )

    recovery_response = command_check_recovery_bad(
        protocol_version=protocol_version,
        hv_interface=hv_interface,
        hv_request=recovery_request,
    )

    presence_response = command_check_channel_presence(
        protocol_version=protocol_version,
        hv_interface=hv_interface,
        hv_request=presence_request,
    )

    power_response = command_check_channel_power(
        protocol_version=protocol_version,
        hv_interface=hv_interface,
        hv_request=power_request,
    )

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=MessageStatus.OK,
        result={
            "recovery": recovery_response.result,
            "presence": presence_response.result,
            "power_sync": power_response.result,
            "ok_channels": hv_interface.getOkChannels(),
            "bad_channels": hv_interface.getBadChannels(),
            "on_channels": hv_interface.getOnChannels(),
            "off_channels": hv_interface.getOffChannels(),
        },
    )
    
    
def command_feb_change_address(
    protocol_version: int,
    hv_interface: HV,
    hv_request: HVRequest,
) -> HVResponse:

    channel_index = hv_request.payload["channel_index"]
    standard_addr = hv_request.payload.get("standard_addr")

    new_address = channel_index + 1

    result = hv_interface.change_feb_address(
        new_address=new_address,
        standard_addr=standard_addr,
    )

    status = MessageStatus.OK if result.get("success") else MessageStatus.ERROR

    return HVResponse(
        protocol_version=protocol_version,
        request_id=hv_request.request_id,
        in_reply_to=hv_request.request_id,
        status=status,
        result=result,
        error=None if result.get("success") else result.get("error"),
    )


COMMAND_HANDLERS = {
    "set_common_voltage": command_common_voltage,
    "set_common_threshold": command_common_threshold,
    "set_acquisition_configuration": command_acquisition_configuration,
    
    "hv_on": command_hv_on,
    "hv_off": command_hv_off,

    "hv_on_and_wait": command_hv_on_and_wait,
    "hv_off_and_wait": command_hv_off_and_wait,
    
    "set_hv_sync": command_hv_sync,
    
    "feb_change_address": command_feb_change_address,
    
    "check_channel_safety": command_check_channel_safety,
    "check_channel_power": command_check_channel_power,
    "check_recovery_bad": command_check_recovery_bad,
}

