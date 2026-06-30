from client.hardware.rc.rc_interface import RC
from client.hardware.rc.rc_messages import RCRequest, RCResponse
from common.message_handler import MessageStatus
from client.hardware.hv.hv_interface import HV
from client.utils.channels import channels_definition


def _make_response(
    protocol_version: int,
    rc_request: RCRequest,
    result: dict,
    error_prefix: str,
) -> RCResponse:

    success = result.get("success", False)

    if success:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.OK,
            result=result or {},
            error=None,
        )

    return RCResponse(
        protocol_version=protocol_version,
        request_id=rc_request.request_id,
        in_reply_to=rc_request.request_id,
        status=MessageStatus.ERROR,
        result=result or {},
        error=f"{error_prefix}: {result}",
    )

def _filter_monitoring_channels_by_hv_ok(
    requested_channels,
    rc_interface: RC,
    hv_interface: HV,
) -> dict:
    requested_rc_channels = channels_definition(
        channels=requested_channels,
        n_channels=rc_interface.num_channels,
    )

    hv_ok_channels = set(hv_interface.getOkChannels())

    used_rc_channels = []
    skipped_rc_channels = []
    used_hv_channels = []
    skipped_hv_channels = []

    for rc_ch in requested_rc_channels:
        hv_ch = rc_ch + 1

        if hv_ch in hv_ok_channels:
            used_rc_channels.append(rc_ch)
            used_hv_channels.append(hv_ch)
        else:
            skipped_rc_channels.append(rc_ch)
            skipped_hv_channels.append(hv_ch)

    return {
        "requested_rc_channels": requested_rc_channels,
        "used_rc_channels": used_rc_channels,
        "skipped_rc_channels": skipped_rc_channels,
        "used_hv_channels": used_hv_channels,
        "skipped_hv_channels": skipped_hv_channels,
        "hv_ok_channels": sorted(hv_ok_channels),
    }


def command_start_acquisition_mode(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    result = rc_interface.start(
        channels=rc_request.payload["channels"],
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix="Failed to start acquisition mode",
    )


def command_boot_mode(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    result = rc_interface.boot(
        channels=rc_request.payload["channels"],
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix="Failed to start boot mode",
    )


def command_reset(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    result = rc_interface.reset(
        channels=rc_request.payload["channels"],
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix="Failed to reset channels",
    )


def command_read_register(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    address = rc_request.payload.get("address")

    if address is None:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result={},
            error="Missing RC register address",
        )

    result = rc_interface.read_register(
        address=address,
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix=f"Failed to read RC register {address}",
    )


def command_write_register(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    address = rc_request.payload.get("address")
    value = rc_request.payload.get("value")

    if address is None:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result={},
            error="Missing RC register address",
        )

    if value is None:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result={},
            error="Missing RC register value",
        )

    result = rc_interface.write_register(
        address=address,
        value=value,
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix=f"Failed to write RC register {address}",
    )


def command_free_rate_monitoring(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
    hv_interface: HV,
) -> RCResponse:
    filter_info = _filter_monitoring_channels_by_hv_ok(
        requested_channels=rc_request.payload.get("channels", "all"),
        rc_interface=rc_interface,
        hv_interface=hv_interface,
    )

    if not filter_info["used_rc_channels"]:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result=filter_info,
            error="No RC monitoring channels available after HV ok filtering",
        )

    result = rc_interface.free_rate_monitoring(
        channels=filter_info["used_rc_channels"],
    )

    result.update(filter_info)

    return RCResponse(
        protocol_version=protocol_version,
        request_id=rc_request.request_id,
        in_reply_to=rc_request.request_id,
        status=MessageStatus.OK,
        result=result or {},
        error=None,
    )


def command_trg_rate_monitoring(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
    hv_interface: HV,
) -> RCResponse:
    filter_info = _filter_monitoring_channels_by_hv_ok(
        requested_channels=rc_request.payload.get("channels", "all"),
        rc_interface=rc_interface,
        hv_interface=hv_interface,
    )

    if not filter_info["used_rc_channels"]:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result=filter_info,
            error="No RC monitoring channels available after HV ok filtering",
        )

    result = rc_interface.trg_rate_monitoring(
        channels=filter_info["used_rc_channels"],
    )

    result.update(filter_info)

    return RCResponse(
        protocol_version=protocol_version,
        request_id=rc_request.request_id,
        in_reply_to=rc_request.request_id,
        status=MessageStatus.OK,
        result=result or {},
        error=None,
    )


def command_all_rate_monitoring(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
    hv_interface: HV,
) -> RCResponse:
    filter_info = _filter_monitoring_channels_by_hv_ok(
        requested_channels=rc_request.payload.get("channels", "all"),
        rc_interface=rc_interface,
        hv_interface=hv_interface,
    )

    if not filter_info["used_rc_channels"]:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result=filter_info,
            error="No RC monitoring channels available after HV ok filtering",
        )

    result = {
        "type": "data",
        "data_type": "all_rates",
        "free": rc_interface.free_rate_monitoring(filter_info["used_rc_channels"]),
        "trigger": rc_interface.trg_rate_monitoring(filter_info["used_rc_channels"]),
        **filter_info,
    }

    return RCResponse(
        protocol_version=protocol_version,
        request_id=rc_request.request_id,
        in_reply_to=rc_request.request_id,
        status=MessageStatus.OK,
        result=result,
        error=None,
    )
    
def command_feb_reset_after_flash(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    result = rc_interface.feb_reset_after_flash()

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix="Failed to reset RC after FEB flash",
    )


def command_feb_select_for_address_change(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    result = rc_interface.feb_select_for_address_change(
        channels=rc_request.payload["channels"],
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix="Failed to select FEB for address change",
    )


COMMAND_HANDLERS = {
    "rc_acq_start": command_start_acquisition_mode,
    "rc_boot": command_boot_mode,
    "rc_reset": command_reset,

    "rc_read_register": command_read_register,
    "rc_write_register": command_write_register,

    "rc_free_rate_monitoring": command_free_rate_monitoring,
    "rc_trg_rate_monitoring": command_trg_rate_monitoring,
    "rc_all_rate_monitoring": command_all_rate_monitoring,
    
    "rc_feb_reset_after_flash": command_feb_reset_after_flash,
    "rc_feb_select_address_change": command_feb_select_for_address_change,
}
    