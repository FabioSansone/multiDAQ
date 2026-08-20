from client.hardware.rc.rc_interface import RC
from client.hardware.rc.rc_messages import RCRequest, RCResponse
from common.message_handler import MessageStatus


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


def command_read_acq_registers(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    addresses = rc_request.payload.get("rc_acq_registers")

    if addresses is None:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result={},
            error="Missing RC register address",
        )

    result = rc_interface.read_acq_registers(
        addresses=addresses,
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix=f"Failed to read acquisition related RC registers {addresses}",
    )


def command_set_rc_acq_registers(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:

    rc_acq_dit = rc_request.payload.get("rc_acq_dict")

    if rc_acq_dit is None:
        return RCResponse(
            protocol_version=protocol_version,
            request_id=rc_request.request_id,
            in_reply_to=rc_request.request_id,
            status=MessageStatus.ERROR,
            result={},
            error="Missing RC acquisition registers settings",
        )

    result = rc_interface.set_acq_registers(
        rc_acq_dit=rc_acq_dit,
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix=f"Failed to set acquisition related RC registers {rc_acq_dit}",
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
) -> RCResponse:
    """
    Nessun filtro sui canali (decisione M2.0): si legge sempre tutto,
    inclusi i canali disabilitati, e RC non deve dipendere da HV.
    """
    result = rc_interface.free_rate_monitoring(
        channels=rc_request.payload.get("channels", "all"),
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix="Failed to read RC free-running rates",
    )


def command_trg_rate_monitoring(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:
    """
    Nessun filtro sui canali (decisione M2.0): si legge sempre tutto,
    inclusi i canali disabilitati, e RC non deve dipendere da HV.
    """
    result = rc_interface.trg_rate_monitoring(
        channels=rc_request.payload.get("channels", "all"),
    )

    return _make_response(
        protocol_version=protocol_version,
        rc_request=rc_request,
        result=result,
        error_prefix="Failed to read RC trigger rates",
    )


def command_all_rate_monitoring(
    protocol_version: int,
    rc_interface: RC,
    rc_request: RCRequest,
) -> RCResponse:
    """
    Nessun filtro sui canali (decisione M2.0): si legge sempre tutto,
    inclusi i canali disabilitati, e RC non deve dipendere da HV.
    """
    result = rc_interface.monitor_all_rates(
        channels=rc_request.payload.get("channels", "all"),
    )

    success = result.get("free", {}).get("success", False) and result.get("trigger", {}).get("success", False)

    return RCResponse(
        protocol_version=protocol_version,
        request_id=rc_request.request_id,
        in_reply_to=rc_request.request_id,
        status=MessageStatus.OK if success else MessageStatus.ERROR,
        result=result,
        error=None if success else "One or more RC rate monitoring registers failed to read",
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
    "rc_read_acq_registers": command_read_acq_registers,
    "set_rc_acq": command_set_rc_acq_registers,

    "rc_free_rate_monitoring": command_free_rate_monitoring,
    "rc_trg_rate_monitoring": command_trg_rate_monitoring,
    "rc_all_rate_monitoring": command_all_rate_monitoring,
    
    "rc_feb_reset_after_flash": command_feb_reset_after_flash,
    "rc_feb_select_address_change": command_feb_select_for_address_change,
}