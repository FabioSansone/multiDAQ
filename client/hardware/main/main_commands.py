from client.hardware.main.main_interface import MAIN
from client.hardware.main.main_messages import MainRequest, MainResponse
from common.message_handler import MessageStatus


def command_main_read_snapshot(
    protocol_version: int,
    main_interface: MAIN,
    main_request: MainRequest,
) -> MainResponse:

    result = main_interface.get_all_sensors_data()

    return MainResponse(
        protocol_version=protocol_version,
        request_id=main_request.request_id,
        in_reply_to=main_request.request_id,
        status=MessageStatus.OK,
        result=result,
        error=None,
    )


def command_check_thresholds(
    protocol_version: int,
    main_interface: MAIN,
    main_request: MainRequest,
) -> MainResponse:

    result = main_interface.check_sensor_thresholds()

    return MainResponse(
        protocol_version=protocol_version,
        request_id=main_request.request_id,
        in_reply_to=main_request.request_id,
        status=MessageStatus.OK,
        result=result,
        error=None,
    )


def command_check_events(
    protocol_version: int,
    main_interface: MAIN,
    main_request: MainRequest,
) -> MainResponse:

    result = main_interface.check_sensor_events()

    return MainResponse(
        protocol_version=protocol_version,
        request_id=main_request.request_id,
        in_reply_to=main_request.request_id,
        status=MessageStatus.OK,
        result=result,
        error=None,
    )


COMMAND_HANDLERS = {
    "main_read_snapshot": command_main_read_snapshot,
    "main_check_thresholds": command_check_thresholds,
    "main_check_events": command_check_events,
}