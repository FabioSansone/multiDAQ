def hv_to_user_channels(channels: list[int]) -> list[int]:
    """Convert HV/Modbus numbering (1-based) to external/user numbering (0-based)."""
    return [ch - 1 for ch in channels]


def user_to_hv_channels(channels: list[int]) -> list[int]:
    """Convert external/user numbering (0-based) to HV/Modbus numbering (1-based)."""
    return [ch + 1 for ch in channels]