def hv_to_user_channels(channels: list[int]) -> list[int]:
    """Convert HV/Modbus numbering (1-based) to external/user numbering (0-based)."""
    return [ch - 1 for ch in channels]


def user_to_hv_channels(channels: list[int]) -> list[int]:
    """Convert external/user numbering (0-based) to HV/Modbus numbering (1-based)."""
    return [ch + 1 for ch in channels]

N_CHANNELS = 7


def normalize_user_channels(channels, n_channels: int = N_CHANNELS) -> str | list[int]:
    """Validate the "channels" field of an HV command and return it in a canonical form.

    The field uses external/user numbering (0..n_channels-1): the client converts it
    to HV/Modbus numbering, so the server must never send HV numbers.

    Accepted: "all", a comma separated string ("0,1,2"), an int or a list/tuple of ints.
    Returns "all" or a list of unique ints in the order given.
    Raises ValueError/TypeError on any other type or an out-of-range channel.
    """
    if isinstance(channels, str):
        text = channels.strip()

        if text.lower() == "all":
            return "all"

        try:
            values = [int(part.strip()) for part in text.split(",")]
        except ValueError:
            raise ValueError(f"Invalid channel string: {channels!r}") from None

    elif isinstance(channels, int) and not isinstance(channels, bool):
        values = [channels]

    elif isinstance(channels, (list, tuple)):
        values = list(channels)

        if not values:
            raise ValueError("Empty channel list")

        for value in values:
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"Invalid channel {value!r}: expected int")

    else:
        raise TypeError(f"Invalid type for channels: {type(channels).__name__}")

    for value in values:
        if value < 0 or value >= n_channels:
            raise ValueError(f"Invalid channel {value}: expected 0..{n_channels - 1}")

    return list(dict.fromkeys(values))
