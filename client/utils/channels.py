from typing import Union, List

def channels_definition(
    channels: Union[List[int], str, int],
    n_channels: int = 7,
    hv_channels: bool = False
) -> List[int]:
    """
    Standardize channel input.

    External numbering: 0..n_channels-1
    HV numbering (Modbus): 1..n_channels

    Parameters
    ----------
    channels : int | list[int] | str
        Single channel, list of channels, "all", or "1,2,3"
    n_channels : int
        Total number of channels
    hv_channels : bool
        If True convert to HV numbering (1..n)

    Returns
    -------
    list[int]
    """

    if isinstance(channels, int):
        ch_list = [channels]

    elif isinstance(channels, list):
        ch_list = channels

    elif isinstance(channels, str):

        if channels.lower() == "all":
            ch_list = list(range(n_channels))

        else:
            try:
                ch_list = [int(c) for c in channels.split(",")]
            except ValueError:
                raise ValueError(f"Invalid channel string: {channels}")

    else:
        raise TypeError(f"Invalid type for channels: {type(channels)}")

    for ch in ch_list:
        if ch < 0 or ch >= n_channels:
            raise ValueError(f"Invalid channel: {ch}")

    if hv_channels:
        ch_list = [ch + 1 for ch in ch_list]

    return ch_list