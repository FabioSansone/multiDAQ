# multiDAQ
DAQ system for the SWGO multiPMT

multiDAQ has two programs: a **server** (command line, acquisition, monitoring) and one **client** for each acquisition board.

## Requirements

- Python 3.10 or newer.
- On the server machine, to build the data receiver (`evreceiver`, compiled automatically the first time it is needed): `gcc` and the ZeroMQ library with its headers (`libzmq3-dev` on Debian/Ubuntu).

## Install

Install it in editable mode, from the repository folder, on the server and on every client:

```
pip install -e .
```

Editable mode matters on the server: the detector JSON files in `server/multipmt_config_files` are updated while the program runs (for example the fixed-bad channels), so they must stay inside the repository.

The Python libraries are listed in `pyproject.toml` and are installed together. The client talks to the high voltage boards through a serial port and needs `pymodbus` 3.10 or newer.

## Run

```
multidaq-server [test|calibration|multipmt]      # add --no-web to skip the web control panel
multidaq-client --server-ip <server address> --hv-port /dev/ttyPS1      # --iface eth0 (default)
```

The default mode of the server is `test` (acquisition without high voltage).

At the server prompt, `connect --num_clients <n>` opens the ports the clients connect to: 8888 (control), 8889 (acquisition) and 8890 (monitoring). The client has the same defaults.

## multiPMT registry (identity of the boards)

Nothing is configured on the boards. The client sends the MAC address of its network interface
(`eth0`, the one the DHCP server knows; `--iface` to change it) and the server assigns `batch_id`,
`multipmt_id` and the numeric id of the data stream (evproducer) from
`server/multipmt_config_files/multipmt_registry.json`, written by hand:

```
{
  "fc:0f:e7:e5:f8:be": {"multipmt_id": "cile247", "batch_id": "batch_1", "numeric_id": 0}
}
```

- One entry per Main Board. `numeric_id` is written in the data files: never reuse or change it.
- MAC, `numeric_id` and `multipmt_id` must be unique: a file with errors is refused as a whole (the
  server keeps the previous valid version and logs the reason).
- A board whose MAC is not in the file is rejected at the handshake, with the reason on both sides;
  the client retries every 30 s. The server reads the file again when it changes: no restart needed.
- Moving a Main Board to another multiPMT means changing its `multipmt_id`/`batch_id` here; its
  `numeric_id` stays with the board.

To use a board with another set of PMTs only for a while, select their configuration file from the
server prompt instead of editing the registry. The choice is kept in RAM for that board (its MAC), also
if it disconnects and reconnects, until `configuration reset` or the restart of the server:

```
configuration set --client-id auger-batch_4-e5f8be --multipmt-id cile1 --batch-id batch_1
configuration show
configuration reset --client-id auger-batch_4-e5f8be
```

The client keeps only a local cache (the I2C bus of the Main Board sensors) in
`/var/lib/multidaq/client_state.json` (`~/.local/state/multidaq/` when not run as root).

