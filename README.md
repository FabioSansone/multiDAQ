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
multidaq-client --server-ip <server address> --hv-port /dev/ttyPS1
```

The default mode of the server is `test` (acquisition without high voltage).

At the server prompt, `connect --num_clients <n>` opens the ports the clients connect to: 8888 (control), 8889 (acquisition) and 8890 (monitoring). The client has the same defaults.
