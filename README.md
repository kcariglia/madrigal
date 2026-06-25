# Madrigal HAPI Server

A [HAPI](https://hapi-server.org/) (Heliophysics API) server that streams
upper-atmospheric and space-physics time-series data to any standard HAPI
client. It presents one uniform HAPI interface in front of several different
back-end data sources via pluggable "reader" programs.

This repository is a fork of the
[upstream reference server](https://github.com/hapi-server/server-python),
originally by jbfaden with a Python 3 update by sandyfreelance (2021 onward).
The Madrigal and SuperMAG readers are local additions to that base.

## Introduction

The server reads a per-mission configuration file plus a set of HAPI-compliant
JSON files, and uses a *reader* program to convert your data into HAPI-formatted
output. Conceptually:

* The **config file** (`<mission>_config.py`) tells the server where its files
  live and which reader to use.
* The **JSON files** (`capabilities.json`, `catalog.json`, `info/*.json`)
  declare to the server and to users what datasets and parameters are available.
* The **reader** uses those same JSON keys to locate the requested data in the
  back-end and return it as CSV.

The server can stream per-file as data is processed, or fetch everything then
serve, controlled by `stream_flag` in the config (see below).

## Requirements

Install dependencies with:

```text
pip install -r requirements.txt
```

Not every package is needed for every mission — `requirements.txt` groups them
by component (core server, Madrigal reader, NetCDF reader, SuperMAG reader). The
catalog-generation script `populateMadHAPI.py` additionally requires a full
server-side Madrigal installation (the `madrigal` package), which is not
pip-installable; it is only needed to (re)generate JSON metadata, not to serve.

## Usage

```text
python hapi_server.py <MISSIONNAME> [localhost|http|https|custom]
```

(If no arguments are provided, defaults to `csv` and `localhost`.)

`MISSIONNAME` selects the `<MISSIONNAME>_config.py` file to load. The second
argument selects the port:

| Mode        | Port                                  |
| ----------- | ------------------------------------- |
| `localhost` | localhost:8080                        |
| `http`      | 80                                    |
| `https`     | 443                                   |
| `custom`    | a custom port hardcoded in the server |

Examples:

```text
python hapi_server.py csv localhost       # CSV sample data on :8080
python hapi_server.py madtest localhost   # Madrigal test mission
python3.12 hapi_server.py supermag custom # SuperMAG pass-through
```

Once running, try the endpoints, e.g. `http://localhost:8080/hapi/catalog`.

## Bundled missions and readers

Each mission has a `<name>_config.py` config file, a `home_<name>/` data
directory, and a reader module:

| Mission    | Config               | Reader                   | Source                                              |
| ---------- | -------------------- | ------------------------ | --------------------------------------------------- |
| `csv`      | `csv_config.py`      | `csv_hapireader.py`      | Local CSV flat files                                |
| `madtest`  | `madtest_config.py`  | `madhapireader.py`       | Madrigal database (HTTPS query via `madrigalWeb`)   |
| `netcdf`   | `netcdf_config.py`   | `netcdf_hapireader.py`   | NetCDF files (GUVI satellite UV imager)             |
| `supermag` | `supermag_config.py` | `supermag_hapireader.py` | SuperMAG magnetometer-network web API (pass-through)|

### CSV reader

Reads flat files laid out as `data/[id]/YYYY/[id].YYYYMMDD.csv`. Streams
per-file.

### Madrigal reader

Queries a remote [Madrigal](https://madrigalweb.haystack.mit.edu/) site over
HTTPS. Dataset IDs use the `kinst_kindat` form (e.g. `8250_1700` = Jicamarca
magnetometer, kind-of-data 1700). `madhapi_api.py` maps Madrigal IDs and
parameters to/from HAPI form, and `populateMadHAPI.py` generates the
`catalog.json` / `info/*.json` metadata from Madrigal's instrument catalog.

### NetCDF reader

Reads NetCDF files (e.g. GUVI) via `xarray` and emits CSV for a predefined set
of keys. Streams per-file.

### SuperMAG reader

Passes requests through to the SuperMAG web API using the APL-distributed
`supermag_api.py` client. Because it is a live web pass-through, it does **not**
stream (`stream_flag=False`); it fetches then serves. Supports HAPI subparams
such as `delta=start` and `baseline=yearly` (see `tags_allowed` in its config).

## Sample data sets

Unzipped sample trees are included for each mission: `home_csv/`,
`home_madtest/`, `home_netcdf/`, and `home_supermag/`. Each contains the
HAPI JSON (`capabilities.json`, `catalog.json`, `info/*.json`) and, where
applicable, sample data (e.g. `home_csv/data/...`, `home_netcdf/rawdata/...`).
Zipped archives `home_csv.zip` and `home_netcdf.zip` are also provided.

## Writing your own reader

The architecture is reader-plugin based and you are encouraged to add your own.
A reader is a module exposing a `do_data_*` handler that the config file wires
up via `hapi_handler`. The handler signature follows the bundled examples
(see `madhapireader.do_data_madrigal`):

```python
def do_data_<source>(
    id: str,                 # HAPI dataset id
    timemin: str,            # ISO8601 start
    timemax: str,            # ISO8601 stop
    parameters: list[str],   # requested parameters
    catalog=None,
    floc=None,               # file-location dict from config
    stream_flag=False,
    stream=None,             # output stream when streaming per-file
) -> tuple[int, str]:        # (status_code, csv_data)
    ...
```

The reader reads your data and returns CSV for the selected subset of variables.

### Required config variables

A `<mission>_config.py` must define (the loader verifies these on startup):

| Variable        | Purpose                                                                |
| --------------- | ---------------------------------------------------------------------- |
| `HAPI_HOME`     | Directory holding this mission's JSON + data (`home_<name>/`)          |
| `title`         | Human-readable server title                                            |
| `api_datatype`  | `'file'` or `'web'`                                                    |
| `floc`          | File-location dict passed to the reader (e.g. `{'dir': ...}`)          |
| `hapi_handler`  | The reader's `do_data_*` callable                                      |
| `tags_allowed`  | Allowed HAPI subparams (e.g. `['delta=start']`); `['']` if none        |
| `loaded_config` | `True` — sentinel used to confirm the config loaded                    |
| `stream_flag`   | `True` = stream per-file (recommended); `False` = fetch-all then serve |

Use `stream_flag=False` when you need to post-process before sending, when data
sets are small, or when the source is a web pass-through (as with SuperMAG).

## Configuration notes

* `capabilities.json` and `catalog.json` live in `HAPI_HOME`; `info` responses
  live in `HAPI_HOME/info/`. IDs must be defined in `info/*.json`, per HAPI.
* Time templates are supported in responses: `"lasthour"` means the last hour
  boundary, and `"lastday-P1D"` means the last midnight minus one day.

## Fun fact

Although intended as a big-data server, this began as a module that ran on
jfaden's Raspberry Pi — and it should still work on one.
