import datetime
import logging
import os
import sys
from typing import Callable

LOG_LEVEL: int = logging.DEBUG
LOG_FORMAT: str = "%(asctime)s [%(levelname)s] %(name)s %(funcName)s {%(lineno)d}: %(message)s"

APRS_SERVER: str = "rotate.aprs.net"
APRS_PORT: int = 14580  # 10152
APRS_CALLSIGN: str = "N0CALL"
APRS_FILTER: str = ""
APRS_HEARTBEAT_INTERVAL: datetime.timedelta = datetime.timedelta(minutes=10)

INFLUXDB_URL: str = "http://influxdb:8086"
INFLUXDB_TOKEN: str = ""
INFLUXDB_ORG: str = "aprs2influxdb"
INFLUXDB_BUCKET: str = "aprs2influxdb"


def _parse_bool(value: str = "") -> bool:
    return bool(value.strip().lower() in ["t", "true", "1"])


def _parse_datetime_timedelta(value: str = "") -> datetime.timedelta:
    interval_format: str

    if len(value.split(":")) == 1:
        interval_format = "%S"
    elif len(value.split(":")) == 2:
        interval_format = "%M:%S"
    elif len(value.split(":")) == 3:
        interval_format = "%H:%M:%S"
    else:
        raise ValueError("Invalid time delta")

    if "." in value:
        interval_format += ".%f"

    ts: datetime.datetime = datetime.datetime.strptime(value, interval_format)
    return datetime.timedelta(hours=ts.hour, minutes=ts.minute, seconds=ts.second, microseconds=ts.microsecond)


def config_parse_command_line() -> None:
    pass


def config_parse_environment() -> None:
    current_module = sys.modules[__name__]
    var_names: list[str] = [x for x in dir(current_module) if (not x.startswith("_") and x.isupper())]
    for var_name in var_names:
        var_default = getattr(current_module, var_name)
        var_type = type(var_default)

        if var_name in ["LOG_LEVEL"]:
            var_value = logging.getLevelName(os.environ.get(var_name, logging.getLevelName(var_default)))
        elif var_name in ["APRS_HEARTBEAT_INTERVAL"]:
            var_value = _parse_datetime_timedelta(os.environ.get(var_name, str(var_default)))
        elif var_type == bool:
            var_value = _parse_bool(os.environ.get(var_name, str(var_default)))
        else:
            var_value = var_type(os.environ.get(var_name, str(var_default)))

        setattr(current_module, var_name, var_value)


def config_print(logging_method: Callable = print) -> None:
    logging_method("###########################################################")

    current_module = sys.modules[__name__]
    var_names: list[str] = [x for x in dir(current_module) if (not x.startswith("_") and x.isupper())]
    for var_name in var_names:
        logging_method(f"{var_name}: {getattr(current_module, var_name)}")

    logging_method("###########################################################")


if __name__ == "__main__":
    config_parse_environment()
    config_print()
