import asyncio
import logging
import signal

import uvloop as uvloop

import appconfig
from aprs2influxdb import APRS2InfluxDB


async def main() -> None:
    appconfig.config_parse_environment()
    appconfig.config_parse_command_line()
    appconfig.config_print()

    logging.basicConfig(level=appconfig.LOG_LEVEL, format=appconfig.LOG_FORMAT)

    app: APRS2InfluxDB = APRS2InfluxDB()

    def signal_handler(_, __):
        asyncio.create_task(app.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)

    await app.start()

    await app.gather()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
