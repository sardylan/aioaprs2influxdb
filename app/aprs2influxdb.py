import asyncio
import datetime
import logging
from typing import Optional

from aioaprs.client import AioAPRSClient
from aioaprs.config import AioAPRSClientConfig
from aioaprs.enrichers.telemetry import TelemetryEnricher
from aioaprs.packets import PacketType
from aioaprs.parser import PacketParser
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api_async import WriteApiAsync

import appconfig

_logger = logging.getLogger(__name__)


class APRS2InfluxDB:
    _lock: asyncio.Lock = asyncio.Lock()

    _aprs: Optional[AioAPRSClient]

    _influx: Optional[InfluxDBClientAsync]
    _influx_write_api: Optional[WriteApiAsync]

    _telemetry_enricher: TelemetryEnricher = TelemetryEnricher()

    def __init__(self) -> None:
        super().__init__()

        self._aprs = None
        self._influx = None
        self._influx_write_api = None

    async def start(self) -> None:
        async with self._lock:
            try:
                await self._start_aprs()
                await self._start_influxdb()
            except Exception as e:
                await self._stop_aprs()
                await self._stop_influxdb()
                raise e

    async def stop(self) -> None:
        async with self._lock:
            await self._stop_influxdb()
            await self._stop_aprs()

    async def gather(self) -> None:
        await self._aprs.gather()

    async def _start_aprs(self) -> None:
        client_config: AioAPRSClientConfig = AioAPRSClientConfig()
        client_config.host = appconfig.APRS_SERVER
        client_config.port = appconfig.APRS_PORT
        client_config.callsign = appconfig.APRS_CALLSIGN
        client_config.server_filter = appconfig.APRS_FILTER
        client_config.heartbeat = appconfig.APRS_HEARTBEAT_INTERVAL.total_seconds()
        self._aprs = AioAPRSClient(client_config, callback=self._callback_aprs_raw_packet)
        await self._aprs.connect()

    async def _stop_aprs(self) -> None:
        if self._aprs:
            await self._aprs.close()
            self._aprs = None

    async def _start_influxdb(self) -> None:
        url: str = appconfig.INFLUXDB_URL
        token: str = appconfig.INFLUXDB_TOKEN
        org: str = appconfig.INFLUXDB_ORG

        self._influx = InfluxDBClientAsync(
            url=url,
            token=token,
            org=org
        )

        await self._influx.ping()

        self._influx_write_api = self._influx.write_api()

    async def _stop_influxdb(self) -> None:
        if self._influx_write_api:
            self._influx_write_api = None

        if self._influx:
            await self._influx.close()
            self._influx = None

    async def _callback_aprs_raw_packet(self, raw_packet: str):
        _logger.debug(f"APRS Packet: {raw_packet}")

        packet_parser: PacketParser = PacketParser(raw_packet)
        packet: dict = packet_parser.parse()

        if packet["type"] == PacketType.MESSAGE:
            self._telemetry_enricher.parse(packet)

        if packet["type"] == PacketType.TELEMETRY_DATA:
            self._telemetry_enricher.enrich(packet)

        fields: dict = dict()

        for field_name in [
            "source",
            "destination",
            "path",
            "via",
            "type",
            "values_real",
            "project_name",
            "unit_labels"
        ]:
            if field_name not in packet:
                return

            if isinstance(packet[field_name], list):
                fields[field_name] = ",".join([str(x) for x in packet[field_name]])
            elif isinstance(packet[field_name], PacketType):
                fields[field_name] = packet[field_name].name
            else:
                fields[field_name] = packet[field_name]

        dictionary: dict = {
            "measurement": "APRS",
            "tags": {
                "source": packet["source"],
                "destination": packet["destination"],
                "via": packet["via"],
                "type": packet["type"].name,
            },
            "fields": fields,
            "time": datetime.datetime.utcnow()
        }

        point: Point = Point.from_dict(dictionary=dictionary)

        async with self._lock:
            if not self._influx_write_api:
                return

            await self._influx_write_api.write(
                bucket=appconfig.INFLUXDB_BUCKET,
                record=point
            )

            print("created")
