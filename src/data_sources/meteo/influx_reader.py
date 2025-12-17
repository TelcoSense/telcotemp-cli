from influxdb_client import InfluxDBClient
import pandas as pd
from src.data_sources.base import InfluxReader


class MeteoInfluxReader(InfluxReader):
    """Read weather station data from InfluxDB."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        influx_cfg = config.get_influx_config()
        self.url = influx_cfg["url"]
        self.token = influx_cfg["token"]
        self.org = influx_cfg["org"]
        self.window = influx_cfg["window"]
        self.bucket = influx_cfg["bucket"]
        self.measurements = influx_cfg["measurements"]
        self.fields = influx_cfg["fields"]

    def read(self, start_time, end_time):
        """Fetch weather station data."""
        query = self._build_query(start_time, end_time)

        self.logger.debug(f"Fetching Meteo data from {start_time} to {end_time}")

        try:
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                result = client.query_api().query(org=self.org, query=query)
        except Exception as e:
            self.logger.error(f"Error fetching from InfluxDB: {e}")
            return pd.DataFrame()

        records = []
        for table in result:
            for record in table.records:
                records.append(
                    {
                        "Time": record.get_time(),
                        "ID": record.values.get("_field"),
                        "Temperature": record.get_value(),
                    }
                )

        if not records:
            self.logger.warning("No data returned from InfluxDB")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["Time"] = pd.to_datetime(df["Time"], utc=True)

        self.logger.info(f"Fetched {len(df)} Meteo records")
        print(df)
        return df

    def _build_query(self, start_time, end_time):
        """Build Flux query for meteo data."""
        start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        measurements_filter = " or ".join([f'r["_measurement"] == "{m}"' for m in self.measurements])

        query = f"""
from(bucket: "{self.bucket}")
  |> range(start: {start_iso}, stop: {end_iso})
  |> filter(fn: (r) => {measurements_filter})
  |> aggregateWindow(every: {self.window}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value", "_field", "agent_host"])
"""
        return query
