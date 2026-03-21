from influxdb_client import InfluxDBClient
import pandas as pd
from telcotemp.data_sources.base import InfluxReader


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
                result = client.query_api().query_data_frame(
                    org=self.org, query=query
                )
        except Exception as e:
            self.logger.error(f"Error fetching from InfluxDB: {e}")
            return pd.DataFrame()

        frames = result if isinstance(result, list) else [result]
        frames = [frame for frame in frames if isinstance(frame, pd.DataFrame) and not frame.empty]
        if not frames:
            self.logger.warning("No data returned from InfluxDB")
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        drop_columns = [
            column
            for column in ["result", "table", "_start", "_stop"]
            if column in df.columns
        ]
        if drop_columns:
            df = df.drop(columns=drop_columns)
        if "_time" not in df.columns:
            self.logger.warning("Meteo query returned no _time column")
            return pd.DataFrame()

        value_columns = [
            column for column in df.columns if column not in {"_time", "agent_host"}
        ]
        if not value_columns:
            self.logger.warning("Meteo query returned no station columns after pivot")
            return pd.DataFrame()

        df = df.melt(
            id_vars=["_time"],
            value_vars=value_columns,
            var_name="ID",
            value_name="Temperature",
        )
        df = df.dropna(subset=["Temperature"])
        df.rename(columns={"_time": "Time"}, inplace=True)
        df["Time"] = pd.to_datetime(df["Time"], utc=True)
        df = df.sort_values(["Time", "ID"]).reset_index(drop=True)

        self.logger.info(f"Fetched {len(df)} Meteo records")
        return df

    def _build_query(self, start_time, end_time):
        """Build Flux query for meteo data."""
        start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        measurements_filter = " or ".join(
            [f'r["_measurement"] == "{m}"' for m in self.measurements]
        )

        query = f"""
from(bucket: "{self.bucket}")
  |> range(start: {start_iso}, stop: {end_iso})
  |> filter(fn: (r) => {measurements_filter})
  |> aggregateWindow(every: {self.window}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value", "_field"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
"""
        return query
