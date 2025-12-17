from influxdb_client import InfluxDBClient
import pandas as pd
from src.data_sources.base import InfluxReader


class CMLInfluxReader(InfluxReader):
    """Read CML data from InfluxDB."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        influx_cfg = config.get_influx_config("read")
        self.url = influx_cfg["url"]
        self.token = influx_cfg["token"]
        self.org = influx_cfg["org"]
        self.bucket = influx_cfg["bucket"]
        self.measurements = influx_cfg["measurements"]
        self.fields = influx_cfg["fields"]
        self.window = influx_cfg["window"]
        self.tag_device = influx_cfg["tag_device"]

    def read(self, start_time, end_time):

        """Fetch CML data and pivot Temperature_MW + Signal."""
        query = self._build_query(start_time, end_time)

        self.logger.debug(f"Fetching CML data from {start_time} to {end_time}")

        try:
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                result = client.query_api().query(org=self.org, query=query)
        except Exception as e:
            self.logger.error(f"Error fetching from InfluxDB: {e}")
            return pd.DataFrame()

        # Process results
        records = []
        for table in result:
            for record in table.records:
                records.append(
                    {
                        "Time": record.get_time(),
                        "Device": record.values.get("agent_host"),
                        "Measurement": record.get_field(),
                        "Value": record.get_value(),
                    }
                )

        if not records:
            self.logger.warning("No data returned from InfluxDB")
            return pd.DataFrame()

        df = pd.DataFrame(records)

        # Pivot to get Temperature_MW and Signal columns
        df_pivot = df.pivot_table(
            index=["Time", "Device"], columns="Measurement", values="Value"
        ).reset_index()

        df_pivot["Time"] = pd.to_datetime(df_pivot["Time"], utc=True)
        df_pivot.rename(
            columns={"Device": "IP", "Teplota": "Temperature_MW", "PrijimanaUroven": "Signal"},
            inplace=True,
        )

        self.logger.info(f"Fetched {len(df_pivot)} CML records")
        print(df_pivot)
        return df_pivot

    def _build_query(self, start_time, end_time):
        """Build Flux query."""
        start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        measurements = " or ".join([f'r["_measurement"] == "{m}"' for m in self.measurements])
        fields = " or ".join([f'r["_field"] == "{f}"' for f in self.fields])

        return f"""
                from(bucket: "{self.bucket}")
                  |> range(start: {start_iso}, stop: {end_iso})
                  |> filter(fn: (r) => {measurements})
                  |> filter(fn: (r) => {fields})
                  |> aggregateWindow(every: {self.window}, fn: mean)
                  |> group(columns: ["_measurement", "_field", "{self.tag_device}"])
            """
