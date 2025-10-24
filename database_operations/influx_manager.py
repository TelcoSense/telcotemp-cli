from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd


class InfluxDBHandler:
    def __init__(self, config, logger):
        """
        Initializes the InfluxDBHandler with configuration and logger.

        :param config: AppConfig instance for accessing InfluxDB configuration.
        :param logger: Logger instance for logging messages.
        """
        self.config = config
        self.logger = logger

        # Load common InfluxDB configuration
        influx_common = self.config.get_influx_config("read")
        self.url = influx_common["url"]
        self.token = influx_common["token"]
        self.org = influx_common["org"]

        if not self.url or not self.token or not self.org:
            raise ValueError(
                "InfluxDB configuration is incomplete. Ensure 'url', 'token', and 'org' are set."
            )

    def _build_query(self, start_time, end_time, read_cfg):
        """
        Builds the Flux query for fetching data from InfluxDB.

        :param start_time: Start time for the query.
        :param end_time: End time for the query.
        :param read_cfg: Configuration for reading data.
        :return: Flux query string.
        """
        start_time_iso = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time_iso = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        measurements = " or ".join(
            [f'r["_measurement"] == "{m}"' for m in read_cfg["measurements"]]
        )
        fields = " or ".join([f'r["_field"] == "{f}"' for f in read_cfg["fields"]])
        return f"""
                from(bucket: "{read_cfg["bucket"]}")
                  |> range(start: {start_time_iso}, stop: {end_time_iso})
                  |> filter(fn: (r) => {measurements})
                  |> filter(fn: (r) => {fields})
                  |> aggregateWindow(every: {read_cfg["window"]}, fn: mean)
                  |> group(columns: ["_measurement", "_field", "{read_cfg["tag_device"]}"])
            """

    def fetch_data(self, start_time, end_time):
        """
        Fetches data from InfluxDB for the given time range.

        :param start_time: Start time for the query.
        :param end_time: End time for the query.
        :return: DataFrame containing the fetched data.
        """
        try:
            read_cfg = self.config.get_influx_config("read")
            query = self._build_query(start_time, end_time, read_cfg)
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                result = client.query_api().query(org=self.org, query=query)
            return self._process_results(result)
        except Exception as e:
            self.logger.error(f"Failed to fetch data: {e}")
            return pd.DataFrame()

    def _process_results(self, result):
        """
        Processes the results returned by the InfluxDB query.

        :param result: Query result from InfluxDB.
        :return: Processed DataFrame.
        """
        data = []
        for table in result:
            for rec in table.records:
                try:
                    data.append(
                        {
                            "Time": rec.get_time(),
                            "Measurement": rec.values.get("_field"),
                            "Value": rec.get_value(),
                            "Device": rec.values.get(
                                self.config.get_influx_config("read")["tag_device"]
                            ),
                        }
                    )
                except KeyError as e:
                    self.logger.warning(f"Missing expected key in record: {e}")

        df = pd.DataFrame(data)
        if df.empty:
            self.logger.info("Influx returned empty data for the given time range.")
            return df

        read_cfg = self.config.get_influx_config("read")
        field_temp = read_cfg["field_temperature"]
        field_sig = read_cfg["field_signal"]

        df_pivot = df.pivot_table(
            index=["Time", "Device"], columns="Measurement", values="Value"
        ).reset_index()

        df_pivot["Time"] = pd.to_datetime(df_pivot["Time"], utc=True)
        df_pivot["Unix"] = df_pivot["Time"].astype("int64") // 10**9
        if field_temp in df_pivot.columns:
            df_pivot.rename(columns={field_temp: "Temperature_MW"}, inplace=True)
        if field_sig in df_pivot.columns:
            df_pivot.rename(columns={field_sig: "Signal"}, inplace=True)

        return df_pivot.rename(columns={"Device": "IP"})

    def write_data(self, df):
        """
        Writes data to InfluxDB.

        :param df: DataFrame containing the data to write.
        """
        write_cfg = self.config.get_influx_config("write")
        try:
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
                points = self._prepare_points(df, write_cfg)
                write_api.write(bucket=write_cfg["bucket"], record=points)
                self.logger.info(f"Wrote {len(points)} points to InfluxDB.")
        except Exception as e:
            self.logger.error(f"Failed to write data: {e}")

    def _prepare_points(self, df, write_cfg):
        """
        Prepares data points for writing to InfluxDB.

        :param df: DataFrame containing the data.
        :param write_cfg: Configuration for writing data.
        :return: List of InfluxDB points.
        """
        points = []
        for _, row in df.iterrows():
            try:
                point = (
                    Point(write_cfg["measurement"])
                    .tag(write_cfg["tag_cml_id"], row["Link_ID"])
                    .tag(write_cfg["tag_side"], row["Side"])
                    .field(write_cfg["field_temperature"], row["Predicted_Temperature"])
                    .time(row["Time"].to_pydatetime())
                )
                points.append(point)
            except Exception as e:
                self.logger.warning(f"Skipping row due to error: {e}")
        return points
