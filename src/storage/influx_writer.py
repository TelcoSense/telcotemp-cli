from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxWriter:
    """Writes predicted temperatures to InfluxDB (CML mode only)."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        write_cfg = config.get_influx_config("write")
        self.url = write_cfg["url"]
        self.token = write_cfg["token"]
        self.org = write_cfg["org"]
        self.bucket = write_cfg["bucket"]
        self.measurement = write_cfg["measurement"]
        self.tag_cml_id = write_cfg["tag_cml_id"]
        self.tag_side = write_cfg["tag_side"]
        self.field_temperature = write_cfg["field_temperature"]
        
        # Check if writing is enabled
        self.write_enabled = config.is_write_enabled()
        if not self.write_enabled:
            self.logger.warning("Database writes are DISABLED (debug mode)")

    def write(self, df):
        """Write dataframe to InfluxDB."""
        if df.empty:
            self.logger.warning("Empty dataframe, nothing to write")
            return

        if not self.write_enabled:
            self.logger.info(f"DEBUG MODE: Skipping write of {len(df)} points to InfluxDB")
            return

        points = []
        for _, row in df.iterrows():
            point = (
                Point(self.measurement)
                .tag(self.tag_cml_id, str(row["Link_ID"]))
                .tag(self.tag_side, str(row["Side"]))
                .field(self.field_temperature, float(row["Predicted_Temperature"]))
                .time(row["Time"].to_pydatetime())
            )
            points.append(point)

        try:
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
                write_api.write(bucket=self.bucket, record=points)

            self.logger.info(f"Wrote {len(points)} points to InfluxDB")
        except Exception as e:
            self.logger.error(f"Error writing to InfluxDB: {e}")
            raise
