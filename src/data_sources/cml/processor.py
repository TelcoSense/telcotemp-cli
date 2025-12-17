from src.data_sources.base import DataSource
from .influx_reader import CMLInfluxReader
from pyproj import Transformer
import pandas as pd


class CMLDataSource(DataSource):
    """CML-specific data source implementation."""

    def __init__(self, config, logger, metadata_provider):
        super().__init__(config, logger)
        self.influx_reader = CMLInfluxReader(config, logger)
        self.metadata_provider = metadata_provider
        self.crs = "EPSG:3857"

    def fetch_data(self, start_time, end_time):
        """Fetch CML data from InfluxDB."""
        return self.influx_reader.read(start_time, end_time)

    def prepare_data(self, df, filter_ids=None):
        """
        Prepare CML data:
        1. Enrich with metadata
        2. Add daylight flag
        3. Add time features
        4. Transform coordinates
        5. Filter by link_ids if provided
        """
        if df.empty:
            return df

        # Get unique IPs
        df = df.copy()
        ips = df["IP"].unique().tolist()
        metadata = self.metadata_provider.fetch_metadata(ips)

        # Map metadata to dataframe
        df["Latitude"] = df["IP"].map(lambda ip: metadata.get(ip, {}).get("lat"))
        df["Longitude"] = df["IP"].map(lambda ip: metadata.get(ip, {}).get("lon"))
        df["Azimuth"] = df["IP"].map(lambda ip: metadata.get(ip, {}).get("azimuth"))
        df["Link_ID"] = df["IP"].map(lambda ip: metadata.get(ip, {}).get("link_id"))
        df["Technology"] = df["IP"].map(lambda ip: metadata.get(ip, {}).get("technology"))
        df["Side"] = df["IP"].map(lambda ip: metadata.get(ip, {}).get("side"))

        # Drop rows without metadata
        df = df.dropna(subset=["Latitude", "Longitude", "Link_ID"])

        if df.empty:
            self.logger.warning("No data after metadata enrichment")
            return df

        # Filter by link_ids if provided
        if filter_ids:
            df = df[df["Link_ID"].isin(filter_ids)]
            self.logger.info(f"Filtered to {len(df)} records for specified link_ids")

        # Add daylight flag
        from src.utils.time_utils import is_daylight
        location = self.config.get_location()
        df["sun"] = df["Time"].apply(
            lambda ts: is_daylight(ts, location["lat"], location["lng"], location["tz"])
        )

        # Add time features
        df["Hour"] = df["Time"].dt.hour
        df["Day"] = df["Time"].dt.dayofyear

        # Transform coordinates to EPSG:3857
        transformer = Transformer.from_crs("EPSG:4326", self.crs, always_xy=True)
        xs, ys = transformer.transform(df["Longitude"].values, df["Latitude"].values)
        df["X"] = xs
        df["Y"] = ys

        # Add elevation (placeholder for now - will be filled during interpolation)
        df["Elevation"] = 0

        self.logger.info(f"Prepared {len(df)} CML records")
        return df

    def supports_ml_prediction(self):
        """CML supports ML prediction."""
        return True

    def get_temperature_column(self):
        """After ML prediction, temperature is in Predicted_Temperature."""
        return "Predicted_Temperature"
