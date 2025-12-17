from src.data_sources.base import DataSource
from .influx_reader import MeteoInfluxReader
from pyproj import Transformer


class MeteoDataSource(DataSource):
    """Weather station data source implementation."""

    def __init__(self, config, logger, metadata_provider):
        super().__init__(config, logger)
        self.influx_reader = MeteoInfluxReader(config, logger)
        self.metadata_provider = metadata_provider
        self.crs = "EPSG:3857"

    def fetch_data(self, start_time, end_time):
        """Fetch weather station data from InfluxDB."""
        return self.influx_reader.read(start_time, end_time)

    def prepare_data(self, df, filter_ids=None):
        """
        Prepare meteo data:
        1. Enrich with metadata
        2. Transform coordinates
        3. Filter by station_ids if provided
        """
        if df.empty:
            return df

        # Get unique station IDs
        ids = df["ID"].unique().tolist()
        metadata = self.metadata_provider.fetch_metadata(ids)

        # Map metadata to dataframe
        df["Latitude"] = df["ID"].map(lambda id: metadata.get(id, {}).get("lat"))
        df["Longitude"] = df["ID"].map(lambda id: metadata.get(id, {}).get("lon"))
        df["Elevation"] = df["ID"].map(lambda id: metadata.get(id, {}).get("elev"))

        # Drop rows without metadata
        df = df.dropna(subset=["Latitude", "Longitude"])

        if df.empty:
            self.logger.warning("No data after metadata enrichment")
            return df

        # Filter by station_ids if provided
        if filter_ids:
            df = df[df["ID"].isin(filter_ids)]
            self.logger.info(f"Filtered to {len(df)} records for specified stations")

        # Transform coordinates to EPSG:3857
        transformer = Transformer.from_crs("EPSG:4326", self.crs, always_xy=True)
        xs, ys = transformer.transform(df["Longitude"].values, df["Latitude"].values)
        df["X"] = xs
        df["Y"] = ys

        # Rename Temperature to Temperature_Value for unified interface
        df["Temperature_Value"] = df["Temperature"]

        self.logger.info(f"Prepared {len(df)} Meteo records")
        return df

    def supports_ml_prediction(self):
        """Meteo does NOT support ML prediction."""
        return False

    def get_temperature_column(self):
        """Meteo uses direct Temperature column."""
        return "Temperature_Value"
