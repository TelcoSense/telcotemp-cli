from src.data_sources.base import DataSource
from .influx_reader import CMLInfluxReader
from pyproj import Transformer
import pandas as pd
import numpy as np
from rasterio.transform import rowcol


class CMLDataSource(DataSource):
    """CML-specific data source implementation."""

    def __init__(self, config, logger, metadata_provider, geo_components):
        super().__init__(config, logger)
        self.influx_reader = CMLInfluxReader(config, logger)
        self.metadata_provider = metadata_provider

        (
            self.geo_proc,
            self.czech_rep,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
        ) = geo_components

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

        # Calculate elevation from DEM for each point
        # Transform coordinates to raster CRS
        to_raster = Transformer.from_crs("EPSG:4326", self.crs, always_xy=True)
        x_raster, y_raster = to_raster.transform(
            df["Longitude"].to_numpy(), df["Latitude"].to_numpy()
        )
        
        # Get raster indices using inverse affine transformation
        inv_affine = ~self.transform_matrix
        cols_f, rows_f = inv_affine * (x_raster, y_raster)
        
        # Round to nearest integer indices
        cols_i = np.rint(cols_f).astype(np.int64)
        rows_i = np.rint(rows_f).astype(np.int64)
        
        # Check bounds
        h, w = self.elevation_data.shape
        in_bounds = (rows_i >= 0) & (rows_i < h) & (cols_i >= 0) & (cols_i < w)
        
        # Extract elevation values
        elevation = np.full(rows_i.shape, np.nan, dtype=np.float32)
        elevation[in_bounds] = self.elevation_data[
            rows_i[in_bounds], cols_i[in_bounds]
        ].astype(np.float32)
        
        df["Elevation"] = elevation

        self.logger.info(f"Prepared {len(df)} CML records")
        return df

    def supports_ml_prediction(self):
        """CML supports ML prediction."""
        return True

    def get_temperature_column(self):
        """After ML prediction, temperature is in Predicted_Temperature."""
        return "Predicted_Temperature"
