from telcotemp.data_sources.base import DataSource
from .influx_reader import CMLInfluxReader
from pyproj import Transformer
import pandas as pd
import numpy as np


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
        self._to_map_crs = Transformer.from_crs("EPSG:4326", self.crs, always_xy=True)
        self._location = self.config.get_location()

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

        metadata_rows = [
            {
                "IP": ip,
                "Latitude": values.get("lat"),
                "Longitude": values.get("lon"),
                "Azimuth": values.get("azimuth"),
                "Altitude": values.get("altitude"),
                "Link_ID": values.get("link_id"),
                "Technology": values.get("technology"),
                "Side": values.get("side"),
            }
            for ip, values in metadata.items()
        ]
        metadata_df = pd.DataFrame.from_records(
            metadata_rows,
            columns=[
                "IP",
                "Latitude",
                "Longitude",
                "Azimuth",
                "Altitude",
                "Link_ID",
                "Technology",
                "Side",
            ],
        )
        df = df.merge(metadata_df, on="IP", how="left")

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
        from telcotemp.utils.time_utils import is_daylight

        df["sun"] = df["Time"].apply(
            lambda ts: is_daylight(
                ts,
                self._location["lat"],
                self._location["lng"],
                self._location["tz"],
            )
        )

        # Add time features
        df["Hour"] = df["Time"].dt.hour
        df["Day"] = df["Time"].dt.dayofyear

        # Transform coordinates to EPSG:3857
        xs, ys = self._to_map_crs.transform(
            df["Longitude"].values, df["Latitude"].values
        )
        df["X"] = xs
        df["Y"] = ys

        # Calculate elevation from DEM for each point
        x_raster, y_raster = self._to_map_crs.transform(
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
