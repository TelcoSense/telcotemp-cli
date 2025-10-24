from spatial_processing.visualization import MapVisualizer
from data_processing.ml_modeling import temperature_predict
from interpolation.interpolation import SpatialInterpolator
from utils.time_utils import is_daylight
from data_processing.saving_utils import save_color_scale
import pandas as pd
import datetime
import gc
import traceback
import numpy as np
from rasterio.transform import Affine
from rasterio.crs import CRS
from pyproj import Transformer


class DataProcessor:
    """
    Class to handle the data processing pipeline.
    """

    def __init__(
        self,
        config,
        db_ops,
        geo_proc,
        czech_rep,
        elevation_data,
        transform_matrix,
        crs,
        logger,
        influx_handler,
    ):
        self.config = config
        self.db_ops = db_ops
        self.geo_proc = geo_proc
        self.czech_rep = czech_rep
        self.elevation_data = elevation_data
        self.transform_matrix = transform_matrix
        self.crs = crs
        self.logger = logger
        self.map_visualizer = MapVisualizer(config, logger)
        self.interpolator = SpatialInterpolator(config, logger)
        self.influx_handler = influx_handler

    def process_time_range(
        self, start_time: datetime.datetime, end_time: datetime.datetime, link_ids=None
    ):
        """
        Processes data for a given time range.
        """
        current_time = start_time
        while current_time < end_time:
            try:
                self.logger.info(f"Processing data for hour: {current_time}")
                df = self._fetch_data(current_time)
                if df.empty:
                    self.logger.warning("No data fetched for the current hour.")
                    current_time += datetime.timedelta(hours=1)
                    continue

                df = self._prepare_data(df)
                if link_ids:
                    df = self._filter_by_link_ids(df, link_ids)
                    if df.empty:
                        current_time += datetime.timedelta(hours=1)
                        continue

                image_name, image_time = self._collect_data_summary(df)
                df = self._predict_temperature(df)
                grid_x, grid_y, grid_z = self._interpolate(df)
                self._save_results(df, grid_x, grid_y, grid_z, image_name)

            except Exception as e:
                self.logger.error(
                    f"Error during data processing for hour {current_time}: {e}\n{traceback.format_exc()}"
                )
            finally:
                current_time += datetime.timedelta(hours=1)
                gc.collect()

        self.logger.info("Processing completed for the given time range.")

    def _fetch_data(self, current_time: datetime.datetime) -> pd.DataFrame:
        """
        Fetches data from the database for the given time.
        """
        return self.influx_handler.fetch_data(
            current_time, current_time + datetime.timedelta(hours=1)
        )

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares the data by adding metadata and elevation information.
        """
        latitudes, longitudes, azimuths, links, technologies, sides = (
            self.db_ops.get_metadata(df)
        )
        df["Azimuth"] = azimuths
        df["Latitude"] = latitudes
        df["Longitude"] = longitudes
        df["Link_ID"] = links
        df["Technology"] = technologies
        df["Side"] = sides
        df = df.dropna(subset=["Latitude", "Longitude", "Time"])

        location = self.config.get_location()
        df["sun"] = df["Time"].apply(
            lambda ts: is_daylight(ts, location["lat"], location["lng"], location["tz"])
        )
        print(df)
        transformer = Transformer.from_crs("EPSG:4326", self.crs, always_xy=True)
        xs, ys = transformer.transform(
            df["Longitude"].to_numpy(), df["Latitude"].to_numpy()
        )
        inv_affine = ~self.transform_matrix
        cols_f, rows_f = inv_affine * (xs, ys)

        cols_i = np.rint(cols_f).astype(np.int64)
        rows_i = np.rint(rows_f).astype(np.int64)
        h, w = self.elevation_data.shape
        in_bounds = (rows_i >= 0) & (rows_i < h) & (cols_i >= 0) & (cols_i < w)

        elevation = np.full(rows_i.shape, np.nan, dtype=np.float32)
        elevation[in_bounds] = self.elevation_data[
            rows_i[in_bounds], cols_i[in_bounds]
        ].astype(np.float32)
        df["Elevation"] = elevation

        df["Hour"] = df["Time"].dt.hour.astype(np.int16)
        df["Day"] = df["Time"].dt.dayofyear.astype(np.int16)
        return df

    def _filter_by_link_ids(self, df: pd.DataFrame, link_ids: list) -> pd.DataFrame:
        """
        Filters the data by the provided Link_IDs.
        """
        mask = df["Link_ID"].isin(link_ids)
        df = df[mask].reset_index(drop=True)
        self.logger.info(f"Filtered dataset to {len(df)} rows based on Link_IDs.")
        if df.empty:
            self.logger.warning("No data available after filtering by Link_IDs.")
        return df

    def _collect_data_summary(self, df: pd.DataFrame):
        """
        Collects summary information for the data.
        """
        image_time = pd.to_datetime(df["Time"].iloc[0]).ceil("h")
        image_hour = image_time.strftime("%Y-%m-%d_%H%M")
        image_name = f"{image_hour}.png"
        return image_name, image_time

    def _predict_temperature(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Predicts temperature using the ML model.
        """
        ml_cfg = self.config.get_ml()
        return temperature_predict(
            df,
            scaler_path=ml_cfg["scaler_path"],
            lstm_model_path=ml_cfg["lstm_path"],
        )

    def _interpolate(self, df: pd.DataFrame):
        """
        Performs spatial interpolation on the data.
        """
        return self.interpolator.interpolate(
            df,
            self.czech_rep,
            self.geo_proc,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
        )

    def _save_results(self, df: pd.DataFrame, grid_x, grid_y, grid_z, image_name: str):
        """
        Saves the results including predictions and visualizations.
        """
        self.influx_handler.write_data(df)
        self.map_visualizer.map_plotting(
            grid_x, grid_y, grid_z, self.czech_rep, image_name
        )
