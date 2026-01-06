import os
import sys
import numpy as np
from rasterio.transform import rowcol
from pyproj import Transformer
from pykrige.rk import RegressionKriging
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.svm import SVR
import warnings
from scipy.linalg import LinAlgWarning
from contextlib import contextmanager


@contextmanager
def suppress_stdout():
    old = sys.stdout
    try:
        with open(os.devnull, "w") as f:
            sys.stdout = f
            yield
    finally:
        sys.stdout = old


class SpatialInterpolator:
    """Unified spatial interpolator for both modes."""

    def __init__(self, config, logger):
        """
        Initializes the SpatialInterpolator with configuration parameters.

        :param config: AppConfig instance for accessing interpolation configuration.
        :param logger: Logger instance for logging messages.
        """
        self.logger = logger
        self._load_config(config)

    def _load_config(self, config):
        """
        Loads the interpolation configuration from the provided config object.

        :param config: AppConfig instance for accessing interpolation configuration.
        """
        itp = config.get_interpolation_config()
        grid = config.get_grid_config()
        self.variogram_model = itp["variogram_model"]
        self.nlags = itp["nlags"]
        self.regression_model_type = itp["regression_model"]
        self.grid_x_points = grid["x_points"]
        self.grid_y_points = grid["y_points"]

    def _get_regression_model(self):
        """
        Returns the regression model based on the specified type.

        :return: An instance of the selected regression model.
        """
        if self.regression_model_type == "linear":
            return LinearRegression()
        elif self.regression_model_type == "random_forest":
            return RandomForestRegressor(n_estimators=100, random_state=42)
        elif self.regression_model_type == "gradient_boosting":
            return GradientBoostingRegressor(n_estimators=100, random_state=42)
        elif self.regression_model_type == "svr":
            return SVR(kernel="rbf", C=1.0, epsilon=0.1)
        else:
            raise ValueError(
                f"Unknown regression model type: {self.regression_model_type}"
            )

    def _prepare_training_data(
        self, df, elevation_data, transform_matrix, crs, temp_column
    ):
        """
        Prepares training data for regression kriging.

        :param df: DataFrame containing the input data.
        :param elevation_data: 2D array of elevation data.
        :param transform_matrix: Affine transformation matrix for the raster.
        :param crs: Coordinate reference system of the raster.
        :param temp_column: Name of temperature column to use
        :return: Tuple of elevation data, coordinates, temperature values, and mean elevation.
        """
        lon = df["Longitude"].values
        lat = df["Latitude"].values
        temp = df[temp_column].values

        to_raster = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        x_pts_raster, y_pts_raster = to_raster.transform(lon, lat)

        df2 = df[[temp_column]].copy()
        df2["_x"] = x_pts_raster
        df2["_y"] = y_pts_raster

        # rounding in meters in raster CRS; 1–10 m usually enough
        round_m = 1.0
        df2["_xr"] = (df2["_x"] / round_m).round(0) * round_m
        df2["_yr"] = (df2["_y"] / round_m).round(0) * round_m

        df2 = df2.groupby(["_xr", "_yr"], as_index=False)[temp_column].median()

        x_pts_raster = df2["_xr"].to_numpy()
        y_pts_raster = df2["_yr"].to_numpy()
        temp = df2[temp_column].to_numpy()

        rows, cols = rowcol(transform_matrix, x_pts_raster, y_pts_raster)
        rows = np.clip(np.floor(rows).astype(int), 0, elevation_data.shape[0] - 1)
        cols = np.clip(np.floor(cols).astype(int), 0, elevation_data.shape[1] - 1)
        valid_elev = elevation_data[rows, cols]

        mean_elev = np.nanmean(valid_elev)
        fallback = 0.0 if np.isnan(mean_elev) else mean_elev
        valid_elev = np.nan_to_num(valid_elev, nan=fallback)

        return (
            valid_elev.reshape(-1, 1),
            np.c_[x_pts_raster, y_pts_raster],
            temp,
            fallback,
        )

    def _prepare_prediction_grid(
        self, grid_x, grid_y, elevation_data, transform_matrix, crs, rep_crs, fallback
    ):
        """
        Prepares the prediction grid for regression kriging.

        :param grid_x: X-coordinates of the grid.
        :param grid_y: Y-coordinates of the grid.
        :param elevation_data: 2D array of elevation data.
        :param transform_matrix: Affine transformation matrix for the raster.
        :param crs: Coordinate reference system of the raster.
        :param rep_crs: Coordinate reference system of the grid.
        :param fallback: Mean elevation to fill missing data in the grid.
        :return: Tuple of elevation data and coordinates for the prediction grid.
        """
        to_raster = Transformer.from_crs(rep_crs, crs, always_xy=True)
        grid_x_flat = grid_x.ravel()
        grid_y_flat = grid_y.ravel()
        grid_x_raster, grid_y_raster = to_raster.transform(grid_x_flat, grid_y_flat)

        rows, cols = rowcol(transform_matrix, grid_x_raster, grid_y_raster)
        rows = np.clip(np.floor(rows).astype(int), 0, elevation_data.shape[0] - 1)
        cols = np.clip(np.floor(cols).astype(int), 0, elevation_data.shape[1] - 1)
        grid_elev = elevation_data[rows, cols]

        grid_elev = np.nan_to_num(grid_elev, nan=fallback)

        return grid_elev.reshape(-1, 1), np.c_[grid_x_raster, grid_y_raster]

    def interpolate(
        self,
        df,
        rep,
        geo_proc,
        elevation_data,
        transform_matrix,
        crs,
        temp_column="Predicted_Temperature",
    ):
        """
        Performs spatial interpolation using regression kriging.

        :param df: DataFrame containing the input data.
        :param rep: Geodataframe representing the region of interest.
        :param geo_proc: Instance of GeographicalProcessing for mask creation.
        :param elevation_data: 2D array of elevation data.
        :param transform_matrix: Affine transformation matrix for the raster.
        :param crs: Coordinate reference system of the raster.
        :param temp_column: Name of temperature column (Predicted_Temperature or Temperature_Value)
        :return: Tuple of grid X-coordinates, grid Y-coordinates, and predicted temperature grid.
        """
        self.logger.info(
            "spatial_interpolation start (model=%s, variogram=%s, nlags=%s)",
            self.regression_model_type,
            self.variogram_model,
            self.nlags,
        )
        try:
            rep_crs = getattr(rep, "crs", None) or "EPSG:4326"
            bounds = rep.total_bounds
            grid_x, grid_y = np.mgrid[
                bounds[0] : bounds[2] : complex(self.grid_x_points),
                bounds[1] : bounds[3] : complex(self.grid_y_points),
            ]
            mask = geo_proc.create_mask(rep, grid_x, grid_y)

            valid_points = (
                df["Longitude"].notna()
                & df["Latitude"].notna()
                & df[temp_column].notna()
            )
            if valid_points.sum() < 3:
                raise ValueError(
                    "Not enough valid measurements for kriging (at least 3 required)."
                )

            X_train, coords_train, temp, mean_elev = self._prepare_training_data(
                df.loc[valid_points], elevation_data, transform_matrix, crs, temp_column
            )

            regression_model = self._get_regression_model()
            rk = RegressionKriging(
                regression_model=regression_model,
                variogram_model=self.variogram_model,
                n_closest_points=self.nlags,
            )
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=LinAlgWarning)
                with suppress_stdout():
                    rk.fit(X_train, coords_train, temp)

            X_pred, coords_pred = self._prepare_prediction_grid(
                grid_x,
                grid_y,
                elevation_data,
                transform_matrix,
                crs,
                rep_crs,
                mean_elev,
            )
            with suppress_stdout():
                grid_predicted_temp = rk.predict(X_pred, coords_pred).reshape(
                    grid_x.shape
                )

            grid_predicted_temp = np.where(
                mask.reshape(grid_x.shape), grid_predicted_temp, np.nan
            )
            return grid_x, grid_y, grid_predicted_temp

        except Exception as e:
            self.logger.exception("Exception in spatial_interpolation: %s", e)
            raise
