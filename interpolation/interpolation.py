import numpy as np
from rasterio.transform import rowcol
from pyproj import Transformer
from pykrige.rk import RegressionKriging
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.svm import SVR
import logging

backend_logger = logging.getLogger('backend_logger')

def spatial_interpolation(
    df,
    rep,
    geo_proc,
    elevation_data,
    transform_matrix,
    crs,
    variogram_model='spherical',
    nlags=40,
    regression_model_type='linear',
    grid_x_points=500,
    grid_y_points=500
):
    backend_logger.info("spatial_interpolation start (model=%s, variogram=%s, nlags=%s)",
                        regression_model_type, variogram_model, nlags)
    try:
        rep_crs = getattr(rep, "crs", None) or "EPSG:4326"

        bounds = rep.total_bounds
        grid_x, grid_y = np.mgrid[
            bounds[0]:bounds[2]:complex(grid_x_points),
            bounds[1]:bounds[3]:complex(grid_y_points)
        ]
        mask = geo_proc.create_mask(rep, grid_x, grid_y)

        valid_points = (~df['Longitude'].isna()) & (~df['Latitude'].isna()) & (~df['Predicted_Temperature'].isna())
        if valid_points.sum() < 3:
            raise ValueError("Málo platných měření pro kriging (potřeba alespoň 3).")

        lon = df.loc[valid_points, 'Longitude'].values
        lat = df.loc[valid_points, 'Latitude'].values
        temp = df.loc[valid_points, 'Predicted_Temperature'].values

        to_raster_from_wgs = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        x_pts_raster, y_pts_raster = to_raster_from_wgs.transform(lon, lat)

        to_raster_from_rep = Transformer.from_crs(rep_crs, crs, always_xy=True)
        grid_x_flat_rep = grid_x.ravel()
        grid_y_flat_rep = grid_y.ravel()
        grid_x_raster, grid_y_raster = to_raster_from_rep.transform(grid_x_flat_rep, grid_y_flat_rep)

        rows, cols = rowcol(transform_matrix, x_pts_raster, y_pts_raster)
        rows = np.clip(np.floor(rows).astype(int), 0, elevation_data.shape[0] - 1)
        cols = np.clip(np.floor(cols).astype(int), 0, elevation_data.shape[1] - 1)
        valid_elev = elevation_data[rows, cols]
        if np.isnan(valid_elev).any():
            mean_elev = np.nanmean(valid_elev)
            valid_elev = np.nan_to_num(valid_elev, nan=(0.0 if np.isnan(mean_elev) else mean_elev))

        if regression_model_type == 'linear':
            regression_model = LinearRegression()
        elif regression_model_type == 'random_forest':
            regression_model = RandomForestRegressor(n_estimators=100, random_state=42)
        elif regression_model_type == 'gradient_boosting':
            regression_model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
        elif regression_model_type == 'svr':
            regression_model = SVR(kernel='rbf', C=1.0, epsilon=0.1)
        else:
            raise ValueError(f"Unknown regression model type: {regression_model_type}")

        X_train = valid_elev.reshape(-1, 1)
        coords_train = np.c_[x_pts_raster, y_pts_raster]
        rk = RegressionKriging(
            regression_model=regression_model,
            variogram_model=variogram_model,
            n_closest_points=nlags
        )
        rk.fit(X_train, coords_train, temp)

        grid_rows, grid_cols = rowcol(transform_matrix, grid_x_raster, grid_y_raster)
        grid_rows = np.clip(np.floor(grid_rows).astype(int), 0, elevation_data.shape[0] - 1)
        grid_cols = np.clip(np.floor(grid_cols).astype(int), 0, elevation_data.shape[1] - 1)
        grid_elev = elevation_data[grid_rows, grid_cols]

        if np.isnan(grid_elev).any():
            mean_elev = np.nanmean(valid_elev)
            grid_elev = np.nan_to_num(grid_elev, nan=(0.0 if np.isnan(mean_elev) else mean_elev))

        X_pred = grid_elev.reshape(-1, 1)
        coords_pred = np.c_[grid_x_raster, grid_y_raster]
        grid_predicted_temp = rk.predict(X_pred, coords_pred).reshape(grid_x.shape)

        grid_predicted_temp = np.where(mask.reshape(grid_x.shape), grid_predicted_temp, np.nan)
        return grid_x, grid_y, grid_predicted_temp

    except Exception as e:
        backend_logger.exception("Exception in spatial_interpolation: %s", e)
        raise
