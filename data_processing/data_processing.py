import logging
from data_processing.ml_modeling import temperature_predict
from interpolation.interpolation import spatial_interpolation
from database_operations.influx_manager import get_data, write_predictions
from spatial_processing.visualization import map_plotting
import pandas as pd
import datetime
import gc
import traceback
import numpy as np
from rasterio.transform import Affine
from rasterio.crs import CRS
from pyproj import Transformer

backend_logger = logging.getLogger("backend_logger")
first_run = True


def collect_data_summary(df):

    image_time = pd.to_datetime(df["Time"].iloc[0]).ceil('h')
    image_hour = image_time.strftime("%Y-%m-%d_%H%M")
    image_name = f"{image_hour}.png"

    return image_name, image_time


def prepare_data(
        df: pd.DataFrame,
        elevation_data: np.ndarray,
        transform_matrix: Affine,
        crs: CRS,
        latitudes,
        longitudes,
        azimuths,
        links,
        technologies,
        sides
) -> pd.DataFrame:
    df["Azimuth"] = azimuths
    df["Latitude"] = latitudes
    df["Longitude"] = longitudes
    df["Link_ID"] = links
    df["Technology"] = technologies
    df["Side"] = sides

    df = df.dropna(subset=["Latitude", "Longitude", "Time"])

    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    xs, ys = transformer.transform(
        df["Longitude"].to_numpy(),
        df["Latitude"].to_numpy()
    )

    inv_affine = ~transform_matrix
    cols_f, rows_f = inv_affine * (xs, ys)

    cols_i = np.rint(cols_f).astype(np.int64)
    rows_i = np.rint(rows_f).astype(np.int64)

    h, w = elevation_data.shape
    in_bounds = (rows_i >= 0) & (rows_i < h) & (cols_i >= 0) & (cols_i < w)

    elevation = np.full(rows_i.shape, np.nan, dtype=np.float32)
    elevation[in_bounds] = elevation_data[rows_i[in_bounds], cols_i[in_bounds]].astype(np.float32)
    df["Elevation"] = elevation

    df["Hour"] = df["Time"].dt.hour.astype(np.int16)
    df["Day"] = df["Time"].dt.dayofyear.astype(np.int16)
    return df


def process_data_round(config, db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs):
    global first_run
    start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    backend_logger.info(f"Calculation started on {start_datetime}")

    try:

        df = get_data(config)
        latitudes, longitudes, azimuths, links, technologies, sides = db_ops.get_metadata(df)
        df = prepare_data(df, elevation_data, transform_matrix, crs, latitudes, longitudes, azimuths, links,
                          technologies,
                          sides)
        unique_links_list, image_name, image_time = collect_data_summary(df)

        ml_cfg = config.get_ml()
        df = temperature_predict(df, scaler_path=ml_cfg["scaler_path"], lstm_model_path=ml_cfg["lstm_path"])

        itp = config.get_interpolation_config()
        grid = config.get_grid_config()
        grid_x, grid_y, grid_z = spatial_interpolation(
            df, czech_rep, geo_proc, elevation_data, transform_matrix, crs,
            variogram_model=itp["variogram_model"],
            nlags=itp["nlags"],
            regression_model_type=itp["regression_model"],
            grid_x_points=grid["x_points"],
            grid_y_points=grid["y_points"]
        )

        write_predictions(df, config)
        map_plotting(grid_x, grid_y, grid_z, czech_rep, image_name, config)
    except Exception as e:
        backend_logger.error(f"Error during data processing round: {e}\n{traceback.format_exc()}")

    finally:
        if "df" in locals():
            del df
        gc.collect()
    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    backend_logger.info(
        f"Calculation ended on {end_datetime}. Waiting for another round.."
    )
