import logging
from data_processing.ml_modeling import temperature_predict
from interpolation.interpolation import spatial_interpolation
from database_operations.influx_manager import get_data, write_predictions
from spatial_processing.visualization import map_plotting
from data_processing.saving_utils import save_color_scale
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

    image_time = pd.to_datetime(df["Time"].iloc[0]).ceil("h")
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
    sides,
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
        df["Longitude"].to_numpy(), df["Latitude"].to_numpy()
    )

    inv_affine = ~transform_matrix
    cols_f, rows_f = inv_affine * (xs, ys)

    cols_i = np.rint(cols_f).astype(np.int64)
    rows_i = np.rint(rows_f).astype(np.int64)

    h, w = elevation_data.shape
    in_bounds = (rows_i >= 0) & (rows_i < h) & (cols_i >= 0) & (cols_i < w)

    elevation = np.full(rows_i.shape, np.nan, dtype=np.float32)
    elevation[in_bounds] = elevation_data[rows_i[in_bounds], cols_i[in_bounds]].astype(
        np.float32
    )
    df["Elevation"] = elevation

    df["Hour"] = df["Time"].dt.hour.astype(np.int16)
    df["Day"] = df["Time"].dt.dayofyear.astype(np.int16)
    return df


def process_data_round(
    config,
    db_ops,
    geo_proc,
    czech_rep,
    elevation_data,
    transform_matrix,
    crs,
    start_time=None,
    end_time=None,
    link_ids=None,
):
    """
    Processes data for a given time range. If no range is provided, processes the current hour.
    """
    start_time = start_time or datetime.datetime.now().replace(
        minute=0, second=0, microsecond=0
    )
    end_time = end_time or start_time + datetime.timedelta(hours=1)

    current_time = start_time
    while current_time < end_time:
        try:
            backend_logger.info(f"Processing data for hour: {current_time}")
            df = get_data(
                config, current_time, current_time + datetime.timedelta(hours=1)
            )
            latitudes, longitudes, azimuths, links, technologies, sides = (
                db_ops.get_metadata(df)
            )
            backend_logger.info(f"Link IDs passed to process_data_round: {link_ids}")

            df = prepare_data(
                df,
                elevation_data,
                transform_matrix,
                crs,
                latitudes,
                longitudes,
                azimuths,
                links,
                technologies,
                sides,
            )

            if link_ids is not None:
                mask = df["Link_ID"].isin(link_ids)
                df = df[mask].reset_index(drop=True)
                print(df)
                backend_logger.info(
                    f"Filtered dataset to {len(df)} rows based on Link_IDs."
                )
                if df.empty:
                    backend_logger.warning(
                        "No data available after filtering by Link_IDs."
                    )
                    current_time += datetime.timedelta(hours=1)
                    continue

            image_name, image_time = collect_data_summary(df)

            ml_cfg = config.get_ml()
            df = temperature_predict(
                df,
                scaler_path=ml_cfg["scaler_path"],
                lstm_model_path=ml_cfg["lstm_path"],
            )

            itp = config.get_interpolation_config()
            grid = config.get_grid_config()
            grid_x, grid_y, grid_z = spatial_interpolation(
                df,
                czech_rep,
                geo_proc,
                elevation_data,
                transform_matrix,
                crs,
                variogram_model=itp["variogram_model"],
                nlags=itp["nlags"],
                regression_model_type=itp["regression_model"],
                grid_x_points=grid["x_points"],
                grid_y_points=grid["y_points"],
            )

            # write_predictions(df, config)
            map_plotting(grid_x, grid_y, grid_z, czech_rep, image_name, config)

        except Exception as e:
            backend_logger.error(
                f"Error during data processing for hour {current_time}: {e}\n{traceback.format_exc()}"
            )
        finally:
            current_time += datetime.timedelta(hours=1)
            gc.collect()
    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    backend_logger.info(
        f"Calculation ended on {end_datetime}. Waiting for another round.."
    )
