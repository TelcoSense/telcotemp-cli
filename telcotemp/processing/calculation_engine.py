from datetime import datetime, timedelta, timezone
from telcotemp.data_sources.cml.processor import CMLDataSource
from telcotemp.data_sources.meteo.processor import MeteoDataSource
from telcotemp.processing.ml_modeling import temperature_predict
from telcotemp.geo.interpolation import SpatialInterpolator
from telcotemp.visualization.map_visualizer import MapVisualizer
from telcotemp.storage.influx_writer import InfluxWriter
from telcotemp.storage.file_writer import FileWriter
from telcotemp.core.config import AppConfig
from telcotemp.utils.map_cleanup import MapCleanup
from telcotemp.utils.diagnostics import run_bias_report
import threading
import time
import math
import numpy as np
import pandas as pd


class CalculationEngine:
    """Unified calculation engine for both CML and Meteo modes."""

    def __init__(
        self,
        config,
        logger_manager,
        metadata_provider,
        geo_components,
        mode_override=None,
    ):
        """
        Initialize calculation engine.

        :param config: AppConfig instance
        :param logger_manager: LoggerManager instance
        :param metadata_provider: CMLMetadataProvider, MeteoMetadataProvider, or tuple of both
        :param geo_components: Tuple of (geo_proc, czech_rep, elevation_data, transform_matrix, crs)
        :param mode_override: Override mode for combined mode (internal use)
        """
        self.config = config
        self.logger = logger_manager.get_logger("backend_logger")
        self.mode = mode_override if mode_override else config.mode
        self.map_cleanup = MapCleanup(config, self.logger)

        (
            self.geo_proc,
            self.czech_rep,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
        ) = geo_components

        if self.mode == "cml":
            self.data_source = CMLDataSource(
                config, self.logger, metadata_provider, geo_components
            )
            self.influx_writer = InfluxWriter(config, self.logger)
        elif self.mode == "meteo":
            self.data_source = MeteoDataSource(config, self.logger, metadata_provider)
            self.influx_writer = None
        else:
            raise ValueError(f"Invalid mode for single engine: {self.mode}")

        self.interpolator = SpatialInterpolator(config, self.logger)
        self.visualizer = MapVisualizer(config, self.logger)
        self.file_writer = FileWriter(config, self.logger)

    # ---------------------------------------------------------------------
    # HELPERS
    # ---------------------------------------------------------------------

    def _dedupe_points_for_kriging(self, df, temp_column: str):
        """Reduce duplicate/near-duplicate measurement locations before kriging.

        This helps avoid numerical issues (e.g., singular matrices) inside the
        kriging solver when many records share identical coordinates.

        Requirements:
        - columns: Longitude, Latitude, temp_column
        - optionally uses X/Y (meters) if present to group in projected space

        Returns a dataframe that still contains Longitude/Latitude (required by
        SpatialInterpolator).
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        required = ["Longitude", "Latitude", temp_column]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"Missing required columns for interpolation: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        # Coerce temperature to float and drop invalid rows
        df[temp_column] = pd.to_numeric(df[temp_column], errors="coerce")
        df = df.dropna(subset=["Longitude", "Latitude", temp_column])
        if df.empty:
            return df

        # Grouping key:
        # - prefer projected meters if available (X/Y are created in prepare_data)
        # - otherwise use rounded lon/lat in degrees
        if "X" in df.columns and "Y" in df.columns:
            round_m = 1.0  # meters; 1–10 m is typically enough
            df["_xr"] = (pd.to_numeric(df["X"], errors="coerce") / round_m).round(
                0
            ) * round_m
            df["_yr"] = (pd.to_numeric(df["Y"], errors="coerce") / round_m).round(
                0
            ) * round_m
        else:
            # ~1e-5 deg ≈ 1 m (order-of-magnitude)
            df["_xr"] = pd.to_numeric(df["Longitude"], errors="coerce").round(5)
            df["_yr"] = pd.to_numeric(df["Latitude"], errors="coerce").round(5)

        agg = {temp_column: "median", "Longitude": "median", "Latitude": "median"}
        if "Elevation" in df.columns:
            agg["Elevation"] = "median"

        df2 = df.groupby(["_xr", "_yr"], as_index=False).agg(agg)

        # Keep only finite temperatures
        df2 = df2[np.isfinite(df2[temp_column].to_numpy(dtype=float))]
        return df2

    def _interpolate_with_fallback(self, df, temp_column: str):
        """Run regression kriging; fall back to regression-only surface on numerical failure."""
        try:
            return self.interpolator.interpolate(
                df,
                self.czech_rep,
                self.geo_proc,
                self.elevation_data,
                self.transform_matrix,
                self.crs,
                temp_column=temp_column,
            )
        except Exception as e:
            msg = str(e).lower()
            is_numerical = (
                "singular" in msg or "ill-conditioned" in msg or "linalgerror" in msg
            )
            if not is_numerical:
                raise

            self.logger.warning(
                "[%s] Kriging failed (%s). Falling back to regression-only surface.",
                self.mode.upper(),
                e,
            )

            rep = self.czech_rep
            bounds = rep.total_bounds
            grid_x, grid_y = np.mgrid[
                bounds[0] : bounds[2] : complex(self.interpolator.grid_x_points),
                bounds[1] : bounds[3] : complex(self.interpolator.grid_y_points),
            ]
            mask = self.geo_proc.create_mask(rep, grid_x, grid_y)

            valid = (
                df["Longitude"].notna()
                & df["Latitude"].notna()
                & df[temp_column].notna()
            )
            if valid.sum() < 2:
                raise ValueError(
                    "Not enough valid measurements for regression-only fallback."
                )

            X_train, _coords, temp, mean_elev = (
                self.interpolator._prepare_training_data(
                    df.loc[valid],
                    self.elevation_data,
                    self.transform_matrix,
                    self.crs,
                    temp_column,
                )
            )
            model = self.interpolator._get_regression_model()
            model.fit(X_train, temp)

            X_pred, _coords_pred = self.interpolator._prepare_prediction_grid(
                grid_x,
                grid_y,
                self.elevation_data,
                self.transform_matrix,
                self.crs,
                getattr(rep, "crs", None) or "EPSG:4326",
                mean_elev,
            )
            grid_pred = model.predict(X_pred).reshape(grid_x.shape)
            grid_pred = np.where(mask.reshape(grid_x.shape), grid_pred, np.nan)
            return grid_x, grid_y, grid_pred

    def process_time_range(self, start_time, end_time, filter_ids=None):
        current_time = start_time

        while current_time < end_time:
            self.logger.info(f"[{self.mode.upper()}] Processing {current_time}")

            try:
                # 1. Fetch data
                df = self.data_source.fetch_data(
                    current_time, current_time + timedelta(hours=1)
                )

                if df.empty:
                    self.logger.warning(
                        f"[{self.mode.upper()}] No data for {current_time}"
                    )
                    current_time += timedelta(hours=1)
                    continue

                # 2. Prepare data (metadata, coordinates, filtering)
                df = self.data_source.prepare_data(df, filter_ids)

                if df.empty:
                    self.logger.warning(
                        f"[{self.mode.upper()}] No data after preparation for {current_time}"
                    )
                    current_time += timedelta(hours=1)
                    continue

                # 3. ML Prediction (only for CML)
                if self.data_source.supports_ml_prediction():
                    ml_cfg = self.config.get_ml()
                    if ml_cfg:
                        df = temperature_predict(
                            df,
                            ml_cfg["scaler_path"],
                            ml_cfg["lstm_path"],
                            ml_cfg["bias_offset"],
                        )
                        self.logger.info(
                            f"[{self.mode.upper()}] ML prediction completed"
                        )
                    else:
                        self.logger.warning(
                            f"[{self.mode.upper()}] ML config not found, skipping prediction"
                        )
                        current_time += timedelta(hours=1)
                        continue

                # 4. Interpolation
                temp_column = self.data_source.get_temperature_column()

                # 5. Deduplication
                df_interp = self._dedupe_points_for_kriging(df, temp_column)

                if df_interp.empty or len(df_interp) < 3:
                    self.logger.warning(
                        "[%s] Not enough unique points for interpolation at %s (n=%d). Skipping.",
                        self.mode.upper(),
                        current_time,
                        len(df_interp),
                    )
                    current_time += timedelta(hours=1)
                    continue

                self.logger.debug(
                    "[%s] Interp points: raw=%d, unique_xy=%d",
                    self.mode.upper(),
                    len(df),
                    len(df_interp),
                )

                grid_x, grid_y, grid_z = self._interpolate_with_fallback(
                    df_interp, temp_column=temp_column
                )

                # 5. Visualization
                image_time = current_time.replace(minute=0, second=0, microsecond=0)

                # Compute median temperature for filename (use original df, not deduped)
                try:
                    vals = df[temp_column].to_numpy(dtype=float)
                    seg_temp = float(np.nanmedian(vals))
                except Exception:
                    seg_temp = float("nan")

                if math.isnan(seg_temp):
                    seg_temp_str = "NA"
                else:
                    if abs(seg_temp) < 0.05:
                        seg_temp = 0.0
                    seg_temp_str = f"{seg_temp:.1f}"

                image_name = (
                    f"{image_time.strftime('%Y-%m-%d_%H%M')}_{seg_temp_str}.png"
                )

                paths = self.config.get_paths()
                output_dir = (
                    paths.get("cml_dir")
                    if self.mode == "cml"
                    else paths.get("meteo_dir")
                )

                self.visualizer.plot(
                    grid_x, grid_y, grid_z, self.czech_rep, image_name, output_dir
                )

                # 6. Storage
                if self.influx_writer and self.mode == "cml":
                    self.influx_writer.write(df)

                self.file_writer.save_grid(grid_x, grid_y, grid_z, image_name)

                self.logger.debug(
                    f"[{self.mode.upper()}] Successfully processed {current_time}"
                )

            except Exception as e:
                self.logger.exception(
                    f"[{self.mode.upper()}] Error processing {current_time}: {e}"
                )

            current_time += timedelta(hours=1)

    def process_historical_data(self, start_time, end_time, filter_ids=None):
        self.logger.info(
            f"[{self.mode.upper()}] Processing historical data from {start_time} to {end_time}"
        )
        self.process_time_range(start_time, end_time, filter_ids)

    def data_processing_loop(
        self, first_run=False, start_time=None, end_time=None, filter_ids=None
    ):
        if first_run:
            self.logger.info(
                f"[{self.mode.upper()}] First run: Processing data for the last week."
            )

            now = datetime.now(timezone.utc)
            historical_end_time = now.replace(minute=0, second=0, microsecond=0)
            historical_start_time = historical_end_time - timedelta(days=7)

            self.process_historical_data(
                historical_start_time, historical_end_time, filter_ids
            )
            self.logger.info(
                f"[{self.mode.upper()}] Historical processing completed at {historical_end_time}"
            )

            now_updated = datetime.now(timezone.utc)
            safe_processing_time = (now_updated - timedelta(minutes=90)).replace(
                minute=0, second=0, microsecond=0
            )

            if historical_end_time < safe_processing_time:
                gap_hours = int(
                    (safe_processing_time - historical_end_time).total_seconds() / 3600
                )
                self.logger.info(
                    f"[{self.mode.upper()}] Filling gap of {gap_hours} hours from "
                    f"{historical_end_time} to {safe_processing_time}"
                )
                self.process_time_range(
                    historical_end_time, safe_processing_time, filter_ids
                )
                self.logger.info(
                    f"[{self.mode.upper()}] Gap filled, now at {safe_processing_time}"
                )
            else:
                self.logger.info(
                    f"[{self.mode.upper()}] No gap to fill, ready for continuous mode"
                )

            self.logger.info(f"[{self.mode.upper()}] Switching to continuous mode.")

        elif start_time and end_time:
            self.logger.info(
                f"[{self.mode.upper()}] Processing time range: {start_time} to {end_time}"
            )
            self.process_historical_data(start_time, end_time, filter_ids)
            return

        self.logger.info(f"[{self.mode.upper()}] Starting continuous processing")

        last_processed_time = None

        while True:
            now = datetime.now(timezone.utc)

            if now.hour == 3 and self.map_cleanup.enabled:
                self.logger.info(f"[{self.mode.upper()}] Running daily map cleanup")
                self.map_cleanup.cleanup_all_outputs()

            safe_processing_time = (now - timedelta(minutes=90)).replace(
                minute=0, second=0, microsecond=0
            )

            if last_processed_time != safe_processing_time:
                self.logger.info(
                    f"[{self.mode.upper()}] Processing safe time window: "
                    f"{safe_processing_time.strftime('%Y-%m-%d %H:00')}"
                )

                self.process_time_range(
                    safe_processing_time,
                    safe_processing_time + timedelta(hours=1),
                    filter_ids,
                )
                last_processed_time = safe_processing_time
            else:
                self.logger.info(
                    f"[{self.mode.upper()}] Time {safe_processing_time.strftime('%Y-%m-%d %H:00')} "
                    f"already processed, waiting for next hour."
                )

            now = datetime.now(timezone.utc)
            next_run_minute = 40

            if now.minute < next_run_minute:
                next_run = now.replace(minute=next_run_minute, second=0, microsecond=0)
            else:
                next_run = (now + timedelta(hours=1)).replace(
                    minute=next_run_minute, second=0, microsecond=0
                )

            sleep_seconds = (next_run - now).total_seconds()
            self.logger.info(
                f"[{self.mode.upper()}] Sleeping {sleep_seconds/60:.1f} minutes "
                f"until {next_run.strftime('%H:%M')}"
            )
            time.sleep(sleep_seconds)


class CombinedCalculationEngine:
    """Combined calculation engine that runs both CML and Meteo modes in parallel."""

    def __init__(
        self,
        config,
        logger_manager,
        cml_metadata_provider,
        meteo_metadata_provider,
        geo_components,
    ):
        """
        Initialize combined calculation engine.

        :param config: AppConfig instance (with mode="combined")
        :param logger_manager: LoggerManager instance
        :param cml_metadata_provider: CMLMetadataProvider
        :param meteo_metadata_provider: MeteoMetadataProvider
        :param geo_components: Tuple of (geo_proc, czech_rep, elevation_data, transform_matrix, crs)
        """
        self.config = config
        self.logger = logger_manager.get_logger("backend_logger")
        self.map_cleanup = MapCleanup(config, self.logger)

        cml_config = AppConfig(config_dir=config.config_dir, mode="cml")
        meteo_config = AppConfig(config_dir=config.config_dir, mode="meteo")

        self.cml_engine = CalculationEngine(
            cml_config,
            logger_manager,
            cml_metadata_provider,
            geo_components,
            mode_override="cml",
        )
        self.meteo_engine = CalculationEngine(
            meteo_config,
            logger_manager,
            meteo_metadata_provider,
            geo_components,
            mode_override="meteo",
        )

        self.logger.info("Combined calculation engine initialized (CML + Meteo)")

    def _get_meteo_available_time(self, target_time):
        return target_time + timedelta(hours=1, minutes=30)

    def process_time_range(
        self, start_time, end_time, cml_filter_ids=None, meteo_filter_ids=None
    ):
        self.logger.info(
            f"[COMBINED] Processing time range: {start_time} to {end_time}"
        )

        current_time = start_time

        while current_time < end_time:
            hour_start = current_time
            hour_end = current_time + timedelta(hours=1)

            self.logger.debug(
                f"[COMBINED] Processing hour: {hour_start.strftime('%Y-%m-%d %H:%M')}"
            )

            available_at = self._get_meteo_available_time(hour_start)
            now = datetime.now().replace(tzinfo=hour_start.tzinfo)

            if now < available_at:
                self.logger.warning(
                    f"[COMBINED] Meteo data not yet available (available at {available_at.strftime('%H:%M')}). "
                    f"Processing CML only for now."
                )
                self.cml_engine.process_time_range(hour_start, hour_end, cml_filter_ids)
            else:
                cml_thread = threading.Thread(
                    target=self.cml_engine.process_time_range,
                    args=(hour_start, hour_end, cml_filter_ids),
                    name="CML-Thread",
                )
                meteo_thread = threading.Thread(
                    target=self.meteo_engine.process_time_range,
                    args=(hour_start, hour_end, meteo_filter_ids),
                    name="Meteo-Thread",
                )

                cml_thread.start()
                meteo_thread.start()

                cml_thread.join()
                meteo_thread.join()

                diag_conf = self.config.get_diagnostics_config()

                if diag_conf["enable_bias_report"]:
                    self.logger.info("[COMBINED] Bias report enabled")
                    out_dir = diag_conf["bias_report_dir"]
                    run_bias_report(
                        cml_engine=self.cml_engine,
                        meteo_engine=self.meteo_engine,
                        start_time=hour_start,
                        end_time=hour_end,
                        out_dir=out_dir,
                    )

            self.logger.debug(
                f"[COMBINED] Completed processing for {hour_start.strftime('%Y-%m-%d %H:%M')}"
            )
            current_time += timedelta(hours=1)

    def process_historical_data(
        self, start_time, end_time, cml_filter_ids=None, meteo_filter_ids=None
    ):
        self.logger.debug(
            f"[COMBINED] Processing historical data: {start_time} to {end_time}"
        )
        self.process_time_range(start_time, end_time, cml_filter_ids, meteo_filter_ids)

    def data_processing_loop(
        self,
        first_run=False,
        start_time=None,
        end_time=None,
        cml_filter_ids=None,
        meteo_filter_ids=None,
    ):
        if first_run:
            self.logger.debug(
                "[COMBINED] First run: Processing data for the last week."
            )

            now = datetime.now(timezone.utc)
            historical_end_time = now.replace(minute=0, second=0, microsecond=0)
            historical_start_time = historical_end_time - timedelta(days=7)

            self.process_historical_data(
                historical_start_time,
                historical_end_time,
                cml_filter_ids,
                meteo_filter_ids,
            )
            self.logger.debug(
                f"[COMBINED] Historical processing completed at {historical_end_time}"
            )

            now_updated = datetime.now(timezone.utc)
            safe_start_time = (now_updated - timedelta(minutes=90)).replace(
                minute=0, second=0, microsecond=0
            )

            if historical_end_time < safe_start_time:
                gap_hours = int(
                    (safe_start_time - historical_end_time).total_seconds() / 3600
                )
                self.logger.debug(
                    f"[COMBINED] Filling gap of {gap_hours} hours from "
                    f"{historical_end_time} to {safe_start_time}"
                )
                self.process_time_range(
                    historical_end_time,
                    safe_start_time,
                    cml_filter_ids,
                    meteo_filter_ids,
                )
                self.logger.debug(f"[COMBINED] Gap filled, now at {safe_start_time}")
            else:
                self.logger.debug(
                    "[COMBINED] No gap to fill, ready for continuous mode"
                )

            self.logger.debug("[COMBINED] Switching to continuous mode.")

        elif start_time and end_time:
            self.logger.debug(
                f"[COMBINED] Processing time range: {start_time} to {end_time}"
            )
            self.process_historical_data(
                start_time, end_time, cml_filter_ids, meteo_filter_ids
            )
            return

        self.logger.debug(
            "[COMBINED] Starting continuous processing (both CML and Meteo)"
        )

        last_processed_time = None

        while True:
            now = datetime.now(timezone.utc)
            if now.hour == 3 and self.map_cleanup.enabled:
                self.logger.debug("[COMBINED] Running daily map cleanup")
                self.map_cleanup.cleanup_all_outputs()

            safe_start_time = (now - timedelta(minutes=90)).replace(
                minute=0, second=0, microsecond=0
            )

            if last_processed_time != safe_start_time:
                self.logger.debug(
                    f"[COMBINED] Processing safe time window: {safe_start_time.strftime('%Y-%m-%d %H:00')}"
                )

                self.process_time_range(
                    safe_start_time,
                    safe_start_time + timedelta(hours=1),
                    cml_filter_ids,
                    meteo_filter_ids,
                )
                last_processed_time = safe_start_time
            else:
                self.logger.info(
                    f"[COMBINED] Window {safe_start_time.strftime('%Y-%m-%d %H:00')} already processed, waiting."
                )

            now = datetime.now(timezone.utc)
            next_run_minute = 40

            if now.minute < next_run_minute:
                next_run = now.replace(minute=next_run_minute, second=0, microsecond=0)
            else:
                next_run = (now + timedelta(hours=1)).replace(
                    minute=next_run_minute, second=0, microsecond=0
                )

            sleep_seconds = (next_run - now).total_seconds()
            self.logger.info(
                f"[COMBINED] Sleeping {sleep_seconds/60:.1f} minutes until {next_run.strftime('%H:%M')} UTC time"
            )

            time.sleep(sleep_seconds)
