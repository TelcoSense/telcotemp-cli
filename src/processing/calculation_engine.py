from datetime import datetime, timedelta, timezone
from src.data_sources.cml.processor import CMLDataSource
from src.data_sources.meteo.processor import MeteoDataSource
from src.processing.ml_modeling import temperature_predict
from src.geo.interpolation import SpatialInterpolator
from src.visualization.map_visualizer import MapVisualizer
from src.storage.influx_writer import InfluxWriter
from src.storage.file_writer import FileWriter
from src.core.config import AppConfig
import threading
import time
import math


class CalculationEngine:
    """Unified calculation engine for both CML and Meteo modes."""

    def __init__(self, config, logger_manager, metadata_provider, geo_components, mode_override=None):
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

        # Unpack geo components
        (
            self.geo_proc,
            self.czech_rep,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
        ) = geo_components

        # Initialize data source based on mode
        if self.mode == "cml":
            self.data_source = CMLDataSource(config, self.logger, metadata_provider)
            self.influx_writer = InfluxWriter(config, self.logger)
        elif self.mode == "meteo":
            self.data_source = MeteoDataSource(config, self.logger, metadata_provider)
            self.influx_writer = None  # Meteo doesn't write to InfluxDB
        else:
            raise ValueError(f"Invalid mode for single engine: {self.mode}")

        # Common components
        self.interpolator = SpatialInterpolator(config, self.logger)
        self.visualizer = MapVisualizer(config, self.logger)
        self.file_writer = FileWriter(config, self.logger)

    def process_time_range(self, start_time, end_time, filter_ids=None):
        """
        Process data for given time range.
        
        :param start_time: Start datetime
        :param end_time: End datetime
        :param filter_ids: Optional list of link_ids (CML) or station_ids (Meteo)
        """
        current_time = start_time

        while current_time < end_time:
            self.logger.info(f"[{self.mode.upper()}] Processing {current_time}")

            try:
                # 1. Fetch data
                df = self.data_source.fetch_data(
                    current_time, current_time + timedelta(hours=1)
                )

                if df.empty:
                    self.logger.warning(f"[{self.mode.upper()}] No data for {current_time}")
                    current_time += timedelta(hours=1)
                    continue

                # 2. Prepare data (metadata, coordinates, filtering)
                df = self.data_source.prepare_data(df, filter_ids)

                if df.empty:
                    self.logger.warning(f"[{self.mode.upper()}] No data after preparation for {current_time}")
                    current_time += timedelta(hours=1)
                    continue

                # 3. ML Prediction (only for CML)
                if self.data_source.supports_ml_prediction():
                    ml_cfg = self.config.get_ml()
                    if ml_cfg:
                        df = temperature_predict(
                            df, ml_cfg["scaler_path"], ml_cfg["lstm_path"]
                        )
                        self.logger.info(f"[{self.mode.upper()}] ML prediction completed")
                    else:
                        self.logger.warning(f"[{self.mode.upper()}] ML config not found, skipping prediction")
                        current_time += timedelta(hours=1)
                        continue

                # 4. Interpolation
                temp_column = self.data_source.get_temperature_column()
                grid_x, grid_y, grid_z = self.interpolator.interpolate(
                    df,
                    self.czech_rep,
                    self.geo_proc,
                    self.elevation_data,
                    self.transform_matrix,
                    self.crs,
                    temp_column=temp_column,
                )

                # 5. Visualization
                image_time = current_time.replace(minute=0, second=0, microsecond=0)
                image_name = f"{self.mode}_{image_time.strftime('%Y-%m-%d_%H%M')}.png"
                self.visualizer.plot(
                    grid_x, grid_y, grid_z, self.czech_rep, image_name
                )

                # 6. Storage
                if self.influx_writer and self.mode == "cml":
                    self.influx_writer.write(df)

                self.file_writer.save_grid(grid_x, grid_y, grid_z, image_name)

                self.logger.info(f"[{self.mode.upper()}] Successfully processed {current_time}")

            except Exception as e:
                self.logger.exception(f"[{self.mode.upper()}] Error processing {current_time}: {e}")

            current_time += timedelta(hours=1)

    def process_historical_data(self, start_time, end_time, filter_ids=None):
        """
        Processes historical data for the given time range.
        Alias for process_time_range for backward compatibility.
        """
        self.logger.info(f"[{self.mode.upper()}] Processing historical data from {start_time} to {end_time}")
        self.process_time_range(start_time, end_time, filter_ids)

    def data_processing_loop(
        self, first_run=False, start_time=None, end_time=None, filter_ids=None
    ):
        """
        Main data processing loop.
        """
        from src.core.initialization import wait_for_next_hour
        
        historical_processed = False

        if first_run:
            self.logger.info(f"[{self.mode.upper()}] First run: Processing data for the last week.")
            historical_end_time = datetime.now().replace(
                minute=0, second=0, microsecond=0
            )
            historical_start_time = historical_end_time - timedelta(days=7)
            self.process_historical_data(
                historical_start_time, historical_end_time, filter_ids
            )
            historical_processed = True
            self.logger.info(f"[{self.mode.upper()}] Switching to regular hourly processing loop.")

        elif start_time and end_time:
            self.logger.info(f"[{self.mode.upper()}] Processing time range: {start_time} to {end_time}")
            self.process_historical_data(start_time, end_time, filter_ids)
            return

        # Continuous loop
        self.logger.info(f"[{self.mode.upper()}] Starting continuous processing")
        
        # Keep track of the last processed hour to avoid duplicates
        last_processed_time = None

        while True:
            now = datetime.now(timezone.utc)
            
            # Determine safe processing time based on mode latency
            # Meteo data is ready at HH:30 for the previous hour (latency ~90 mins safe margin)
            # CML data is usually faster, but we align to Meteo in combined or if strictly Meteo
            if self.mode == "meteo":
                # Safe time is now - 90 minutes, floored to hour
                safe_processing_time = (now - timedelta(minutes=90)).replace(
                    minute=0, second=0, microsecond=0
                )
            else:
                # CML can be processed sooner, e.g., 10 mins after hour
                safe_processing_time = (now - timedelta(minutes=10)).replace(
                    minute=0, second=0, microsecond=0
                )

            # If we haven't processed this time yet, do it now
            if last_processed_time != safe_processing_time:
                self.logger.info(f"[{self.mode.upper()}] Processing safe time window starting: {safe_processing_time}")
                
                self.process_time_range(
                    safe_processing_time, 
                    safe_processing_time + timedelta(hours=1), 
                    filter_ids
                )
                last_processed_time = safe_processing_time
            else:
                self.logger.info(f"[{self.mode.upper()}] Time {safe_processing_time} already processed.")

            # Calculate sleep time until next XX:40
            # If we are currently at XX:15, we wait until XX:40
            # If we are at XX:45, we wait until (XX+1):40
            now = datetime.now(timezone.utc)
            next_run_minute = 40
            
            if now.minute < next_run_minute:
                next_run = now.replace(minute=next_run_minute, second=0, microsecond=0)
            else:
                next_run = (now + timedelta(hours=1)).replace(
                    minute=next_run_minute, second=0, microsecond=0
                )
            
            sleep_seconds = (next_run - now).total_seconds()
            self.logger.info(f"[{self.mode.upper()}] Sleeping {sleep_seconds/60:.1f} minutes until {next_run.strftime('%H:%M')}")
            time.sleep(sleep_seconds)


class CombinedCalculationEngine:
    """Combined calculation engine that runs both CML and Meteo modes in parallel."""
    
    def __init__(self, config, logger_manager, cml_metadata_provider, meteo_metadata_provider, geo_components):
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
        
        # Create separate configs for each mode
        cml_config = AppConfig(config_dir=config.config_dir, mode="cml")
        meteo_config = AppConfig(config_dir=config.config_dir, mode="meteo")
        
        # Create separate engines
        self.cml_engine = CalculationEngine(
            cml_config, logger_manager, cml_metadata_provider, geo_components, mode_override="cml"
        )
        self.meteo_engine = CalculationEngine(
            meteo_config, logger_manager, meteo_metadata_provider, geo_components, mode_override="meteo"
        )
        
        self.logger.info("Combined calculation engine initialized (CML + Meteo)")

    def _get_meteo_available_time(self, target_time):
        """
        Calculate when Meteo data for target_time becomes available.
        Meteo data is written at HH:30 for the previous hour (HH-1:00 to HH:00).
        
        :param target_time: Target hour to process
        :return: Time when data becomes available
        """
        # Data for hour HH:00-HH:00 is available at (HH+1):30
        available_at = target_time + timedelta(hours=1, minutes=30)
        return available_at

    def _wait_for_meteo_data(self, target_time):
        """
        Wait until Meteo data for target_time is available.
        
        :param target_time: Target hour to process
        """
        available_at = self._get_meteo_available_time(target_time)
        now = datetime.now().replace(tzinfo=target_time.tzinfo)
        
        if now < available_at:
            wait_seconds = (available_at - now).total_seconds()
            self.logger.info(
                f"[COMBINED] Waiting {wait_seconds/60:.1f} minutes for Meteo data "
                f"(available at {available_at.strftime('%H:%M')})"
            )
            time.sleep(wait_seconds)

    def process_time_range(self, start_time, end_time, cml_filter_ids=None, meteo_filter_ids=None):
        """
        Process data for given time range in both modes (parallel).
        For each hour, waits for Meteo data availability before processing.
        
        :param start_time: Start datetime
        :param end_time: End datetime
        :param cml_filter_ids: Optional list of link_ids for CML
        :param meteo_filter_ids: Optional list of station_ids for Meteo
        """
        self.logger.info(f"[COMBINED] Processing time range: {start_time} to {end_time}")
        
        current_time = start_time
        
        while current_time < end_time:
            hour_start = current_time
            hour_end = current_time + timedelta(hours=1)
            
            self.logger.info(f"[COMBINED] Processing hour: {hour_start.strftime('%Y-%m-%d %H:%M')}")
            
            # Check if Meteo data is available yet
            available_at = self._get_meteo_available_time(hour_start)
            now = datetime.now().replace(tzinfo=hour_start.tzinfo)
            
            if now < available_at:
                self.logger.warning(
                    f"[COMBINED] Meteo data not yet available (available at {available_at.strftime('%H:%M')}). "
                    f"Processing CML only for now."
                )
                # Process only CML
                self.cml_engine.process_time_range(hour_start, hour_end, cml_filter_ids)
            else:
                # Both data sources available - process in parallel
                cml_thread = threading.Thread(
                    target=self.cml_engine.process_time_range,
                    args=(hour_start, hour_end, cml_filter_ids),
                    name="CML-Thread"
                )
                meteo_thread = threading.Thread(
                    target=self.meteo_engine.process_time_range,
                    args=(hour_start, hour_end, meteo_filter_ids),
                    name="Meteo-Thread"
                )
                
                # Start both threads
                cml_thread.start()
                meteo_thread.start()
                
                # Wait for completion
                cml_thread.join()
                meteo_thread.join()
            
            self.logger.info(f"[COMBINED] Completed processing for {hour_start.strftime('%Y-%m-%d %H:%M')}")
            
            current_time += timedelta(hours=1)

    def process_historical_data(self, start_time, end_time, cml_filter_ids=None, meteo_filter_ids=None):
        """
        Process historical data in both modes (parallel).
        No waiting needed as historical data is already available.
        """
        self.logger.info(f"[COMBINED] Processing historical data: {start_time} to {end_time}")
        self.process_time_range(start_time, end_time, cml_filter_ids, meteo_filter_ids)

    def data_processing_loop(
        self, first_run=False, start_time=None, end_time=None, 
        cml_filter_ids=None, meteo_filter_ids=None
    ):
        """
        Main data processing loop for combined mode.
        Handles Meteo data availability timing automatically.
        
        :param first_run: If True, process last week first
        :param start_time: Optional custom start time
        :param end_time: Optional custom end time
        :param cml_filter_ids: Optional filter for CML links
        :param meteo_filter_ids: Optional filter for Meteo stations
        """
        historical_processed = False
        
        if first_run:
            self.logger.info("[COMBINED] First run: Processing data for the last week.")
            historical_end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            historical_start_time = historical_end_time - timedelta(days=7)
            self.process_historical_data(
                historical_start_time, historical_end_time, 
                cml_filter_ids, meteo_filter_ids
            )
            historical_processed = True
            self.logger.info("[COMBINED] Switching to regular hourly processing loop.")
        
        elif start_time and end_time:
            self.logger.info(f"[COMBINED] Processing time range: {start_time} to {end_time}")
            self.process_historical_data(
                start_time, end_time, cml_filter_ids, meteo_filter_ids
            )
            return
        
        # Continuous loop
        self.logger.info("[COMBINED] Starting continuous processing (both CML and Meteo)")
        
        last_processed_time = None

        while True:
            now = datetime.now(timezone.utc)
            
            # LOGIC: Meteo data for hour H (H:00-H+1:00) is written at H+1:30.
            # To be safe, we can process hour H only when current time is > H+1:30.
            # Formula: Safe Start Time = Floor_Hour(Current_Time - 90 minutes)
            # Example 12:15 -> 10:45 -> 10:00 (Data 10-11 ready at 11:30)
            # Example 12:40 -> 11:10 -> 11:00 (Data 11-12 ready at 12:30)
            
            safe_start_time = (now - timedelta(minutes=90)).replace(
                minute=0, second=0, microsecond=0
            )
            
            if last_processed_time != safe_start_time:
                self.logger.info(
                    f"[COMBINED] Current time: {now.strftime('%H:%M')}. "
                    f"Processing last complete safe window: {safe_start_time.strftime('%Y-%m-%d %H:00')}"
                )
                
                # Process the hour immediately
                self.process_time_range(
                    safe_start_time, 
                    safe_start_time + timedelta(hours=1), 
                    cml_filter_ids, 
                    meteo_filter_ids
                )
                last_processed_time = safe_start_time
            else:
                self.logger.info(f"[COMBINED] Window {safe_start_time} already processed.")

            # Schedule next run for XX:40
            # This ensures we are always past the XX:30 mark when Meteo data is written
            now = datetime.now(timezone.utc)
            next_run_minute = 40
            
            if now.minute < next_run_minute:
                next_run = now.replace(minute=next_run_minute, second=0, microsecond=0)
            else:
                next_run = (now + timedelta(hours=1)).replace(
                    minute=next_run_minute, second=0, microsecond=0
                )
            
            sleep_seconds = (next_run - now).total_seconds()
            self.logger.info(f"[COMBINED] Sleeping {sleep_seconds/60:.1f} minutes until {next_run.strftime('%H:%M')}")
            time.sleep(sleep_seconds)
