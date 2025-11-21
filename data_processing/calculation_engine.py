import datetime
from data_processing.data_processing import DataProcessor
from initialization import EnvironmentInitializer
from utils.time_utils import wait_for_next_hour


class CalculationEngine:
    def __init__(self, config, logger_manager):
        self.config = config
        self.logger_manager = logger_manager
        self.logger = logger_manager.get_logger("backend_logger")

        initializer = EnvironmentInitializer(config, self.logger)
        (
            self.influx_handler,
            self.db_ops,
            self.geo_proc,
            self.czech_rep,
            self.elevation_data,
            self.transform_matrix,
            self.crs,
            self.map_visualizer,
            self.interpolator,
        ) = initializer.initialize_all()

        self.data_processor = DataProcessor(
            config=self.config,
            db_ops=self.db_ops,
            geo_proc=self.geo_proc,
            czech_rep=self.czech_rep,
            elevation_data=self.elevation_data,
            transform_matrix=self.transform_matrix,
            crs=self.crs,
            logger=self.logger,
            influx_handler=self.influx_handler,
        )

    def process_historical_data(self, start_time, end_time, link_ids=None):
        """
        Processes historical data for the given time range.
        """
        self.logger.info(f"Processing historical data from {start_time} to {end_time}")
        self.data_processor.process_time_range(start_time, end_time, link_ids)

    def data_processing_loop(
        self, first_run=False, start_time=None, end_time=None, link_ids=None
    ):
        """
        Main data processing loop.
        """
        historical_processed = False

        if first_run:
            self.logger.info("First run: Processing data for the last week.")
            historical_end_time = datetime.datetime.now().replace(
                minute=0, second=0, microsecond=0
            )
            historical_start_time = historical_end_time - datetime.timedelta(days=7)
            self.process_historical_data(
                historical_start_time, historical_end_time, link_ids
            )
            historical_processed = True
            self.logger.info("Switching to regular hourly processing loop.")

        elif start_time and end_time:
            self.logger.info(
                f"Processing historical data from {start_time} to {end_time}"
            )
            self.process_historical_data(start_time, end_time, link_ids)
            return

        while True:
            now = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
            if historical_processed:
                self.logger.info(
                    f"Catching up on missed hours from {historical_end_time} to {now}"
                )
                self.process_historical_data(historical_end_time, now)
                historical_processed = False

            self.data_processor.process_time_range(
                now - datetime.timedelta(hours=1), now,
            )
            wait_for_next_hour()
