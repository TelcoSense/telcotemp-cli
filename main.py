from initialization import (
    initialize_app,
    wait_for_next_hour,
)
from data_processing.data_processing import process_data_round
from config import AppConfig
from log import setup_logger


config = AppConfig()
log_config = config.get_logging_config()
backend_logger = setup_logger('backend_logger', log_config.get("backend_log"), level=log_config.get("level"))


def data_processing_loop():
    db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs = initialize_app(config)
    while True:
        process_data_round(config, db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs)
        wait_for_next_hour()


if __name__ == "__main__":
    backend_logger.info("Backend processing started")
    data_processing_loop()
