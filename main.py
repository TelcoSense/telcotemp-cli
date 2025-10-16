from data_processing.calculation_engine import CalculationEngine
from config import AppConfig
from log import LoggerManager
import datetime
import argparse

config = AppConfig()
log_config = config.get_logging_config()
logger_manager = LoggerManager(config)
backend_logger = logger_manager.get_logger("backend_logger")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data processing.")
    parser.add_argument(
        "--first_run",
        action="store_true",
        help="Process hourly maps for the last week.",
    )
    parser.add_argument(
        "--start_time", type=str, help="Start time in format YYYY-MM-DD HH:MM"
    )
    parser.add_argument(
        "--end_time", type=str, help="End time in format YYYY-MM-DD HH:MM"
    )
    parser.add_argument(
        "--link_ids",
        type=str,
        help="Comma-separated list of Link_IDs to include in processing.",
    )
    args = parser.parse_args()

    if args.link_ids and (not args.start_time or not args.end_time):
        parser.error(
            "--link_ids can only be used when both --start_time and --end_time are specified."
        )

    start_time = (
        datetime.datetime.strptime(args.start_time, "%Y-%m-%d %H:%M")
        if args.start_time
        else None
    )
    end_time = (
        datetime.datetime.strptime(args.end_time, "%Y-%m-%d %H:%M")
        if args.end_time
        else None
    )
    link_ids = (
        [int(link_id.strip()) for link_id in args.link_ids.split(",")]
        if args.link_ids
        else None
    )

    backend_logger.info("Backend processing started")
    processor = CalculationEngine(config, logger_manager)
    processor.data_processing_loop(
        first_run=args.first_run,
        start_time=start_time,
        end_time=end_time,
        link_ids=link_ids,
    )
