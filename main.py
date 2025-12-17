"""
TelcoTemp Unified - Temperature Mapping System
Supports both CML (microwave links), Meteo (weather stations), and Combined modes
"""

from src.core.config import AppConfig
from src.core.log import LoggerManager
from src.core.initialization import initialize_database, initialize_geographical_processing
from src.processing.calculation_engine import CalculationEngine, CombinedCalculationEngine
import argparse
import datetime
import sys


class Application:
    """Unified CLI application for CML, Meteo, and Combined modes."""

    def __init__(self, mode="combined"):
        """
        Initialize application.
        
        :param mode: "cml", "meteo", or "combined"
        """
        try:
            self.config = AppConfig(mode=mode)
            self.logger_manager = LoggerManager(self.config)
            self.logger = self.logger_manager.get_logger("backend_logger")
            
            self.logger.info(f"Initializing TelcoTemp in {mode.upper()} mode")
            
            # Initialize database and metadata provider(s)
            self.engine, self.metadata_provider = initialize_database(
                self.config, self.logger
            )
            
            # Initialize geographical processing
            self.geo_components = initialize_geographical_processing(self.config)
            
            # Initialize calculation engine(s)
            if mode == "combined":
                cml_provider, meteo_provider = self.metadata_provider
                self.calculation_engine = CombinedCalculationEngine(
                    self.config,
                    self.logger_manager,
                    cml_provider,
                    meteo_provider,
                    self.geo_components,
                )
            else:
                self.calculation_engine = CalculationEngine(
                    self.config,
                    self.logger_manager,
                    self.metadata_provider,
                    self.geo_components,
                )
            
            self.logger.info("Application initialized successfully")
            
        except Exception as e:
            print(f"Error initializing application: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    def parse_arguments(self):
        """Parse command line arguments."""
        parser = argparse.ArgumentParser(
            description="TelcoTemp - Unified temperature mapping from CML, weather stations, or both",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Combined mode (default) - continuous processing
  python main.py
  
  # Combined mode - process last week
  python main.py --first_run
  
  # Combined mode - specific time range
  python main.py --start_time "2024-01-01 00:00" --end_time "2024-01-02 00:00"
  
  # CML mode only
  python main.py --mode cml --start_time "2024-01-01 00:00" --end_time "2024-01-02 00:00"
  
  # Meteo mode only
  python main.py --mode meteo --start_time "2024-01-01 00:00" --end_time "2024-01-02 00:00"
  
  # Combined mode - filter both sources
  python main.py --cml_filter_ids "123,456" --meteo_filter_ids "LKCB,LKKV"
            """,
        )
        
        parser.add_argument(
            "--mode",
            choices=["cml", "meteo", "combined"],
            default="combined",
            help="Data source mode: cml, meteo, or combined (default: combined)",
        )
        parser.add_argument(
            "--first_run",
            action="store_true",
            help="Process hourly maps for the last week",
        )
        parser.add_argument(
            "--start_time",
            type=str,
            help="Start time in format YYYY-MM-DD HH:MM",
        )
        parser.add_argument(
            "--end_time",
            type=str,
            help="End time in format YYYY-MM-DD HH:MM",
        )
        parser.add_argument(
            "--filter_ids",
            type=str,
            help="Comma-separated list of Link IDs (CML mode) or Station IDs (Meteo mode)",
        )
        parser.add_argument(
            "--cml_filter_ids",
            type=str,
            help="Comma-separated list of Link IDs (Combined mode)",
        )
        parser.add_argument(
            "--meteo_filter_ids",
            type=str,
            help="Comma-separated list of Station IDs (Combined mode)",
        )
        parser.add_argument(
            "--continuous",
            action="store_true",
            help="Run in continuous mode (process every hour)",
        )
        
        return parser.parse_args()

    def run(self):
        """Run the application."""
        args = self.parse_arguments()

        # Update mode if specified and different
        if args.mode != self.config.mode:
            self.logger.info(f"Switching to {args.mode.upper()} mode")
            self.__init__(mode=args.mode)

        self.logger.info(f"Starting in {args.mode.upper()} mode")

        # Handle filter IDs based on mode
        if args.mode == "combined":
            cml_filter_ids = None
            meteo_filter_ids = None
            
            if args.cml_filter_ids:
                cml_filter_ids = [fid.strip() for fid in args.cml_filter_ids.split(",")]
                self.logger.info(f"CML filter: {cml_filter_ids}")
            
            if args.meteo_filter_ids:
                meteo_filter_ids = [fid.strip() for fid in args.meteo_filter_ids.split(",")]
                self.logger.info(f"Meteo filter: {meteo_filter_ids}")

            # Process based on arguments
            if args.first_run:
                # Process last week
                end_time = datetime.datetime.now(datetime.timezone.utc).replace(
                    minute=0, second=0, microsecond=0
                )
                start_time = end_time - datetime.timedelta(days=7)
                self.logger.info(f"Processing last week: {start_time} to {end_time}")
                self.calculation_engine.process_time_range(
                    start_time, end_time, cml_filter_ids, meteo_filter_ids
                )

            elif args.start_time and args.end_time:
                # Process specific time range
                try:
                    start_time = datetime.datetime.strptime(
                        args.start_time, "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=datetime.timezone.utc)
                    end_time = datetime.datetime.strptime(
                        args.end_time, "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=datetime.timezone.utc)

                    self.logger.info(f"Processing time range: {start_time} to {end_time}")
                    self.calculation_engine.process_time_range(
                        start_time, end_time, cml_filter_ids, meteo_filter_ids
                    )
                except ValueError as e:
                    self.logger.error(f"Invalid time format: {e}")
                    self.logger.error("Use format: YYYY-MM-DD HH:MM")
                    sys.exit(1)
            else:
                # Default: Continuous processing
                self.logger.info("Starting continuous processing (default mode)")
                self.calculation_engine.data_processing_loop(
                    first_run=False,
                    cml_filter_ids=cml_filter_ids,
                    meteo_filter_ids=meteo_filter_ids
                )

        else:
            # Single mode (CML or Meteo)
            filter_ids = None
            if args.filter_ids:
                filter_ids = [fid.strip() for fid in args.filter_ids.split(",")]
                id_type = "link_ids" if args.mode == "cml" else "station_ids"
                self.logger.info(f"Filtering by {id_type}: {filter_ids}")

            # Determine execution mode
            if args.continuous:
                # Continuous processing loop
                self.logger.info("Starting continuous processing loop")
                self.calculation_engine.data_processing_loop(
                    first_run=args.first_run, filter_ids=filter_ids
                )
                
            elif args.first_run:
                # Process last week
                end_time = datetime.datetime.now(datetime.timezone.utc).replace(
                    minute=0, second=0, microsecond=0
                )
                start_time = end_time - datetime.timedelta(days=7)
                self.logger.info(f"Processing last week: {start_time} to {end_time}")
                self.calculation_engine.process_time_range(start_time, end_time, filter_ids)
                
            elif args.start_time and args.end_time:
                # Process specific time range
                try:
                    start_time = datetime.datetime.strptime(
                        args.start_time, "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=datetime.timezone.utc)
                    end_time = datetime.datetime.strptime(
                        args.end_time, "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=datetime.timezone.utc)
                    
                    self.logger.info(f"Processing time range: {start_time} to {end_time}")
                    self.calculation_engine.process_time_range(
                        start_time, end_time, filter_ids
                    )
                except ValueError as e:
                    self.logger.error(f"Invalid time format: {e}")
                    self.logger.error("Use format: YYYY-MM-DD HH:MM")
                    sys.exit(1)
                    
            else:
                self.logger.error(
                    "Specify --first_run, --continuous, or both --start_time and --end_time"
                )
                sys.exit(1)

        self.logger.info("Processing completed")


def main():
    """Main entry point."""
    app = Application()
    app.run()


if __name__ == "__main__":
    main()
