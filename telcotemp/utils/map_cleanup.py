import os
import re
from datetime import datetime, timedelta
from pathlib import Path


class MapCleanup:
    """Utility class to clean up old map files based on timestamp in filename."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.cleanup_config = config.get_cleanup_config()
        self.enabled = self.cleanup_config.get("enabled", False)
        self.retention_days = self.cleanup_config.get("retention_days", 30)
        
        # Pattern to extract timestamp from filename: 2025-12-21_0000_-50_50.png
        self.filename_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2}_\d{4})_.*\.png$')

    def parse_timestamp_from_filename(self, filename):
        """
        Extract datetime from filename.
        
        :param filename: Name of the file
        :return: datetime object or None if parsing fails
        """
        match = self.filename_pattern.match(filename)
        if not match:
            return None
        
        timestamp_str = match.group(1)
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d_%H%M")
        except ValueError:
            self.logger.warning(f"Failed to parse timestamp from filename: {filename}")
            return None

    def cleanup_directory(self, directory_path):
        """
        Remove files older than retention_days from specified directory.
        
        :param directory_path: Path to directory to clean
        """
        if not self.enabled:
            self.logger.debug("Map cleanup is disabled in config")
            return

        if not os.path.exists(directory_path):
            self.logger.warning(f"Directory does not exist: {directory_path}")
            return

        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        removed_count = 0
        skipped_count = 0

        self.logger.info(
            f"Cleaning directory: {directory_path} "
            f"(removing files older than {self.retention_days} days)"
        )

        for filename in os.listdir(directory_path):
            if not filename.endswith('.png'):
                continue

            file_path = os.path.join(directory_path, filename)
            
            # Parse timestamp from filename
            file_timestamp = self.parse_timestamp_from_filename(filename)
            
            if file_timestamp is None:
                skipped_count += 1
                self.logger.debug(f"Skipping file with unparseable name: {filename}")
                continue

            # Check if file is older than cutoff date
            if file_timestamp < cutoff_date:
                try:
                    os.remove(file_path)
                    removed_count += 1
                    self.logger.debug(f"Removed old map: {filename} (date: {file_timestamp})")
                except Exception as e:
                    self.logger.error(f"Error removing file {filename}: {e}")
            else:
                self.logger.debug(f"Keeping map: {filename} (date: {file_timestamp})")

        self.logger.info(
            f"Cleanup completed for {directory_path}: "
            f"removed {removed_count} files, skipped {skipped_count} files"
        )

    def cleanup_all_outputs(self):
        """Clean up all configured output directories."""
        if not self.enabled:
            self.logger.info("Map cleanup is disabled")
            return

        paths = self.config.get_paths()
        
        # Clean CML output directory
        cml_dir = paths.get("cml_dir")
        if cml_dir:
            self.cleanup_directory(cml_dir)
        
        # Clean Meteo output directory
        meteo_dir = paths.get("meteo_dir")
        if meteo_dir:
            self.cleanup_directory(meteo_dir)

        self.logger.info("Map cleanup finished")