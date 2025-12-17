from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime


class DataSource(ABC):
    """Abstract base class for data sources."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    @abstractmethod
    def fetch_data(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Fetch raw data from source."""
        pass

    @abstractmethod
    def prepare_data(self, df: pd.DataFrame, filter_ids=None) -> pd.DataFrame:
        """Prepare data for interpolation (add metadata, transform coordinates, etc.)."""
        pass

    @abstractmethod
    def supports_ml_prediction(self) -> bool:
        """Returns True if this data source supports ML temperature prediction."""
        pass

    @abstractmethod
    def get_temperature_column(self) -> str:
        """Returns the name of the temperature column after processing."""
        pass


class MetadataProvider(ABC):
    """Abstract base for SQL metadata providers."""

    @abstractmethod
    def fetch_metadata(self, identifiers: list) -> dict:
        """Fetch metadata for given device/station IDs."""
        pass


class InfluxReader(ABC):
    """Abstract base for InfluxDB readers."""

    @abstractmethod
    def read(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Read data from InfluxDB."""
        pass
