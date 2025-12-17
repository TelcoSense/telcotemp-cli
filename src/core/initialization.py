
from sqlalchemy import create_engine
from src.data_sources.cml.sql_metadata import CMLMetadataProvider
from src.data_sources.meteo.sql_metadata import MeteoMetadataProvider
from src.geo.geographical_processing import GeographicalProcessing
import datetime
import time


def wait_for_next_hour():
    """
    Waits until the start of the next hour.
    Useful for scheduling periodic tasks (Meteo mode).
    """
    current = datetime.datetime.now()
    next_hour = (current + datetime.timedelta(hours=1)).replace(
        minute=0, second=0, microsecond=0
    )
    wait_seconds = (next_hour - current).total_seconds()
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def initialize_database(config, logger):
    """Initialize database connection and metadata provider(s) based on mode."""
    db_config = config.get_mysql_config()
    engine = create_engine(
        f"mysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}"
    )
    
    if config.mode == "cml":
        return engine, CMLMetadataProvider(engine, logger)
    elif config.mode == "meteo":
        return engine, MeteoMetadataProvider(engine, logger)
    elif config.mode == "combined":
        # Return both metadata providers
        cml_provider = CMLMetadataProvider(engine, logger)
        meteo_provider = MeteoMetadataProvider(engine, logger)
        return engine, (cml_provider, meteo_provider)
    else:
        raise ValueError(f"Unknown mode: {config.mode}")


def initialize_geographical_processing(config):
    """Initialize geographical processing and load country/elevation data."""
    geo_proc = GeographicalProcessing()
    paths = config.get_paths()
    
    state = geo_proc.load_country_data(paths["country_file"])
    czech_rep = geo_proc.json_to_geodataframe(state)
    
    # Pro všechny režimy konvertuj do EPSG:3857
    czech_rep = czech_rep.to_crs("EPSG:3857")
    
    elevation_data, transform_matrix, crs = geo_proc.load_elevation_data(
        paths["dem_tif"]
    )
    
    return geo_proc, czech_rep, elevation_data, transform_matrix, crs
