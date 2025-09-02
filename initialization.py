from sqlalchemy import create_engine
from database_operations.sql_manager import DatabaseOperations
from spatial_processing.geographical_processing import GeographicalProcessing
from datetime import datetime, timedelta
from time import sleep



def wait_for_next_hour():
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    sleep((next_hour - now).seconds)

def initialize_app(config):
    db_config = config.get_database_credentials()
    paths = config.get_paths()

    engine = create_engine(
        f"mysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}"
    )
    db_ops = DatabaseOperations(engine)
    geo_proc = GeographicalProcessing()
    state = geo_proc.load_country_data(paths["country_file"])
    czech_rep = geo_proc.json_to_geodataframe(state)
    elevation_data, transform_matrix, crs = geo_proc.load_elevation_data(paths["dem_tif"])
    return db_ops, geo_proc, czech_rep, elevation_data, transform_matrix, crs

