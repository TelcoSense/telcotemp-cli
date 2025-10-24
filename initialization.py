from sqlalchemy import create_engine
from database_operations.sql_manager import DatabaseOperations
from spatial_processing.geographical_processing import GeographicalProcessing
from spatial_processing.visualization import MapVisualizer
from interpolation.interpolation import SpatialInterpolator
from database_operations.influx_manager import InfluxDBHandler


class EnvironmentInitializer:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def initialize_database(self):
        db_config = self.config.get_database_credentials()
        engine = create_engine(
            f"mysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}"
        )
        return DatabaseOperations(engine, self.logger)

    def initialize_influx_handler(self):
        return InfluxDBHandler(self.config, self.logger)

    def initialize_geographical_processing(self):
        geo_proc = GeographicalProcessing()
        paths = self.config.get_paths()
        state = geo_proc.load_country_data(paths["country_file"])
        czech_rep = geo_proc.json_to_geodataframe(state)
        elevation_data, transform_matrix, crs = geo_proc.load_elevation_data(
            paths["dem_tif"]
        )
        return geo_proc, czech_rep, elevation_data, transform_matrix, crs

    def initialize_visualization(self):
        return MapVisualizer(self.config, self.logger)

    def initialize_interpolator(self):
        return SpatialInterpolator(self.config, self.logger)

    def initialize_all(self):
        db_ops = self.initialize_database()
        influx_handler = self.initialize_influx_handler()
        geo_proc, czech_rep, elevation_data, transform_matrix, crs = (
            self.initialize_geographical_processing()
        )
        map_visualizer = self.initialize_visualization()
        interpolator = self.initialize_interpolator()

        return (
            influx_handler,
            db_ops,
            geo_proc,
            czech_rep,
            elevation_data,
            transform_matrix,
            crs,
            map_visualizer,
            interpolator,
        )
