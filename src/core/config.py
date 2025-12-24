import configparser
import os


class AppConfig:
    """Unified configuration manager for both CML and Meteo modes."""
    
    def __init__(self, config_dir="configs", mode="combined", config_file="config.ini"):
        """
        Args:
            config_dir: Path to config directory
            mode: "cml", "meteo", or "combined" - determines data source
            config_file: Name of the config file (default: config.ini)
        """
        self.config_dir = config_dir
        self.mode = mode
        self.config = self._load(config_file)

    def _load(self, filename):
        cfg = configparser.ConfigParser()
        path = os.path.join(self.config_dir, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file {path} does not exist.")
        cfg.read(path, encoding="utf-8")
        return cfg

    # --- APP ---
    def get_logging_config(self):
        lg = self.config["logging"]
        return {
            "level": lg.get("level", "INFO"),
            "backend_log": lg.get("backend_log", "app.log"),
            "max_bytes": lg.getint("max_bytes", 10 * 1024 * 1024),
            "backups": lg.getint("backups", 1),
            "fmt": lg.get(
                "fmt",
                raw=True,
                fallback="%(asctime)s -%(funcName)s - %(levelname)s - %(message)s",
            ),
        }

    def get_paths(self):
        p = self.config["paths"]
        paths = {
            "country_file": p.get("country_file"),
            "dem_tif": p.get("dem_tif"),
            "images_dir": p.get("images_dir", "outputs_web"),
            "saved_grids_dir": p.get("saved_grids_dir", "saved_grids"),
            "color_scale_dir": p.get("color_scale_dir", "configs"),
        }
        return paths

    def get_visualization(self):
        vis = self.config["visualization"]
        colormap_str = vis.get("colormap", "[]")
        import ast
        try:
            colormap = ast.literal_eval(colormap_str) if colormap_str.strip() else []
        except:
            colormap = []
        
        return {
            "n_levels": vis.getint("n_levels", 15),
            "colormap": colormap,
            "median_offset": vis.getint("median_offset", 2),
            "scale_mode": vis.get("scale_mode", "dynamic"),
        }

    def get_ml(self):
        """Returns ML config - only for CML mode."""
        if self.mode != "cml":
            return None
        
        if "ml" not in self.config:
            return None
            
        ml = self.config["ml"]
        return {
            "lstm_path": ml.get("lstm_path"),
            "scaler_path": ml.get("scaler_path"),
        }

    # --- DATABASE / MYSQL ---
    def get_database_credentials(self):
        """For CML mode compatibility."""
        mysql = self.config["mysql"]
        return {
            "user": mysql.get("user"),
            "password": mysql.get("password"),
            "host": mysql.get("host"),
            "port": mysql.getint("port", 3306),
        }

    def get_mysql_url(self):
        """For CML mode compatibility."""
        creds = self.get_database_credentials()
        return f"mysql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}"

    def get_mysql_config(self):
        """For Meteo mode compatibility."""
        return self.get_database_credentials()

    # --- DEBUG ---
    def is_write_enabled(self):
        """
        Check if database writes are enabled.
        Returns False if either global debug.disable_all_writes is True
        or influx_write.enable_write is False.
        """
        # Check global debug setting
        if "debug" in self.config:
            if self.config["debug"].getboolean("disable_all_writes", False):
                return False
        
        # Check specific influx_write setting (for CML mode)
        if self.mode == "cml" and "influx_write" in self.config:
            if not self.config["influx_write"].getboolean("enable_write", True):
                return False
        
        return True
    
    def is_grid_saving_enabled(self):
        """
        Check if saving intermediate grids is enabled.
        Returns False by default.
        """
        if "debug" in self.config:
            return self.config["debug"].getboolean("save_grids", False)
        return False

    # --- INFLUX ---
    def get_influx_config(self, operation="read"):
        """
        Unified InfluxDB config.
        Args:
            operation: "read" or "write" (only for CML mode)
        """
        if self.mode == "cml":
            return self._get_cml_influx_config(operation)
        else:
            return self._get_meteo_influx_config()

    def _get_cml_influx_config(self, operation):
        """CML-specific InfluxDB config with read/write buckets."""
        if operation not in ("read", "write"):
            raise ValueError("operation must be 'read' or 'write'")

        # Check if influx_common section exists
        common = self.config["influx_common"] if "influx_common" in self.config else None
        section_key = f"influx_{operation}"
        
        if section_key not in self.config:
            raise KeyError(f"Missing [{section_key}] section in config.ini")
            
        section = self.config[section_key]

        cfg = {
            "org": common.get("org") if common else section.get("org"),
            "url": common.get("url") if common else section.get("url"),
            "token": common.get("token") if common else section.get("token"),
        }

        if operation == "read":
            cfg.update({
                "bucket": section.get("bucket", "realtime_cbl"),
                "measurements": [
                    s.strip()
                    for s in section.get("measurements", "").split(",")
                    if s.strip()
                ],
                "fields": [
                    s.strip()
                    for s in section.get("fields", "").split(",")
                    if s.strip()
                ],
                "tag_device": section.get("tag_device", "agent_host"),
                "field_temperature": section.get("field_temperature", "Teplota"),
                "field_signal": section.get("field_signal", "PrijimanaUroven"),
                "window": section.get("window", "10m"),
                "range": section.get("range", "-1h"),
            })
        else:  # write
            cfg.update({
                "bucket": section.get("bucket", "telcotemp_output"),
                "measurement": section.get("measurement", "telcorain"),
                "tag_cml_id": section.get("tag_cml_id", "cml_id"),
                "tag_side": section.get("tag_side", "side"),
                "field_temperature": section.get("field_temperature", "temperature"),
                "enable_write": section.getboolean("enable_write", True),
            })
        
        return cfg

    def _get_meteo_influx_config(self):
        """Meteo-specific InfluxDB config (read-only)."""
        if "influx" not in self.config:
            raise KeyError("Missing [influx] section in config.ini")
            
        influx = self.config["influx"]
        return {
            "org": influx.get("org"),
            "url": influx.get("url"),
            "token": influx.get("token"),
            "bucket": influx.get("bucket", "chmi_data"),
            "measurements": [
                s.strip()
                for s in influx.get("measurements", "T").split(",")
                if s.strip()
            ],
            "fields": [
                s.strip()
                for s in influx.get("fields", "Temperature").split(",")
                if s.strip()
            ],
            "tag_device": influx.get("tag_device", "agent_host"),
            "field_temperature": influx.get("field_temperature", "Temperature"),
            "window": influx.get("window", "10m"),
            "range": influx.get("range", "-2h"),
        }

    # --- COMPUTE ---
    def get_grid_config(self):
        grid = self.config["grid"]
        return {
            "x_points": grid.getint("x_points", 500),
            "y_points": grid.getint("y_points", 500),
            "mask_resolution_safe": grid.getboolean("mask_resolution_safe", True),
        }

    def get_interpolation_config(self):
        itp = self.config["interpolation"]
        return {
            "variogram_model": itp.get("variogram_model", "spherical"),
            "nlags": itp.getint("nlags", 40),
            "regression_model": itp.get("regression_model", "linear"),
        }

    def get_location(self):
        loc = self.config["location"]
        return {
            "lat": loc.getfloat("lat", 49.8175),
            "lng": loc.getfloat("lng", 15.4730),
            "tz": loc.get("tz", "Europe/Prague"),
        }
