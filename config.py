import os
import ast
import configparser


class AppConfig:
    def __init__(self, config_dir="configs"):
        self.config_dir = config_dir
        self.app = self._load("app.ini.dist")
        self.database = self._load("database.ini.dist")
        self.compute = self._load("compute.ini.dist")

    def _load(self, filename):
        cfg = configparser.ConfigParser()
        path = os.path.join(self.config_dir, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Konfigurační soubor {path} neexistuje.")
        cfg.read(path, encoding="utf-8")
        return cfg

    # --- APP ---
    def get_logging_config(self):
        lg = self.app["logging"]
        return {
            "level": lg.get("level", "INFO"),
            "backend_log": lg.get("backend_log", "app.log"),
            "max_bytes": lg.getint("max_bytes", 10 * 1024 * 1024),
            "backups": lg.getint("backups", 1),
            "fmt": lg.get("fmt", raw=True, fallback="%(asctime)s -%(funcName)s - %(levelname)s - %(message)s"),
        }

    def get_paths(self):
        p = self.app["paths"]
        return {
            "country_file": p.get("country_file"),
            "dem_tif": p.get("dem_tif"),
            "images_dir": p.get("images_dir", "images"),
            "saved_grids_dir": p.get("saved_grids_dir", "saved_grids"),
        }

    def get_visualization(self):
        vis = self.app["visualization"] if "visualization" in self.app else {}
        cmap_literal = vis.get("colormap", "[]")
        try:
            colormap = ast.literal_eval(cmap_literal)
        except Exception:
            colormap = []
        return {
            "n_levels": int(vis.get("n_levels", "15")),
            "colormap": colormap,
        }

    def get_ml(self):
        ml = self.app["ml"] if "ml" in self.app else {}
        return {
            "linear_model_path": ml.get("linear_model_path", "Linear_model_final1.joblib"),
            "lstm_path": ml.get("lstm_path", "neural/lstm.keras"),
            "scaler_path": ml.get("scaler_path", "neural/scaler.joblib"),
        }

    # --- DATABASE / MYSQL ---
    def get_database_credentials(self):
        db = self.database["mysql"]
        return {
            "host": db.get("host"),
            "port": db.get("port"),
            "user": db.get("user"),
            "password": db.get("password"),
        }

    def get_mysql_url(self):
        c = self.get_database_credentials()
        return f"{c['driver']}://{c['user']}:{c['password']}@{c['host']}:{c['port']}"

    # --- INFLUX ---
    def get_influx_config(self, mode="read"):
        if mode not in ("read", "write"):
            raise ValueError("mode musí být 'read' nebo 'write'")

        common = self.database["influx_common"] if "influx_common" in self.database else {}
        section = self.database["influx_read" if mode == "read" else "influx_write"]

        cfg = {
            "org": common.get("org"),
            "url": common.get("url"),
            "token": common.get("token"),

        }
        if mode == "read":
            cfg.update({
                "bucket": section.get("bucket", "realtime_cbl"),
                "measurements": [s.strip() for s in section.get("measurements", "").split(",") if s.strip()],
                "fields": [s.strip() for s in section.get("fields", "").split(",") if s.strip()],
                "tag_device": section.get("tag_device", "agent_host"),
                "field_temperature": section.get("field_temperature", "Teplota"),
                "field_signal": section.get("field_signal", "PrijimanaUroven"),
                "window": section.get("window", "1m"),
                "range": section.get("range", "-1m"),
            })
        else:
            cfg.update({
                "bucket": section.get("bucket", "telcorain_output"),
                "measurement": section.get("measurement", "telcorain"),
                "tag_cml_id": section.get("tag_cml_id", "cml_id"),
                "tag_side": section.get("tag_side", "side"),
                "field_temperature": section.get("field_temperature", "temperature"),
            })
        return cfg

    # --- COMPUTE ---
    def get_grid_config(self):
        g = self.compute["grid"]
        return {
            "x_points": g.getint("x_points", 500),
            "y_points": g.getint("y_points", 500),
            "mask_resolution_safe": g.getboolean("mask_resolution_safe", True),
        }

    def get_interpolation_config(self):
        itp = self.compute["interpolation"]
        return {
            "variogram_model": itp.get("variogram_model", "spherical"),
            "nlags": itp.getint("nlags", 40),
            "regression_model": itp.get("regression_model", "linear"),
        }

    def get_location(self):
        loc = self.compute["location"] if "location" in self.compute else {}
        return {
            "lat": float(loc.get("lat", "49.8175")),
            "lng": float(loc.get("lng", "15.4730")),
            "tz": loc.get("tz", "Europe/Prague"),
        }

