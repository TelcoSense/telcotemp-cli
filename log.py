import logging
from logging.handlers import RotatingFileHandler


class LoggerManager:
    def __init__(self, config):
        self.config = config
        self.loggers = {}

    def _setup_logger(
        self,
        name,
        log_file,
        level=logging.INFO,
        max_bytes=10 * 1024 * 1024,
        backups=1,
        fmt="%(asctime)s -%(funcName)s - %(levelname)s - %(message)s",
    ):
        """
        Sets up a logger with the given configuration.
        """
        formatter = logging.Formatter(fmt)
        handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
        handler.setFormatter(formatter)

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.propagate = False
        return logger

    def get_logger(self, name):
        """
        Returns a logger with the given name. If the logger does not exist, it is created.
        """
        if name not in self.loggers:
            log_config = self.config.get_logging_config()
            self.loggers[name] = self._setup_logger(
                name,
                log_config.get("backend_log", "app.log"),
                level=log_config.get("level", "INFO"),
            )
        return self.loggers[name]
