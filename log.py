import logging
from logging.handlers import RotatingFileHandler


def setup_logger(
    name,
    log_file,
    level=logging.INFO,
    max_bytes=10 * 1024 * 1024,
    backups=1,
    fmt="%(asctime)s -%(funcName)s - %(levelname)s - %(message)s",
):
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


class LoggerManager:
    def __init__(self, config):
        self.config = config
        self.loggers = {}

    def get_logger(self, name):
        """
        Returns a logger with the given name. If the logger does not exist, it is created.
        """
        if name not in self.loggers:
            log_config = self.config.get_logging_config()
            self.loggers[name] = setup_logger(
                name,
                log_config.get(f"{name}_log", "default.log"),
                level=log_config.get("level", "INFO"),
            )
        return self.loggers[name]
