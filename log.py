import logging
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file, level=logging.INFO, max_bytes=10*1024*1024, backups=1, fmt="%(asctime)s -%(funcName)s - %(levelname)s - %(message)s"):
    formatter = logging.Formatter(fmt)
    handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backups, encoding='utf-8')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

