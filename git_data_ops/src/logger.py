import logging
import os


def get_logger(file, log_level=logging.DEBUG):
    name = os.path.splitext(os.path.basename(file))[0]
    logger = logging.getLogger(name)

    # Configure the logger
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    return logger
