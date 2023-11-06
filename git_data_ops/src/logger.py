import logging


def get_logger(name, log_level=logging.DEBUG):
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
