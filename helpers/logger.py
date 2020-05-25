import logging
import os

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


def setup_logger():
    if logging.getLogger().handlers:
        # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
        # `.basicConfig` does not execute. Thus we set the level directly.
        level = logging.getLevelName(LOG_LEVEL)
        logging.getLogger().setLevel(level)
        logger = logging.getLogger()
    else:
        FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
        logging.basicConfig(level=LOG_LEVEL, format=FORMAT)
        logger = logging.getLogger()

    return logger
