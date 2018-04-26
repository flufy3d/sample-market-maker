import logging
import os
from market_maker.settings import settings


def setup_custom_logger(name, log_level=settings.LOG_LEVEL):
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.addHandler(handler)

    #if not os.path.exists('tmp'):
    #    os.makedirs('tmp')
    #fh = logging.FileHandler('./tmp/log.txt')
    #fh.setFormatter(formatter)
    #logger.addHandler(fh)

    return logger
