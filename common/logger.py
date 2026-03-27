import logging

def get_logger(name=None):
    logger = logging.getLogger(name or "dedupe")
    logger.setLevel(logging.DEBUG)

    if not logger.hasHandlers():
        fh = logging.FileHandler("dedupe.log")
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(fmt)
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger