import logging


def get_logger(name: str | None) -> logging.Logger:
    logger = logging.getLogger(name or "dedupe")
    logger.setLevel(logging.DEBUG)

    if not logger.hasHandlers():
        fh = logging.FileHandler("dedupe.log")
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        fh_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        fh_formatter = logging.Formatter(fh_fmt)
        fh.setFormatter(fh_formatter)

        ch_fmt = "%(levelname)s - %(message)s"
        ch_formatter = logging.Formatter(ch_fmt)
        ch.setFormatter(ch_formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger
