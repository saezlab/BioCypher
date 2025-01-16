"""
Configuration of the module logger.
"""

__all__ = ["get_logger", "log", "logfile"]

import logging
import os
import pydoc

from datetime import datetime

from biocypher import _config
from biocypher._metadata import __version__


def get_logger(name: str = "biocypher") -> logging.Logger:
    """
    Access the module logger, create a new one if does not exist yet.

    Method providing central logger instance to main module. Is called
    only from main submodule, :mod:`biocypher.driver`. In child modules,
    the standard Python logging facility is called
    (using ``logging.getLogger(__name__)``), automatically inheriting
    the handlers from the central logger.

    The file handler creates a log file named after the current date and
    time. Levels to output to file and console can be set here.

    Args:
        name:
            Name of the logger instance.

    Returns:
        An instance of the Python :py:mod:`logging.Logger`.
    """

    if not logging.getLogger(name).hasHandlers():
        # create logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = True

        # formatting
        file_formatter = logging.Formatter(
            "%(asctime)s\t%(levelname)s\tmodule:%(module)s\n%(message)s",
        )
        stdout_formatter = logging.Formatter("%(levelname)s -- %(message)s")

        # file name and creation
        now = datetime.now()
        date_time = now.strftime("%Y%m%d-%H%M%S")

        log_to_disk = _config.config("biocypher").get("log_to_disk")

        if log_to_disk:
            logdir = _config.config("biocypher").get("log_directory") or "biocypher-log"
            os.makedirs(logdir, exist_ok=True)
            logfile = os.path.join(logdir, f"biocypher-{date_time}.log")

            # file handler
            file_handler = logging.FileHandler(logfile)

            if _config.config("biocypher").get("debug"):
                file_handler.setLevel(logging.DEBUG)
            else:
                file_handler.setLevel(logging.INFO)

            file_handler.setFormatter(file_formatter)

            logger.addHandler(file_handler)

        # handlers
        # stream handler
        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(stdout_formatter)

        # add handlers
        logger.addHandler(stdout_handler)

        # startup message
        logger.info(f"This is BioCypher v{__version__}.")
        if log_to_disk:
            logger.info(f"Logging into `{logfile}`.")
        else:
            logger.info("Logging into stdout.")

    return logging.getLogger(name)


def logfile() -> str:
    """
    Path to the log file.
    """

    return get_logger().handlers[0].baseFilename


def log():
    """
    Browse the log file.
    """

    with open(logfile()) as fp:
        pydoc.pager(fp.read())


logger = get_logger()
