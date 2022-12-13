#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#

"""
Configuration of the module logger.
"""

__all__ = ['get_logger', 'log', 'logfile']

from datetime import datetime
import os
import pydoc
import logging

import neo4j_utils

from biocypher import _config
from biocypher._metadata import __version__


def get_logger(name: str = 'biocypher') -> logging.Logger:
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
        logger.propagate = False

        # formatting
        file_formatter = logging.Formatter(
            '%(asctime)s\t%(levelname)s\tmodule: %(module)s\n\t%(message)s',
        )
        stdout_formatter = logging.Formatter('%(levelname)s -- %(message)s')

        # file name and creation
        now = datetime.now()
        date_time = now.strftime('%Y%m%d-%H%M%S')

        logdir = _config.config('logdir')
        os.makedirs(logdir, exist_ok=True)
        logfile = os.path.join(logdir, f'biocypher-{date_time}.log')

        # handlers
        # stream handler
        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(stdout_formatter)

        # file handler
        file_handler = logging.FileHandler(logfile)

        if _config.config('debug'):
            file_handler.setLevel(logging.DEBUG)
        else:
            file_handler.setLevel(logging.INFO)

        file_handler.setFormatter(file_formatter)

        neo4ju_logger = logging.getLogger('neo4ju')

        for _logger in (logger, neo4ju_logger):

            _logger.addHandler(file_handler)
            _logger.addHandler(stdout_handler)

        # startup message
        logger.info(f'This is BioCypher version `{__version__}`.')
        logger.info(f'Logging into `{logfile}`.')

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
