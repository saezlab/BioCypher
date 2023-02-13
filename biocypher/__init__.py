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
Graph database standard for molecular biology
"""

__all__ = [
    '__version__',
    '__author__',
    'module_data',
    'config',
    'logfile',
    'log',
    'Driver',
]

from ._config import config, module_data
from ._driver import Driver
from ._logger import log, logfile
from ._metadata import __author__, __version__
