"""
BioCypher: a unifying framework for biomedical knowledge graphs.
"""

__all__ = [
    "__version__",
    "__author__",
    "module_data",
    "config",
    "logfile",
    "log",
    "Driver",
    "BioCypher",
    "FileDownload",
    "APIRequest",
]

from ._config import config, module_data
from ._core import BioCypher
from ._get import APIRequest, FileDownload
from ._logger import log, logfile, logger
from ._metadata import __author__, __version__


class Driver(BioCypher):
    # initialise parent class but log a warning
    def __init__(self, *args, **kwargs):
        logger.warning(
            "The class `Driver` is deprecated and will be removed in a future "
            "release. Please use `BioCypher` instead."
        )
        super().__init__(*args, **kwargs)
