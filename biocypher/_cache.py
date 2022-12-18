#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#

from __future__ import annotations

"""
Manage the cache directory of Biocypher.
"""

import os
import hashlib
import json
from typing import Any

import appdirs

import biocypher._misc as _misc
import biocypher._config as _config
import biocypher._logger as _logger


def cache_key(*args: Any) -> str:
    """
    Cache key from a set of variables.
    """

    if not args:

        msg = 'Cache key can not be empty.'
        _logger.error(msg)
        raise ValueError(msg)

    args = args[0] if len(args) == 1 else _misc.to_list(args)

    try:

        args = sorted(args) if isinstance(args, list) else args

    except TypeError:

        pass

    return hashlib.md5sum(json.dumps(args)).hexdigest()


def cache_dir() -> str:
    """
    Path to the cache directory.
    """

    return _config.config('cachedir') or appdirs.user_cache_dir('biocypher')


def cache_path(cachedir: str | None = None, *key) -> str:
    """
    Path to a cache item.
    """

    cachedir = cachedir or cache_dir()
    fname = cache_key(*key)

    return os.path.join(cachedir, fname)


def save(obj: Any, *key: Any, cachedir: str | None = None):
    """
    Save an object to the cache.
    """

    path = cache_path(cachedir = cachedir, *key)

    _logger.logger.info(f'Saving object into `{path}`.')

    with open(path, 'wb') as fp:

        pickle.dump(obj = obj, file = fp)

    _logger.logger.debug(f'Finished writing `{path}`.')


def load(*key: Any, cachedir: str | None = None) -> Any | None:
    """
    Load an object from the cache.
    """

    path = cache_path(cachedir = cachedir, *key)

    if os.path.exists(path):

        _logger.logger.info(f'Loading object from `{path}`.')

        with open(path, 'rb') as fp:

            obj = pickle.load(file = fp)

        _logger.logger.debug(f'Finished reading `{path}`.')

    else:

        _logger.logger.debug(f'Not available in cache: `{path}`.')
        obj = None

    return obj


def remove(*key: Any, cachedir: str | None = None):
    """
    Remove a cache item by its key.
    """

    path = cache_path(cachedir = cachedir, *key)
    _remove(path)


def wipe():
    """
    Remove all cache contents.
    """

    cachedir = cache_dir()

    for f in os.listdir(cachedir):

        path = os.path.join(cachedir, f)
        _remove(path)


def _remove(path: str):

    if os.path.isfile(path) or os.path.islink(path):

        os.unlink(path)
