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
Module data directory, including:

* The BioLink database schema
* The default config files
"""

from __future__ import annotations

from typing import Any, Optional
import os
import re
import inspect

import yaml
import appdirs

__all__ = [
    'config',
    'module_data',
    'module_data_path',
    'neo4j_config',
    'read_config',
    'reset',
    'save',
]


_SYNONYMS = {
    'address': 'neo4j_uri',
    'neo4j_address': 'neo4j_uri',
    'uri': 'neo4j_uri',
    'db_uri': 'neo4j_uri',
    'db': 'neo4j_db',
    'database': 'neo4j_db',
    'neo4j_database': 'neo4j_db',
    'db_name': 'neo4j_db',
    'insert_batch_size': 'neo4j_insert_batch_size',
    'batch_size': 'neo4j_insert_batch_size',
    'passwd': 'neo4j_pw',
    'password': 'neo4j_pw',
    'neo4j_passwd': 'neo4j_pw',
    'neo4j_password': 'neo4j_pw',
    'db_passwd': 'neo4j_pw',
    'login': 'neo4j_user',
    'neo4j_login': 'neo4j_user',
    'db_user': 'neo4j_user',
    'neo4j_delimiter': 'csv_delimiter',
    'delimiter': 'csv_delimiter',
    'array_delimiter': 'csv_array_delimiter',
    'quote_char': 'csv_quote_char',
}

_NEO4J_SYNONYMS = {
    'db': 'name',
    'pw': 'passwd',
}

_USER_CONFIG_DIR = appdirs.user_config_dir('biocypher', 'saezlab')
_USER_CONFIG_FILE = os.path.join(_USER_CONFIG_DIR, 'conf.yaml')


def module_data_path(name: str) -> str:
    """
    Absolute path to a YAML file shipped with the module.
    """

    here = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(here, f'{name}.yaml')


def module_data(name: str) -> Any:
    """
    Retrieve the contents of a YAML file shipped with this module.
    """

    path = module_data_path(name)

    return _read_yaml(path)


def _read_yaml(path: str) -> Optional[dict]:

    if os.path.exists(path):

        with open(path) as fp:

            return yaml.load(fp.read(), Loader=yaml.FullLoader)


def _conf_key_synonyms(conf: dict) -> dict:
    """
    Translate config key synonyms in a dict.
    """

    return {_SYNONYMS.get(k, k): v for k, v in conf.items()}


def read_config() -> dict:
    """
    Read the module config.

    Read and merge the built-in default, the user level and directory level
    configuration, the latest having the highest precedence.

    TODO explain path configuration
    """

    defaults = _conf_key_synonyms(module_data('module_config'))
    user = _read_yaml(_USER_CONFIG_FILE) or {}
    local = _read_yaml('biocypher.yaml') or {}

    defaults.update(_conf_key_synonyms(user))
    defaults.update(_conf_key_synonyms(local))

    return defaults


def config(*args, **kwargs) -> Optional[Any]:
    """
    Set or get module config parameters.

    TODO explain: is setting permanent or session-specific?
    """

    if args and kwargs:

        raise ValueError(
            'Setting and getting values in the same call is not allowed.',
        )

    if args:

        result = tuple(
            globals()['_config'].get(key, None)
            for key in args
        )

        return result[0] if len(result) == 1 else result

    globals()['_config'].update(kwargs)


def neo4j_config() -> dict:
    """
    Retrieves config values required for Neo4j connections.

    The names correspond to the argument names of the ``Driver`` class, both
    in this and the ``neo4j_utils`` module.
    """

    n4jprefix = re.compile('^neo4j_')
    db_prefix = {'name', 'uri', 'user', 'passwd'}

    def config_key(key: str) -> str:

        key = n4jprefix.sub('', key)
        key = _NEO4J_SYNONYMS.get(key, key)

        return f'db_{key}' if key in db_prefix else key


    return {
        config_key(k): v
        for k, v in _config.items()
        if k.startswith('neo4j_')
    }


def argconf(key: str) -> Any:
    """
    Option from argument if available, else from config.

    Args:
        key:
            A config key.

    Returns:
        Current value of the config key, looked up by the precendence order
        above.
    """

    parent_locals = inspect.currentframe().f_back.f_locals

    return _misc.if_none(parent_locals.get(key), config(key))


def arginstconf(key: str, attr: str | None) -> Any:
    """
    Option from argument if available, else from instance or class attribute
    if available, else from config.

    Args:
        key:
            A config key.
        attr:
            Name of the instance or class attribute, if it is different
            from the ``key``.

    Returns:
        Current value of the config key, looked up by the precendence order
        above.
    """

    parent_locals = inspect.currentframe().f_back.f_locals
    self = parent_locals.get('self', parent_locals.get('cls'))
    attr = attr or key

    return _misc.if_none(
        parent_locals.get(key),
        getattr(self, attr, None),
        conf(key),
    )


def save(path: str | None = None, user: bool = False):
    """
    Save the current config into a YAML file.

    Args:
        path:
            Path to the YAML file. By default *biocypher.yaml* in the
            current directory.
        user:
            Save to the user level config directory: this will override the
            default values for all sessions run by the same user, but not
            the local config in the working directory.
    """

    path = _USER_CONFIG_FILE if user else path or 'biocypher.yaml'

    with open(path, 'w') as fp:

        yaml.dump(_config, fp)


def reset():
    """
    Reload configuration from the config files.
    """

    globals()['_config'] = read_config()


reset()
