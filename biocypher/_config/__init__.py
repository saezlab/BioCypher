"""
Module data directory, including:

* The BioLink database schema
* The default config files
"""

import os
import warnings

from typing import Any, Optional

import appdirs
import yaml

__all__ = ["module_data", "module_data_path", "read_config", "config", "reset"]

_USER_CONFIG_DIR = appdirs.user_config_dir("biocypher", "saezlab")
_USER_CONFIG_FILE = os.path.join(_USER_CONFIG_DIR, "conf.yaml")


class MyLoader(yaml.SafeLoader):
    def construct_scalar(self, node):
        # Check if the scalar contains double quotes and an escape sequence
        value = super().construct_scalar(node)
        q = bool(node.style == '"')
        b = bool("\\" in value.encode("unicode_escape").decode("utf-8"))
        if q and b:
            warnings.warn(
                (
                    "Double quotes detected in YAML configuration scalar: "
                    f"{value.encode('unicode_escape')}. "
                    "These allow escape sequences and may cause problems, for "
                    "instance with the Neo4j admin import files (e.g. '\\t'). "
                    "Make sure you wanted to do this, and use single quotes "
                    "whenever possible."
                ),
                category=UserWarning,
            )
        return value


def module_data_path(name: str) -> str:
    """
    Absolute path to a YAML file shipped with the module.
    """

    here = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(here, f"{name}.yaml")


def module_data(name: str) -> Any:
    """
    Retrieve the contents of a YAML file shipped with this module.
    """

    path = module_data_path(name)

    return _read_yaml(path)


def _read_yaml(path: str) -> Optional[dict]:
    if os.path.exists(path):
        with open(path, "r") as fp:
            return yaml.load(fp.read(), Loader=MyLoader)


def read_config() -> dict:
    """
    Read the module config.

    Read and merge the built-in default, the user level and directory level
    configuration, with the later taking precendence over the former.

    TODO explain path configuration
    """

    defaults = module_data("biocypher_config")
    user = _read_yaml(_USER_CONFIG_FILE) or {}
    # TODO account for .yml?
    local = _read_yaml("biocypher_config.yaml") or _read_yaml("config/biocypher_config.yaml") or {}

    for key in defaults:
        value = local[key] if key in local else user[key] if key in user else None

        if value is not None:
            if isinstance(defaults[key], str):  # first level config (like title)
                defaults[key] = value
            else:
                defaults[key].update(value)

    return defaults


def config(*args, **kwargs) -> Optional[Any]:
    """
    Set or get module config parameters.
    """

    if args and kwargs:
        raise ValueError(
            "Setting and getting values in the same call is not allowed.",
        )

    if args:
        result = tuple(globals()["_config"].get(key, None) for key in args)

        return result[0] if len(result) == 1 else result

    for key, value in kwargs.items():
        globals()["_config"][key].update(value)


def reset():
    """
    Reload configuration from the config files.
    """

    globals()["_config"] = read_config()


reset()


def update_from_file(path: str):
    """
    Update the module configuration from a YAML file.
    """

    config(**_read_yaml(path))
