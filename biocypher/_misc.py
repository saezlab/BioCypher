from typing import Any
from collections.abc import Iterable
import re

from neo4j_utils._misc import LIST_LIKE, if_none, to_list  # noqa: F401

__all__ = [
    'SIMPLE_TYPES',
    'ensure_iterable',
    'prettyfloat',
    'dict_str',
    'is_str',
    'cc',
    'sc',
    'first',
]

SIMPLE_TYPES = (
    bytes,
    str,
    int,
    float,
    bool,
    type(None),
)


def ensure_iterable(value: Any) -> Iterable:
    """
    Returns iterables, except strings, wraps simple types into tuple.
    """

    return value if isinstance(value, LIST_LIKE) else (value,)


def ensure_iterable_2(value: Any) -> Iterable:
    """
    Same as ``ensure_iterable`` but considers tuple a simple type.
    """

    return (
        (value,)
            if isinstance(value, (SIMPLE_TYPES, tuple)) else
        value
            if isinstance(value, LIST_LIKE) else
        (value,)
    )


def prettyfloat(n: float) -> str:
    """
    Floats as strings of two value digits, for printing.
    """

    return '%.02g' % n if isinstance(n, float) else str(n)


def dict_str(dct: dict, sep: str = ', ') -> str:
    """
    Compact string representation of a dict.
    """

    if not isinstance(dct, dict):

        return str(dct)

    return sep.join(
        f'{key}={prettyfloat(dct[key])}'
        for key in sorted(dct.key())
    )


is_str = lambda x: isinstance(x, str)


def cc(s: str) -> str:
    """
    Convert sentence case to CamelCase.

    From ``bmt.utils``.

    Args:
        s:
            Input string in sentence case

    Returns:
        String in CamelCase form.
    """
    return re.sub(r'(?:^|\.| )([a-zA-Z])', lambda m: m.group(1).upper(), s)


def sc(s: str) -> str:
    """
    Convert sentence case to snake_case.

    From ``bmt.utils``.

    Args:
        s:
            Input string in sentence case.

    Returns:
        String in snake_case form.
    """

    return re.sub('[ \.]', '_', s).lower()


def first(value: Any) -> Any:
    """
    First item of an iterable. Simple values pass thru.
    """

    if isinstance(value, SIMPLE_TYPES):

        return value

    elif isinstance(value, Iterable):

        return next(iter(value), None)
