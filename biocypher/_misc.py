from typing import Any
from collections.abc import Iterable
import re

from neo4j_utils._misc import LIST_LIKE, if_none, to_list  # noqa: F401

__all__ = [
    'SIMPLE_TYPES',
    'cc',
    'dict_str',
    'ensure_iterable',
    'ensure_iterable_2',
    'first',
    'plain_tuple',
    'prettyfloat',
    'sc',
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
            if isinstance(value, SIMPLE_TYPES) or not plain_tuple(value) else
        value
            if isinstance(value, LIST_LIKE) else
        (value,)
    )


def plain_tuple(value: Any) -> bool:
    """
    Tells if ``value`` is just a tuple, or some fancy subclass of it.
    """

    return isinstance(value, tuple) and value.__class__.__mro__[0] == tuple


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

    return re.sub(r'[ \.]', '_', s).lower()


def first(value: Any) -> Any:
    """
    First item of an iterable. Simple values pass thru.
    """

    if isinstance(value, SIMPLE_TYPES):

        return value

    elif isinstance(value, Iterable):

        return next(iter(value), None)
