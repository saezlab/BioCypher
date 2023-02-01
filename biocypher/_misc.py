from __future__ import annotations

from typing import (
    Any,
    Mapping,
    KeysView,
    Generator,
    ItemsView,
    TYPE_CHECKING,
    ValuesView,
)
from collections.abc import Iterable
import re

from neo4j_utils._misc import LIST_LIKE, if_none, to_list  # noqa: F401
import treelib

if TYPE_CHECKING:

    import networkx as nx

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
    'to_set',
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
            if isinstance(value, SIMPLE_TYPES) or tuple_child(value) else
        value
            if isinstance(value, LIST_LIKE) else
        (value,)
    )


def to_set(value: Any) -> set:
    """
    Makes sure `value` is a `set`.

    If it is already, returns it unchanged; otherwise converts it to a set.
    """

    if isinstance(value, set):

        return value

    elif value is None:

        return set()

    elif isinstance(value, SIMPLE_TYPES):

        return {value}

    else:

        value = set(value)


def tuple_child(value: Any) -> bool:
    """
    Tells if ``value`` is just a tuple, or some fancy subclass of it.

    Returns:
        *True* if ``value`` is a tuple, but not just a plain tuple.
    """

    return isinstance(value, tuple) and value.__class__.__mro__[0] != tuple


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
        for key in sorted(dct.keys())
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


def tree_figure(tree: dict | 'nx.Graph') -> treelib.Tree:
    """
    Creates a visualisation of the inheritance tree using treelib.
    """

    nx = try_import('networkx')

    if isinstance(tree, nx.Graph):

        tree = nx.to_dict_of_lists(tree)
        # unlist values
        tree = {k: v[0] for k, v in tree.items() if v}

    # ugly, imperfect solution, to be fixed later
    # find root node
    classes = set(tree.keys())
    parents = set(tree.values())
    root = list(parents - classes)

    if len(root) > 1:
        raise ValueError(
            'Inheritance tree cannot have more than one root node.'
        )
    else:
        root = root[0]

    if not root:

        # find key whose value is None
        root = list(tree.keys())[
            list(tree.values()).index(None)
        ]

    _tree = treelib.Tree()
    _tree.create_node(root, root)

    while classes:

        for child in classes:

            parent = tree[child]

            if parent in _tree.nodes.keys() or parent == root:

                _tree.create_node(child, child, parent=parent)

        for node in _tree.nodes.keys():

            if node in classes:

                classes.remove(node)

    return _tree


def try_import(module):
    """
    Import a module, send warning if not available.

    Returns:
        The module object on successful import, otherwise None.
    """

    try:

        the_module = __import__(module, fromlist = [module.split('.')[0]])

    except ModuleNotFoundError:

        from ._logger import logger
        msg = f'Module `{module}` not available.'
        warnings.warn(msg)
        logger.warning(msg)
        the_module = None

    return the_module


def nested_tree(tree: dict[str, str], root: Any = None) -> dict[str, dict]:
    """
    Nested dict representation of a tree, from a dict of child->parent pairs.

    Args:
        tree:
            Child parent pairs.
    """

    flat_stack = collections.defaultdict(dict)
    tree_stack = {}

    roots = set(tree.values()) - set(tree.keys())
    tree = tree.copy()
    tree.update({k: root for k in roots})

    for child, parent in tree.items():

        _ = flat_stack[child]

        if parent:

            flat_stack[parent][child] = flat_stack[child]

        elif child:

            tree_stack[child] = flat_stack[child]

    return tree_stack
