"""
Handy functions for use in various places.
"""

import re

from collections.abc import Iterable
from typing import (
    Any,
    Generator,
    ItemsView,
    KeysView,
    Mapping,
    Union,
    ValuesView,
)

import networkx as nx
import stringcase

from treelib import Tree

from ._logger import logger

logger.debug(f"Loading module {__name__}.")

__all__ = ["LIST_LIKE", "SIMPLE_TYPES", "ensure_iterable", "to_list"]

SIMPLE_TYPES = (
    bytes,
    str,
    int,
    float,
    bool,
    type(None),
)

LIST_LIKE = (
    list,
    set,
    tuple,
    Generator,
    ItemsView,
    KeysView,
    Mapping,
    ValuesView,
)


def to_list(value: Any) -> list:
    """
    Ensures that ``value`` is a list.
    """

    if isinstance(value, LIST_LIKE):
        value = list(value)

    else:
        value = [value]

    return value


def ensure_iterable(value: Any) -> Iterable:
    """
    Returns iterables, except strings, wraps simple types into tuple.
    """

    return value if isinstance(value, LIST_LIKE) else (value,)


def create_tree_visualisation(inheritance_graph: Union[dict, nx.Graph]) -> Tree:
    """
    Creates a visualisation of the inheritance tree using treelib.
    """
    inheritance_tree = _get_inheritance_tree(inheritance_graph)
    classes, root = _find_root_node(inheritance_tree)

    tree = Tree()
    tree.create_node(root, root)
    while classes:
        for child in classes:
            parent = inheritance_tree[child]
            if parent in tree.nodes.keys() or parent == root:
                tree.create_node(child, child, parent=parent)

        for node in tree.nodes.keys():
            if node in classes:
                classes.remove(node)

    return tree


def _get_inheritance_tree(inheritance_graph: Union[dict, nx.Graph]) -> dict:
    """Transforms an inheritance_graph into an inheritance_tree.

    Args:
        inheritance_graph: A dict or nx.Graph representing the inheritance graph.

    Returns:
        A dict representing the inheritance tree.
    """
    if isinstance(inheritance_graph, nx.Graph):
        inheritance_tree = nx.to_dict_of_lists(inheritance_graph)

        multiple_parents_present = _multiple_inheritance_present(inheritance_tree)
        if multiple_parents_present:
            logger.warning(
                "The ontology contains multiple inheritance (one child node "
                "has multiple parent nodes). This is not visualized in the "
                "following hierarchy tree (the child node is only added once). "
                "If you wish to browse all relationships of the parsed "
                "ontologies, write a graphml file to disk using "
                "`to_disk = <directory>` and view this file."
            )

        # unlist values
        inheritance_tree = {k: v[0] for k, v in inheritance_tree.items() if v}
        return inheritance_tree
    elif not _multiple_inheritance_present(inheritance_graph):
        return inheritance_graph


def _multiple_inheritance_present(inheritance_tree: dict) -> bool:
    """Checks if multiple inheritance is present in the inheritance_tree."""
    return any(len(value) > 1 for value in inheritance_tree.values())


def _find_root_node(inheritance_tree: dict) -> tuple[set, str]:
    classes = set(inheritance_tree.keys())
    parents = set(inheritance_tree.values())
    root = list(parents - classes)
    if len(root) > 1:
        if "entity" in root:
            root = "entity"  # TODO: default: good standard?
        else:
            raise ValueError("Inheritance tree cannot have more than one root node. " f"Found {len(root)}: {root}.")
    else:
        root = root[0]
    if not root:
        # find key whose value is None
        root = list(inheritance_tree.keys())[list(inheritance_tree.values()).index(None)]
    return classes, root


# string conversion, adapted from Biolink Model Toolkit
lowercase_pattern = re.compile(r"[a-zA-Z]*[a-z][a-zA-Z]*")
underscore_pattern = re.compile(r"(?<!^)(?=[A-Z][a-z])")


def from_pascal(s: str, sep: str = " ") -> str:
    underscored = underscore_pattern.sub(sep, s)
    lowercased = lowercase_pattern.sub(
        lambda match: match.group(0).lower(),
        underscored,
    )
    return lowercased


def pascalcase_to_sentencecase(s: str) -> str:
    """
    Convert PascalCase to sentence case.

    Args:
        s: Input string in PascalCase

    Returns:
        string in sentence case form
    """
    return from_pascal(s, sep=" ")


def snakecase_to_sentencecase(s: str) -> str:
    """
    Convert snake_case to sentence case.

    Args:
        s: Input string in snake_case

    Returns:
        string in sentence case form
    """
    return stringcase.sentencecase(s).lower()


def sentencecase_to_snakecase(s: str) -> str:
    """
    Convert sentence case to snake_case.

    Args:
        s: Input string in sentence case

    Returns:
        string in snake_case form
    """
    return stringcase.snakecase(s).lower()


def sentencecase_to_pascalcase(s: str, sep: str = r"\s") -> str:
    """
    Convert sentence case to PascalCase.

    Args:
        s: Input string in sentence case

    Returns:
        string in PascalCase form
    """
    return re.sub(
        r"(?:^|[" + sep + "])([a-zA-Z])",
        lambda match: match.group(1).upper(),
        s,
    )


def to_lower_sentence_case(s: str) -> str:
    """
    Convert any string to lower sentence case. Works with snake_case,
    PascalCase, and sentence case.

    Args:
        s: Input string

    Returns:
        string in lower sentence case form
    """
    if "_" in s:
        return snakecase_to_sentencecase(s)
    elif " " in s:
        return s.lower()
    elif s[0].isupper():
        return pascalcase_to_sentencecase(s)
    else:
        return s


def is_nested(lst) -> bool:
    """
    Check if a list is nested.

    Args:
        lst (list): The list to check.

    Returns:
        bool: True if the list is nested, False otherwise.
    """
    for item in lst:
        if isinstance(item, list):
            return True
    return False
