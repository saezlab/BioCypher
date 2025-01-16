import logging

import networkx as nx
import pytest

from biocypher._misc import create_tree_visualisation


@pytest.fixture(scope="function")
def _get_inheritance_tree() -> dict:
    inheritance_tree = {
        "B": "A",
        "C": "A",
        "D": "B",
        "E": "B",
        "F": "C",
        "G": "C",
        "H": "E",
        "I": "G",
    }
    return inheritance_tree


@pytest.fixture(scope="function")
def _get_disjoint_tree() -> dict:
    disjoint_tree = {
        "B": "A",
        "C": "A",
        "D": "B",
        "F": "E",
        "G": "E",
        "H": "F",
    }
    return disjoint_tree


def test_tree_vis(_get_inheritance_tree):
    tree_vis = create_tree_visualisation(_get_inheritance_tree)

    expected_tree_vis = create_tree_visualisation(nx.DiGraph(_get_inheritance_tree))

    assert tree_vis.DEPTH == 1
    assert tree_vis.WIDTH == 2
    assert tree_vis.root == "A"
    assert tree_vis.to_json(with_data=True) == expected_tree_vis.to_json(with_data=True)


def test_tree_vis_from_networkx(_get_inheritance_tree):
    graph = nx.DiGraph(_get_inheritance_tree)

    tree_vis = create_tree_visualisation(graph)

    expected_tree_vis = create_tree_visualisation(nx.DiGraph(_get_inheritance_tree))

    assert tree_vis.DEPTH == 1
    assert tree_vis.WIDTH == 2
    assert tree_vis.root == "A"
    assert tree_vis.to_json(with_data=True) == expected_tree_vis.to_json(with_data=True)


def test_disjoint_tree(_get_disjoint_tree):
    with pytest.raises(ValueError):
        create_tree_visualisation(_get_disjoint_tree)


def test_tree_vis_multiple_inheritance(caplog):
    inheritance_tree_data = {
        "root": [],
        "level1A": ["root"],
        "level1B": ["root"],
        "level2A": ["level1A", "level1B"],
    }
    inheritance_tree = nx.DiGraph(inheritance_tree_data)
    with caplog.at_level(logging.INFO):
        tree_vis = create_tree_visualisation(inheritance_tree)

    expected_tree = {
        "root": [],
        "level1A": ["root"],
        "level1B": ["root"],
        "level2A": ["level1A"],
    }
    expected_tree_vis = create_tree_visualisation(nx.DiGraph(expected_tree))

    assert any("The ontology contains multiple inheritance" in record.message for record in caplog.records)
    assert tree_vis.to_json(with_data=True) == expected_tree_vis.to_json(with_data=True)


if __name__ == "__main__":
    # to look at it
    print(create_tree_visualisation(nx.DiGraph(_get_inheritance_tree)).show())
