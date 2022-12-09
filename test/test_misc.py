import pytest
import networkx as nx

import biocypher._misc as _misc


example_tree = {
    'B': 'A',
    'C': 'A',
    'D': 'B',
    'E': 'B',
    'F': 'C',
    'G': 'C',
    'H': 'E',
    'I': 'G',
}


disjoint_tree = {
    'B': 'A',
    'C': 'A',
    'D': 'B',
    'F': 'E',
    'G': 'E',
    'H': 'F',
}


def test_tree_vis():

    tree_vis = _misc.tree_figure(example_tree)

    assert tree_vis.DEPTH == 1
    assert tree_vis.WIDTH == 2
    assert tree_vis.root == 'A'


def test_tree_vis_from_networkx():

    G = nx.DiGraph(example_tree)

    tree_vis = _misc.tree_figure(G)

    assert tree_vis.DEPTH == 1
    assert tree_vis.WIDTH == 2
    assert tree_vis.root == 'A'


def test_disjoint_tree():

    with pytest.raises(ValueError):

        _misc.tree_figure(disjoint_tree)


if __name__ == '__main__':
    # to look at it
    print(_misc.tree_figure(example_tree).show())
    print(_misc.tree_figure(nx.DiGraph(example_tree)).show())
