from typing import Union

from hypothesis import given
from hypothesis import strategies as st
import pytest

from biocypher._meta import VersionNode
from biocypher._entity import Edge, Node, RelAsNode

__all__ = [
    'test_edge',
    'test_getting_properties_via_config',
    'test_node',
    'test_preferred_id_optional',
    'test_rel_as_node',
    'test_rel_as_node_invalid_node',
    'test_version_node',
    'test_virtual_leaves_node',
    'version_node',
]


@pytest.fixture
def version_node():
    yield VersionNode(
        offline=True,
        from_config=True,
        config_file='biocypher/_config/test_schema_config.yaml',
    )


def test_version_node(version_node):
    assert version_node.label == 'BioCypher'


def test_virtual_leaves_node(version_node):
    assert 'wikipathways.pathway' in version_node.schema


def test_getting_properties_via_config(version_node):
    assert 'name' in version_node.schema['protein'].get('properties')


@given(st.builds(Node))
def test_node(node):
    assert isinstance(node.id, str)
    assert isinstance(node.label, str)
    assert isinstance(node.props, dict)
    assert isinstance(node._asdict(), dict)

    assert 'id' in node.props


@given(st.builds(Edge))
def test_edge(edge):
    assert isinstance(edge.id, str) or edge.id == None
    assert isinstance(edge.source, str)
    assert isinstance(edge.target, str)
    assert isinstance(edge.label, str)
    assert isinstance(edge.props, dict)
    assert isinstance(edge._asdict(), dict)


@given(st.builds(RelAsNode))
def test_rel_as_node(rel_as_node):
    assert isinstance(rel_as_node.node, Node)
    assert isinstance(rel_as_node.source, Edge)
    assert isinstance(rel_as_node.target, Edge)


def test_rel_as_node_invalid_node():
    with pytest.raises(TypeError):
        RelAsNode('str', 1, 2.5122)


def test_preferred_id_optional(version_node):

    pti = version_node.schema.get('post translational interaction')

    assert pti.get('preferred_id') is None
