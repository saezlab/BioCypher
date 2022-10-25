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
Intermediate objects that facilitate conversion of data sources to biocypher.

Transforms ordered collections of biomedical entities and relationships
to BioCypher objects that represent property graph nodes and edges.

Todo:
    - Calls to the classes are independent, so there is no way to
      check directly; nodes can be created at any point in time
      previous to edge creation. We could require a pass of all
      the nodes in the graph when creating edges. Pro: this would
      also allow a check whether the existing graph adheres to
      BioCypher, at least in the node domain. If it doesn't,
      the call does not make much sense.
    - establish a dictionary lookup with the id types to be used / basic
      type checking of the input
    - translation of id types using pypath translation facilities (to be
      later externalised)
"""

import re
import collections

from . import _misc
from ._misc import is_str
from ._logger import logger

__all__ = [
    'BC_TYPES',
    'Edge',
    'Node',
    'RelAsNode',
]

logger.debug(f'Loading module {__name__}.')


_RELFCR = re.compile('[\n\r]+')


class Entity:

    @staticmethod
    def _type_in_properties(props: dict) -> dict:

        if ':TYPE' in props:

            logger.warning(
                'Keyword `:TYPE` is reserved for Neo4j. '
                'Removing from properties.',
            )
            del props[':TYPE']

        return props

    @staticmethod
    def _process_str_props(props: dict) -> dict:

        return props

        props = {
            k:
            _RELFCR.sub(' ', ', '.join(_misc.to_list(v))).replace('"', "'")
            if is_str(v) or isinstance(v, list) and all(map(is_str, v)) else v
            for k, v in self.properties.items()
        }


class Node(
        collections.namedtuple(
            'NodeBase',
            (
                'id',
                'label',
                'id_type',
                'props',
            ),
            defaults = ('id', None),
        ),
        Entity,
    ):
    """
    Handoff class to represent biomedical entities as Neo4j nodes.

    Has id, label, property dict; id and label (in the Neo4j sense of a
    label, ie, the entity descriptor after the colon, such as
    ":Protein") are non-optional and called node_id and node_label to
    avoid confusion with "label" properties. Node labels are written in
    PascalCase and as nouns, as per Neo4j consensus.

    Args:
        id:
            Identifier for biological entity.
        label:
            Type of the entity.
        id_type:
            Type of the identifier.
        props:
            Further properties of the node.

    Todo:
        - check and correct small inconsistencies such as capitalisation
            of ID names ("uniprot" vs "UniProt")
        - check for correct ID patterns (eg "ENSG" + string of numbers,
            uniprot length)
        - ID conversion using pypath translation facilities for now
    """

    __slots__ = ()

    def __new__(
            cls,
            id: str,
            label: str,
            id_type: str = 'id',
            props: dict | None = None,
        ):
        """
        Add id field to properties.

        Check for reserved keywords.

        Replace unwanted characters in properties.
        """

        props = props or {}
        props['id'] = id
        props['id_type'] = id_type or None
        props = cls._type_in_properties(props)
        props = cls._process_str_props(props)

        new = super(Node, cls).__new__(
            cls, id, label.capitalize(), id_type = id_type, props = props,
        )
        new.entity = 'node'

        return new

    @property
    def nodes(self) -> tuple['Node']:
        """
        Create a tuple of node(s).

        Returns:
            This node in a single element tuple.
        """

        return (self,)

    @property
    def key(self) -> tuple[str, str]:
        """
        A key that identifies the group of graph components
        this item belongs to.
        """

        return (self.label, self.entity)


class Edge(
        collections.namedtuple(
            'EdgeBase',
            (
                'source',
                'target',
                'label',
                'id',
                'props',
            ),
            defaults = (None, None),
        ),
        Entity,
    ):
    """
    Handoff class to represent biomedical relationships in Neo4j.

    Has source and target ids, label, property dict; ids and label (in
    the Neo4j sense of a label, ie, the entity descriptor after the
    colon, such as ":TARGETS") are non-optional and called source_id,
    target_id, and relationship_label to avoid confusion with properties
    called "label", which usually denotes the human-readable form.
    Relationship labels are written in UPPERCASE and as verbs, as per
    Neo4j consensus.

    Args:
        source:
            Identifier of the source node.
        target:
            Identifier of the target node.
        label:
            Relation label: type of the interaction.
        props:
            Further relation properties.
    """

    __slots__ = ()

    def __new__(
            cls,
            source: str,
            target: str,
            label: str,
            id: str | None = None,
            props: dict | None = None,
        ):
        """
        Check for reserved keywords.

        Make sure label is uppercase.

        Replace unwanted characters in properties.
        """

        props = props or {}
        props = cls._type_in_properties(props)
        props = cls._process_str_props(props)

        new = super(Edge, cls).__new__(
            cls, source, target, label.upper(), id = id, props = props,
        )
        new.entity = 'edge'

        return new

    @property
    def edges(self) -> tuple['Edge']:
        """
        Create a tuple of edge(s).

        Returns:
            This edge in a single element tuple.
        """

        return (self,)

    @property
    def nodes(self) -> tuple:
        """
        Create a tuple of node(s).

        Returns:
            An empty tuple.
        """

        return ()

    @property
    def key(self) -> tuple[str, str]:
        """
        A key that identifies the group of graph components
        this item belongs to.
        """

        return (self.relationship_label, self.entity)


class RelAsNode(
        collections.namedtuple(
            'RelAsNodeBase',
            (
                'node',
                'source',
                'target',
            ),
        ),
        Entity,
    ):
    """
    Class to represent relationships as nodes.

    A relationship can be converted or alternatively represented as a node
    with in- and outgoing edges, ie. a triplet of a Node and two
    Edges. Main usage in type checking (instances where the
    receiving function needs to check whether it receives a relationship
    as a single edge or as a triplet).

    Args:
        node:
            Node representing the relationship.
        source:
            Eedge representing the source of the relationship.
        target:
            Edge representing the target of the relationship.
    """

    def __new__(
            cls,
            node: Node,
            source: Edge,
            target: Edge,
        ):

        for attr, t in cls.__new__.__annotations__.items():

            if not isinstance(locals()[attr], t):

                raise TypeError(
                    f'{cls.__name__}.{attr} must be '
                    f'of type `{t.__name__}`.',
                )

        new = super(RelAsNode, cls).__new__(cls, node, source, target)

        return new

    @property
    def edges(self) -> tuple[Edge, Edge]:
        """
        Create a tuple of edge(s).

        Returns:
            The source and target edges in a two elements tuple.
        """

        return (self.source_edge, self.target_edge)

    @property
    def nodes(self) -> tuple[Node]:
        """
        Create a tuple of node(s).

        Returns:
            An empty tuple.
        """

        return (self.node,)


BC_TYPES = (
    Node |
    Edge |
    RelAsNode
)
