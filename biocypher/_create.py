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
Create a property graph database for biomedical research applications.

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

from dataclasses import field, dataclass
import re

from . import _misc
from ._misc import is_str
from ._logger import logger

__all__ = [
    'BC_TYPES',
    'BioCypherEdge',
    'BioCypherNode',
    'BioCypherRelAsNode',
]

logger.debug(f'Loading module {__name__}.')


_RELFCR = re.compile('[\n\r]+')


class BioCypherEntity:

    def _type_in_properties(self):

        if ':TYPE' in self.properties:

            logger.warning(
                'Keyword `:TYPE` is reserved for Neo4j. '
                'Removing from properties.',
            )
            del self.properties[':TYPE']

    def _process_str_props(self):

        return

        self.properties = {
            k:
            _RELFCR.sub(' ', ', '.join(_misc.to_list(v))).replace('"', "'")
            if is_str(v) or isinstance(v, list) and all(map(is_str, v)) else v
            for k, v in self.properties.items()
        }


@dataclass
class BioCypherNode(BioCypherEntity):
    """
    Handoff class to represent biomedical entities as Neo4j nodes.

    Has id, label, property dict; id and label (in the Neo4j sense of a
    label, ie, the entity descriptor after the colon, such as
    ":Protein") are non-optional and called node_id and node_label to
    avoid confusion with "label" properties. Node labels are written in
    PascalCase and as nouns, as per Neo4j consensus.

    Args:
        node_id (string): consensus "best" id for biological entity
        node_label (string): primary type of entity, capitalised
        **properties (kwargs): collection of all other properties to be
            passed to neo4j for the respective node (dict)

    Todo:
        - check and correct small inconsistencies such as capitalisation
            of ID names ("uniprot" vs "UniProt")
        - check for correct ID patterns (eg "ENSG" + string of numbers,
            uniprot length)
        - ID conversion using pypath translation facilities for now
    """

    node_id: str
    node_label: str
    preferred_id: str = 'id'
    properties: dict = field(default_factory=dict)

    def __post_init__(self):
        """
        Add id field to properties.

        Check for reserved keywords.

        Replace unwanted characters in properties.
        """

        self.entity = 'node'
        self.properties['id'] = self.node_id
        self.properties['preferred_id'] = self.preferred_id or None
        # TODO actually make None possible here; as is, "id" is the default in
        # the dataclass as well as in the configuration file

        self._type_in_properties()
        self._process_str_props()

    def get_id(self) -> str:
        """
        Returns primary node identifier.

        Returns:
            The node identifier.
        """
        return self.node_id

    def get_label(self) -> str:
        """
        Primary node label.

        Returns:
            The label of the node.
        """
        return self.node_label

    def get_preferred_id(self) -> str:
        """
        The preferred identifier.

        Returns:
            The preferred ID of the node.
        """
        return self.preferred_id

    def get_properties(self) -> dict:
        """
        Properties of the node.

        Returns:
            Node properties apart from primary id and label as key-value pairs.
        """
        return self.properties

    def get_dict(self) -> dict:
        """
        Return dict of id, labels, and properties.

        Returns:
            dict: node_id and node_label as top-level key-value pairs,
            properties as second-level dict.
        """
        return {
            'node_id': self.node_id,
            'node_label': self.node_label,
            'properties': self.properties,
        }

    @property
    def nodes(self) -> tuple['BioCypherNode']:
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

        return (self.node_label, self.entity)


@dataclass
class BioCypherEdge(BioCypherEntity):
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
        source_id:
            Consensus "best" id for biological entity.
        target_id:
            Consensus "best" id for biological entity.
        relationship_label:
            Type of interaction, UPPERCASE.
        properties:
            Collection of all other properties of the respective edge.
    """

    source_id: str
    target_id: str
    relationship_label: str
    id: str = None
    properties: dict = field(default_factory=dict)

    def __post_init__(self):
        """
        Check for reserved keywords.
        """

        self.entity = 'edge'
        self._type_in_properties()

    def get_id(self) -> str | None:
        """
        Returns primary node identifier or None.

        Returns:
            str: node_id
        """

        return self.id

    def get_source_id(self) -> str:
        """
        Primary node identifier of the relationship source.

        Returns:
            A node identifier.
        """
        return self.source_id

    def get_target_id(self) -> str:
        """
        Primary node identifier of the relationship target.

        Returns:
            A node identifier.
        """
        return self.target_id

    def get_label(self) -> str:
        """
        Label of the relationship.

        Returns:
            str: relationship_label
        """
        return self.relationship_label

    def get_properties(self) -> dict:
        """
        Properties of the relationship.

        Returns:
            All relationship properties apart from primary ids and label
            as key-value pairs.
        """
        return self.properties

    def get_dict(self) -> dict:
        """
        Return dict of ids, label, and properties.

        Returns:
            dict: source_id, target_id and relationship_label as
                top-level key-value pairs, properties as second-level
                dict.
        """
        return {
            'source_id': self.source_id,
            'target_id': self.target_id,
            'relationship_label': self.relationship_label,
            'properties': self.properties,
        }

    @property
    def edges(self) -> tuple['BioCypherEdge']:
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


@dataclass(frozen=True)
class BioCypherRelAsNode:
    """
    Class to represent relationships as nodes.

    A relationship can be converted or alternatively represented as a node
    with in- and outgoing edges, ie. a triplet of a BioCypherNode and two
    BioCypherEdges. Main usage in type checking (instances where the
    receiving function needs to check whether it receives a relationship
    as a single edge or as a triplet).

    Args:
        node:
            Node representing the relationship.
        source_edge:
            Eedge representing the source of the relationship.
        target_edge:
            Edge representing the target of the relationship.
    """

    node: BioCypherNode
    source_edge: BioCypherEdge
    target_edge: BioCypherEdge

    def __post_init__(self):

        # wow, I thought dataclass at least does this out of the box
        for attr, t in self.__class__.__annotations__.items():

            if not isinstance(getattr(self, attr), t):

                raise TypeError(
                    f'{self.__class__.__name__}.{attr} must be '
                    f'of type `{t.__name__}`.',
                )

    def get_node(self):
        """
        The node representing the former relationship.
        """
        return self.node

    def get_source_edge(self):
        """
        The edge pointing to the source of the relationship.
        """
        return self.source_edge

    def get_target_edge(self) -> BioCypherEdge:
        """
        The edge pointing to the target of the relationship.
        """
        return self.target_edge

    @property
    def edges(self) -> tuple[BioCypherEdge, BioCypherEdge]:
        """
        Create a tuple of edge(s).

        Returns:
            The source and target edges in a two elements tuple.
        """

        return (self.source_edge, self.target_edge)

    @property
    def nodes(self) -> tuple[BioCypherNode]:
        """
        Create a tuple of node(s).

        Returns:
            An empty tuple.
        """

        return (self.node,)


BC_TYPES = (
    BioCypherNode |
    BioCypherEdge |
    BioCypherRelAsNode
)
