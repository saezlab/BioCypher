"""
BioCypher 'create' module. Handles the creation of BioCypher node and edge
dataclasses.
"""

import os

from dataclasses import dataclass, field
from typing import Union

from ._logger import logger

logger.debug(f"Loading module {__name__}.")

__all__ = [
    "BioCypherEdge",
    "BioCypherNode",
    "BioCypherRelAsNode",
]


@dataclass(frozen=True)
class BioCypherNode:
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
    preferred_id: str = "id"
    properties: dict = field(default_factory=dict)

    def __post_init__(self):
        """
        Add id field to properties.

        Check for reserved keywords.

        Replace unwanted characters in properties.
        """
        self.properties["id"] = self.node_id
        self.properties["preferred_id"] = self.preferred_id or None
        # TODO actually make None possible here; as is, "id" is the default in
        # the dataclass as well as in the configuration file

        if ":TYPE" in self.properties.keys():
            logger.warning(
                "Keyword ':TYPE' is reserved for Neo4j. Removing from properties.",
                # "Renaming to 'type'."
            )
            # self.properties["type"] = self.properties[":TYPE"]
            del self.properties[":TYPE"]

        for k, v in self.properties.items():
            if isinstance(v, str):
                self.properties[k] = (
                    v.replace(
                        os.linesep,
                        " ",
                    )
                    .replace(
                        "\n",
                        " ",
                    )
                    .replace(
                        "\r",
                        " ",
                    )
                )

            elif isinstance(v, list):
                self.properties[k] = [
                    val.replace(
                        os.linesep,
                        " ",
                    )
                    .replace(
                        "\n",
                        " ",
                    )
                    .replace("\r", " ")
                    for val in v
                ]

    def get_id(self) -> str:
        """
        Returns primary node identifier.

        Returns:
            str: node_id
        """
        return self.node_id

    def get_label(self) -> str:
        """
        Returns primary node label.

        Returns:
            str: node_label
        """
        return self.node_label

    def get_type(self) -> str:
        """
        Returns primary node label.

        Returns:
            str: node_label
        """
        return self.node_label

    def get_preferred_id(self) -> str:
        """
        Returns preferred id.

        Returns:
            str: preferred_id
        """
        return self.preferred_id

    def get_properties(self) -> dict:
        """
        Returns all other node properties apart from primary id and
        label as key-value pairs.

        Returns:
            dict: properties
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
            "node_id": self.node_id,
            "node_label": self.node_label,
            "properties": self.properties,
        }


@dataclass(frozen=True)
class BioCypherEdge:
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

        source_id (string): consensus "best" id for biological entity

        target_id (string): consensus "best" id for biological entity

        relationship_label (string): type of interaction, UPPERCASE

        properties (dict): collection of all other properties of the
        respective edge

    """

    source_id: str
    target_id: str
    relationship_label: str
    relationship_id: str = None
    properties: dict = field(default_factory=dict)

    def __post_init__(self):
        """
        Check for reserved keywords.
        """

        if ":TYPE" in self.properties.keys():
            logger.debug(
                "Keyword ':TYPE' is reserved for Neo4j. Removing from properties.",
                # "Renaming to 'type'."
            )
            # self.properties["type"] = self.properties[":TYPE"]
            del self.properties[":TYPE"]
        elif "id" in self.properties.keys():
            logger.debug(
                "Keyword 'id' is reserved for Neo4j. Removing from properties.",
                # "Renaming to 'type'."
            )
            # self.properties["type"] = self.properties[":TYPE"]
            del self.properties["id"]
        elif "_ID" in self.properties.keys():
            logger.debug(
                "Keyword '_ID' is reserved for Postgres. Removing from properties.",
                # "Renaming to 'type'."
            )
            # self.properties["type"] = self.properties[":TYPE"]
            del self.properties["_ID"]

    def get_id(self) -> Union[str, None]:
        """
        Returns primary node identifier or None.

        Returns:
            str: node_id
        """

        return self.relationship_id

    def get_source_id(self) -> str:
        """
        Returns primary node identifier of relationship source.

        Returns:
            str: source_id
        """
        return self.source_id

    def get_target_id(self) -> str:
        """
        Returns primary node identifier of relationship target.

        Returns:
            str: target_id
        """
        return self.target_id

    def get_label(self) -> str:
        """
        Returns relationship label.

        Returns:
            str: relationship_label
        """
        return self.relationship_label

    def get_type(self) -> str:
        """
        Returns relationship label.

        Returns:
            str: relationship_label
        """
        return self.relationship_label

    def get_properties(self) -> dict:
        """
        Returns all other relationship properties apart from primary ids
        and label as key-value pairs.

        Returns:
            dict: properties
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
            "relationship_id": self.relationship_id or None,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relationship_label": self.relationship_label,
            "properties": self.properties,
        }


@dataclass(frozen=True)
class BioCypherRelAsNode:
    """
    Class to represent relationships as nodes (with in- and outgoing
    edges) as a triplet of a BioCypherNode and two BioCypherEdges. Main
    usage in type checking (instances where the receiving function needs
    to check whether it receives a relationship as a single edge or as
    a triplet).

    Args:

        node (BioCypherNode): node representing the relationship

        source_edge (BioCypherEdge): edge representing the source of the
            relationship

        target_edge (BioCypherEdge): edge representing the target of the
            relationship

    """

    node: BioCypherNode
    source_edge: BioCypherEdge
    target_edge: BioCypherEdge

    def __post_init__(self):
        if not isinstance(self.node, BioCypherNode):
            raise TypeError(
                f"BioCypherRelAsNode.node must be a BioCypherNode, " f"not {type(self.node)}.",
            )

        if not isinstance(self.source_edge, BioCypherEdge):
            raise TypeError(
                f"BioCypherRelAsNode.source_edge must be a BioCypherEdge, " f"not {type(self.source_edge)}.",
            )

        if not isinstance(self.target_edge, BioCypherEdge):
            raise TypeError(
                f"BioCypherRelAsNode.target_edge must be a BioCypherEdge, " f"not {type(self.target_edge)}.",
            )

    def get_node(self) -> BioCypherNode:
        return self.node

    def get_source_edge(self) -> BioCypherEdge:
        return self.source_edge

    def get_target_edge(self) -> BioCypherEdge:
        return self.target_edge
