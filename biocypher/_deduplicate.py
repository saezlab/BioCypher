from ._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from ._logger import logger

logger.debug(f"Loading module {__name__}.")


class Deduplicator:
    """
    Singleton class responsible of deduplicating BioCypher inputs. Maintains
    sets/dictionaries of node and edge types and their unique identifiers.

    Nodes identifiers should be globally unique (represented as a set), while
    edge identifiers are only unique per edge type (represented as a dict of
    sets, keyed by edge type).

    Stores collection of duplicate node and edge identifiers and types for
    troubleshooting and to avoid overloading the log.
    """

    def __init__(self):
        self.seen_entity_ids = set()
        self.duplicate_entity_ids = set()

        self.entity_types = set()
        self.duplicate_entity_types = set()

        self.seen_relationships = {}
        self.duplicate_relationship_ids = set()
        self.duplicate_relationship_types = set()

    def node_seen(self, entity: BioCypherNode) -> bool:
        """
        Adds a node to the instance and checks if it has been seen before.

        Args:
            node: BioCypherNode to be added.

        Returns:
            True if the node has been seen before, False otherwise.
        """
        if entity.get_label() not in self.entity_types:
            self.entity_types.add(entity.get_label())

        if entity.get_id() in self.seen_entity_ids:
            self.duplicate_entity_ids.add(entity.get_id())
            if entity.get_label() not in self.duplicate_entity_types:
                logger.warning(f"Duplicate node type {entity.get_label()} found. ")
                self.duplicate_entity_types.add(entity.get_label())
            return True

        self.seen_entity_ids.add(entity.get_id())
        return False

    def edge_seen(self, relationship: BioCypherEdge) -> bool:
        """
        Adds an edge to the instance and checks if it has been seen before.

        Args:
            edge: BioCypherEdge to be added.

        Returns:
            True if the edge has been seen before, False otherwise.
        """
        if relationship.get_type() not in self.seen_relationships:
            self.seen_relationships[relationship.get_type()] = set()

        # concatenate source and target if no id is present
        if not relationship.get_id():
            _id = f"{relationship.get_source_id()}_{relationship.get_target_id()}"
        else:
            _id = relationship.get_id()

        if _id in self.seen_relationships[relationship.get_type()]:
            self.duplicate_relationship_ids.add(_id)
            if relationship.get_type() not in self.duplicate_relationship_types:
                logger.warning(f"Duplicate edge type {relationship.get_type()} found. ")
                self.duplicate_relationship_types.add(relationship.get_type())
            return True

        self.seen_relationships[relationship.get_type()].add(_id)
        return False

    def rel_as_node_seen(self, rel_as_node: BioCypherRelAsNode) -> bool:
        """
        Adds a rel_as_node to the instance (one entity and two relationships)
        and checks if it has been seen before. Only the node is relevant for
        identifying the rel_as_node as a duplicate.

        Args:
            rel_as_node: BioCypherRelAsNode to be added.

        Returns:
            True if the rel_as_node has been seen before, False otherwise.
        """
        node = rel_as_node.get_node()

        if node.get_label() not in self.seen_relationships:
            self.seen_relationships[node.get_label()] = set()

        # rel as node always has an id
        _id = node.get_id()

        if _id in self.seen_relationships[node.get_type()]:
            self.duplicate_relationship_ids.add(_id)
            if node.get_type() not in self.duplicate_relationship_types:
                logger.warning(f"Duplicate edge type {node.get_type()} found. ")
                self.duplicate_relationship_types.add(node.get_type())
            return True

        self.seen_relationships[node.get_type()].add(_id)
        return False

    def get_duplicate_nodes(self):
        """
        Function to return a list of duplicate nodes.

        Returns:
            list: list of duplicate nodes
        """

        if self.duplicate_entity_types:
            return (self.duplicate_entity_types, self.duplicate_entity_ids)
        else:
            return None

    def get_duplicate_edges(self):
        """
        Function to return a list of duplicate edges.

        Returns:
            list: list of duplicate edges
        """

        if self.duplicate_relationship_types:
            return (
                self.duplicate_relationship_types,
                self.duplicate_relationship_ids,
            )
        else:
            return None
