from abc import ABC, abstractmethod


class _InMemoryKG(ABC):
    """Abstract class for handling the in-memory Knowledge Graph instance.
    Specifics of the different in-memory implementations (e.g. csv, networkx)
    are implemented in the child classes. Any concrete in-memory implementation
    needs to implement at least:
    - add_nodes
    - add_edges
    - get_kg

    Raises:
        NotImplementedError: InMemoryKG implementation must override 'add_nodes'
        NotImplementedError: InMemoryKG implementation must override 'add_edges'
        NotImplementedError: InMemoryKG implementation must override 'get_kg'
    """

    @abstractmethod
    def add_nodes(self, nodes):
        """Add nodes to the in-memory knowledge graph.

        Args:
            nodes (Iterable[BioCypherNode]): Iterable of BioCypherNode objects.
        """
        raise NotImplementedError("InMemoryKG implementation must override 'add_nodes'")

    @abstractmethod
    def add_edges(self, edges):
        """Add edges to the in-memory knowledge graph.

        Args:
            edges (Iterable[BioCypherEdge]): Iterable of BioCypherEdge objects.
        """
        raise NotImplementedError("InMemoryKG implementation must override 'add_edges'")

    @abstractmethod
    def get_kg(self):
        """Return the in-memory knowledge graph."""
        raise NotImplementedError("InMemoryKG implementation must override 'get_kg'")
