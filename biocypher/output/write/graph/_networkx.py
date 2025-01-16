import pickle

from biocypher._logger import logger
from biocypher.output.in_memory._networkx import NetworkxKG
from biocypher.output.write._writer import _Writer


class _NetworkXWriter(_Writer):
    """
    Class for writing the in-memory networkx DiGraph to file.

    Call `_construct_import_call` to write the networkx DiGraph to a pickle
    file and return the Python call to load it.

    TODO: this is a non-intuitive name, should be adjusted.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_memory_networkx_kg = NetworkxKG(
            deduplicator=self.deduplicator,
        )

    def _construct_import_call(self) -> str:
        """Dump networkx graph to a pickle file and return Python call.

        Returns:
            str: Python code to load the networkx graph from a pickle file.
        """
        self.G = self.in_memory_networkx_kg._create_networkx_kg()
        logger.info(f"Writing networkx {self.G} to pickle file networkx_graph.pkl.")
        with open(f"{self.output_directory}/networkx_graph.pkl", "wb") as f:
            pickle.dump(self.G, f)

        import_call = "import pickle\n"
        import_call += "with open('./networkx_graph.pkl', 'rb') as f:\n\tG_loaded = pickle.load(f)"
        return import_call

    def _get_import_script_name(self) -> str:
        """Function to return the name of the import script."""
        return "import_networkx.py"

    def _write_node_data(self, nodes) -> bool:
        """Add nodes to the networkx graph.

        TODO: this is not strictly writing, should be refactored.

        Args:
            nodes (list): List of nodes to add to the networkx graph.

        Returns:
            bool: True if the nodes were added successfully, False otherwise.
        """
        passed = self.in_memory_networkx_kg.add_nodes(nodes)
        return passed

    def _write_edge_data(self, edges) -> bool:
        """Add edges to the networkx graph.

        TODO: this is not strictly writing, should be refactored.

        Args:
            edges (list): List of edges to add to the networkx graph.

        Returns:
            bool: True if the edges were added successfully, False otherwise.
        """
        passed = self.in_memory_networkx_kg.add_edges(edges)
        return passed
