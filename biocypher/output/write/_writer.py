import os

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Optional, Union

from biocypher._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from biocypher._deduplicate import Deduplicator
from biocypher._logger import logger
from biocypher._translate import Translator

__all__ = ["_Writer"]


class _Writer(ABC):
    """Abstract class for writing node and edge representations to disk.
    Specifics of the different writers (e.g. neo4j, postgresql, csv, etc.)
    are implemented in the child classes. Any concrete writer needs to
    implement at least:
    - _write_node_data
    - _write_edge_data
    - _construct_import_call
    - _get_import_script_name

    Args:
        translator (Translator): Instance of :py:class:`Translator` to enable translation of
            nodes and manipulation of properties.
        deduplicator (Deduplicator): Instance of :py:class:`Deduplicator` to enable deduplication
            of nodes and edges.
        output_directory (str, optional): Path for exporting CSV files. Defaults to None.
        strict_mode (bool, optional): Whether to enforce source, version, and license properties. Defaults to False.
    strict_mode (bool, optional): Whether to enforce source, version, and license properties. Defaults to False.

    Raises:
        NotImplementedError: Writer implementation must override '_write_node_data'
        NotImplementedError: Writer implementation must override '_write_edge_data'
        NotImplementedError: Writer implementation must override '_construct_import_call'
        NotImplementedError: Writer implementation must override '_get_import_script_name'
    """

    def __init__(
        self,
        translator: Translator,
        deduplicator: Deduplicator,
        output_directory: Optional[str] = None,
        strict_mode: bool = False,
        *args,
        **kwargs,
    ):
        """Abstract class for writing node and edge representations to disk.

        Args:
            translator (Translator): Instance of :py:class:`Translator` to enable translation of
                nodes and manipulation of properties.
            deduplicator (Deduplicator): Instance of :py:class:`Deduplicator` to enable deduplication
                of nodes and edges.
            output_directory (str, optional): Path for exporting CSV files. Defaults to None.
            strict_mode (bool, optional): Whether to enforce source, version, and license properties. Defaults to False.
        strict_mode (bool, optional): Whether to enforce source, version, and license properties. Defaults to False.
        """
        self.translator = translator
        self.deduplicator = deduplicator
        self.strict_mode = strict_mode
        self.output_directory = output_directory

        if os.path.exists(self.output_directory):
            if kwargs.get("write_to_file", True):
                logger.warning(
                    f"Output directory `{self.output_directory}` already exists. "
                    "If this is not planned, file consistency may be compromised."
                )
        else:
            logger.info(f"Creating output directory `{self.output_directory}`.")
            os.makedirs(self.output_directory)

    @abstractmethod
    def _write_node_data(
        self,
        nodes: Iterable[Union[BioCypherNode, BioCypherEdge, BioCypherRelAsNode]],
    ) -> bool:
        """Implement how to output.write nodes to disk.

        Args:
            nodes (Iterable): An iterable of BioCypherNode / BioCypherEdge / BioCypherRelAsNode objects.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        raise NotImplementedError("Writer implementation must override 'write_nodes'")

    @abstractmethod
    def _write_edge_data(
        self,
        edges: Iterable[Union[BioCypherNode, BioCypherEdge, BioCypherRelAsNode]],
    ) -> bool:
        """Implement how to output.write edges to disk.

        Args:
            edges (Iterable): An iterable of BioCypherNode / BioCypherEdge / BioCypherRelAsNode objects.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        raise NotImplementedError("Writer implementation must override 'write_edges'")

    @abstractmethod
    def _construct_import_call(self) -> str:
        """
        Function to construct the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name. Built after all data has been
        processed to ensure that nodes are called before any edges.

        Returns:
            str: command for importing the output files into a DBMS.
        """
        raise NotImplementedError("Writer implementation must override '_construct_import_call'")

    @abstractmethod
    def _get_import_script_name(self) -> str:
        """Returns the name of the import script.

        Returns:
            str: The name of the import script (ending in .sh)
        """
        raise NotImplementedError("Writer implementation must override '_get_import_script_name'")

    def write_nodes(self, nodes, batch_size: int = int(1e6), force: bool = False):
        """Wrapper for writing nodes.

        Args:
            nodes (BioCypherNode): a list or generator of nodes in
                :py:class:`BioCypherNode` format
            batch_size (int): The batch size for writing nodes.
            force (bool): Whether to force writing nodes even if their type is
                not present in the schema.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        passed = self._write_node_data(nodes)
        if not passed:
            logger.error("Error while writing node data.")
            return False
        return True

    def write_edges(self, edges, batch_size: int = int(1e6), force: bool = False):
        """Wrapper for writing edges.

        Args:
            nodes (BioCypherNode): a list or generator of nodes in
                :py:class:`BioCypherNode` format
            batch_size (int): The batch size for writing nodes.
            force (bool): Whether to force writing nodes even if their type is
                not present in the schema.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        passed = self._write_edge_data(edges)
        if not passed:
            logger.error("Error while writing edge data.")
            return False
        return True

    def write_import_call(self):
        """
        Function to output.write the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name, to the export folder as txt.

        Returns:
            str: The path of the file holding the import call.
        """
        file_path = os.path.join(self.output_directory, self._get_import_script_name())
        logger.info(f"Writing {self.__class__.__name__} import call to `{file_path}`.")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(self._construct_import_call())

        return file_path
