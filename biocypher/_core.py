"""BioCypher core module.

Interfaces with the user and distributes tasks to submodules.
"""

import itertools
import json
import os

from datetime import datetime

import yaml

from ._config import (
    config as _config,
    update_from_file as _file_update,
)
from ._create import BioCypherNode
from ._deduplicate import Deduplicator
from ._get import Downloader
from ._logger import logger
from ._mapping import OntologyMapping
from ._ontology import Ontology
from ._translate import Translator
from .output.connect._get_connector import get_connector
from .output.in_memory._get_in_memory_kg import IN_MEMORY_DBMS, get_in_memory_kg
from .output.write._get_writer import DBMS_TO_CLASS, get_writer

logger.debug(f"Loading module {__name__}.")
__all__ = ["BioCypher"]

SUPPORTED_DBMS = DBMS_TO_CLASS.keys()

REQUIRED_CONFIG = [
    "dbms",
    "offline",
    "strict_mode",
    "head_ontology",
]


class BioCypher:
    """Orchestration of BioCypher operations.

    Instantiate this class to interact with BioCypher.

    Args:
    ----
        dbms (str): The database management system to use. For supported
            systems see SUPPORTED_DBMS.

        offline (bool): Whether to run in offline mode. In offline mode
            the Knowledge Graph is written to files. In online mode, it
            is written to a database or hold in memory.

        strict_mode (bool): Whether to run in strict mode. If True, the
            translator will raise an error if a node or edge does not
            provide source, version, and licence information.

        biocypher_config_path (str): Path to the BioCypher config file.

        schema_config_path (str): Path to the user schema config
            file.

        head_ontology (dict): The head ontology defined by URL ('url') and root
            node ('root_node').

        tail_ontologies (dict): The tail ontologies defined by URL and
            join nodes for both head and tail ontology.

        output_directory (str): Path to the output directory. If not
            provided, the default value 'biocypher-out' will be used.

        cache_directory (str): Path to the cache directory.

    """

    def __init__(
        self,
        dbms: str = None,
        offline: bool = None,
        strict_mode: bool = None,
        biocypher_config_path: str = None,
        schema_config_path: str = None,
        head_ontology: dict = None,
        tail_ontologies: dict = None,
        output_directory: str = None,
        cache_directory: str = None,
        # legacy params
        db_name: str = None,
    ):
        # Update configuration if custom path is provided
        if biocypher_config_path:
            _file_update(biocypher_config_path)

        if db_name:
            logger.warning(
                "The parameter `db_name` is deprecated. Please set the "
                "`database_name` setting in the `biocypher_config.yaml` file "
                "instead.",
            )
            _config(**{db_name: {"database_name": db_name}})

        # Load configuration
        self.base_config = _config("biocypher")

        # Check for required configuration
        for key in REQUIRED_CONFIG:
            if key not in self.base_config:
                msg = f"Configuration key {key} is required."
                raise ValueError(msg)

        # Set configuration - mandatory
        self._dbms = dbms or self.base_config["dbms"]

        if offline is None:
            self._offline = self.base_config["offline"]
        else:
            self._offline = offline

        if strict_mode is None:
            self._strict_mode = self.base_config["strict_mode"]
        else:
            self._strict_mode = strict_mode

        self._schema_config_path = schema_config_path or self.base_config.get(
            "schema_config_path",
        )

        if not self._schema_config_path:
            logger.warning("Running BioCypher without schema configuration.")
        else:
            logger.info(
                f"Running BioCypher with schema configuration from {self._schema_config_path}.",
            )

        self._head_ontology = head_ontology or self.base_config["head_ontology"]

        # Set configuration - optional
        self._output_directory = output_directory or self.base_config.get(
            "output_directory",
        )
        self._cache_directory = cache_directory or self.base_config.get(
            "cache_directory",
        )
        self._tail_ontologies = tail_ontologies or self.base_config.get(
            "tail_ontologies",
        )

        if self._dbms not in SUPPORTED_DBMS:
            msg = f"DBMS {self._dbms} not supported. Please select from {SUPPORTED_DBMS}."
            raise ValueError(msg)

        # Initialize
        self._ontology_mapping = None
        self._deduplicator = None
        self._translator = None
        self._downloader = None
        self._ontology = None
        self._writer = None
        self._driver = None
        self._in_memory_kg = None

        self._in_memory_kg = None
        self._nodes = None
        self._edges = None

    def _initialize_in_memory_kg(self) -> None:
        """Create in-memory KG instance.

        Set as instance variable `self._in_memory_kg`.
        """
        if not self._in_memory_kg:
            self._in_memory_kg = get_in_memory_kg(
                dbms=self._dbms,
                deduplicator=self._get_deduplicator(),
            )

    def add_nodes(self, nodes) -> None:
        """Add new nodes to the internal representation.

        Initially, receive nodes data from adaptor and create internal
        representation for nodes.

        Args:
        ----
            nodes(iterable): An iterable of nodes

        """
        if isinstance(nodes, list):
            self._nodes = list(itertools.chain(self._nodes, nodes))
        else:
            self._nodes = itertools.chain(self._nodes, nodes)

    def add_edges(self, edges) -> None:
        """Add new edges to the internal representation.

        Initially, receive edges data from adaptor and create internal
        representation for edges.

        Args:
        ----
             edges(iterable): An iterable of edges.

        """
        if isinstance(edges, list):
            self._edges = list(itertools.chain(self._edges, edges))
        else:
            self._edges = itertools.chain(self._edges, edges)

    def to_df(self):
        """Create DataFrame using internal representation.

        TODO: to_df implies data frame, should be specifically that use case
        """
        return self._to_KG()

    def to_networkx(self):
        """Create networkx using internal representation."""
        return self._to_KG()

    def _to_KG(self):
        """Convert the internal representation to knowledge graph.

        The knowledge graph is returned based on the `dbms` parameter in
        the biocypher configuration file.

        Returns
        -------
             Any: knowledge graph.

        """
        if not self._in_memory_kg:
            self._initialize_in_memory_kg()
        if not self._translator:
            self._get_translator()
        tnodes = self._translator.translate_entities(self._nodes)
        tedges = self._translator.translate_entities(self._edges)
        self._in_memory_kg.add_nodes(tnodes)
        self._in_memory_kg.add_edges(tedges)
        return self._in_memory_kg.get_kg()

    def _get_deduplicator(self) -> Deduplicator:
        """Create deduplicator if not exists and return."""
        if not self._deduplicator:
            self._deduplicator = Deduplicator()

        return self._deduplicator

    def _get_ontology_mapping(self) -> OntologyMapping:
        """Create ontology mapping if not exists and return."""
        if not self._schema_config_path:
            self._ontology_mapping = OntologyMapping()

        if not self._ontology_mapping:
            self._ontology_mapping = OntologyMapping(
                config_file=self._schema_config_path,
            )

        return self._ontology_mapping

    def _get_ontology(self) -> Ontology:
        """Create ontology if not exists and return."""
        if not self._ontology:
            self._ontology = Ontology(
                ontology_mapping=self._get_ontology_mapping(),
                head_ontology=self._head_ontology,
                tail_ontologies=self._tail_ontologies,
            )

        return self._ontology

    def _get_translator(self) -> Translator:
        """Create translator if not exists and return."""
        if not self._translator:
            self._translator = Translator(
                ontology=self._get_ontology(),
                strict_mode=self._strict_mode,
            )

        return self._translator

    def _get_writer(self):
        """Create writer if not online.

        Set as instance variable `self._writer`.
        """
        if self._offline:

            def timestamp() -> str:
                return datetime.now().strftime("%Y%m%d%H%M%S")

            outdir = self._output_directory or os.path.join(
                "biocypher-out",
                timestamp(),
            )
            self._output_directory = os.path.abspath(outdir)

            self._writer = get_writer(
                dbms=self._dbms,
                translator=self._get_translator(),
                deduplicator=self._get_deduplicator(),
                output_directory=self._output_directory,
                strict_mode=self._strict_mode,
            )
        else:
            msg = "Cannot get writer in online mode."
            raise NotImplementedError(msg)

        return self._writer

    def _get_driver(self):
        """Create driver if not exists.

        Set as instance variable `self._driver`.
        """
        if not self._offline:
            self._driver = get_connector(
                dbms=self._dbms,
                translator=self._get_translator(),
            )
        else:
            msg = "Cannot get driver in offline mode."
            raise NotImplementedError(msg)

        return self._driver

    def _get_in_memory_kg(self):
        """Create in-memory KG instance.

        Set as instance variable `self._in_memory_kg`.
        """
        if not self._in_memory_kg:
            self._in_memory_kg = get_in_memory_kg(
                dbms=self._dbms,
                deduplicator=self._get_deduplicator(),
            )

        return self._in_memory_kg

    def _add_nodes(
        self,
        nodes,
        batch_size: int = int(1e6),
        force: bool = False,
    ):
        """Add nodes to the BioCypher KG.

        First uses the `_translator` to translate the nodes to `BioCypherNode`
        objects. Depending on the configuration the translated nodes are then
        passed to the

        - `_writer`: if `_offline` is set to `False`

        - `_in_memory_kg`: if `_offline` is set to `False` and the `_dbms` is an
            `IN_MEMORY_DBMS`

        - `_driver`: if `_offline` is set to `True` and the `_dbms` is not an
            `IN_MEMORY_DBMS`

        """
        if not self._translator:
            self._get_translator()
        translated_nodes = self._translator.translate_entities(nodes)

        if self._offline:
            passed = self._get_writer().write_nodes(
                translated_nodes,
                batch_size=batch_size,
                force=force,
            )
        elif self._is_online_and_in_memory():
            passed = self._get_in_memory_kg().add_nodes(translated_nodes)
        else:
            passed = self._get_driver().add_biocypher_nodes(translated_nodes)

        return passed

    def _add_edges(self, edges, batch_size: int = int(1e6)):
        """Add edges to the BioCypher KG.

        First uses the `_translator` to translate the edges to `BioCypherEdge`
        objects. Depending on the configuration the translated edges are then
        passed to the

        - `_writer`: if `_offline` is set to `False`

        - `_in_memory_kg`: if `_offline` is set to `False` and the `_dbms` is an
            `IN_MEMORY_DBMS`

        - `_driver`: if `_offline` is set to `True` and the `_dbms` is not an
            `IN_MEMORY_DBMS`

        """
        if not self._translator:
            self._get_translator()
        translated_edges = self._translator.translate_entities(edges)

        if self._offline:
            if not self._writer:
                self._initialize_writer()
            passed = self._writer.write_edges(
                translated_edges,
                batch_size=batch_size,
            )
        elif self._is_online_and_in_memory():
            if not self._in_memory_kg:
                self._initialize_in_memory_kg()
            passed = self._in_memory_kg.add_edges(translated_edges)
        else:
            if not self._driver:
                self._initialize_driver()
            passed = self._driver.add_biocypher_nodes(translated_edges)

        return passed

    def _is_online_and_in_memory(self) -> bool:
        """Return True if in online mode and in-memory dbms is used."""
        return (not self._offline) & (self._dbms in IN_MEMORY_DBMS)

    def write_nodes(
        self,
        nodes,
        batch_size: int = int(1e6),
        force: bool = False,
    ) -> bool:
        """Write nodes to database.

        Either takes an iterable of tuples (if given, translates to
        ``BioCypherNode`` objects) or an iterable of ``BioCypherNode`` objects.

        Args:
        ----
            nodes (iterable): An iterable of nodes to write to the database.
            batch_size (int): The batch size to use when writing to disk.
            force (bool): Whether to force writing to the output directory even
                if the node type is not present in the schema config file.

        Returns:
        -------
            bool: True if successful.

        """
        return self._add_nodes(nodes, batch_size=batch_size, force=force)

    def write_edges(self, edges, batch_size: int = int(1e6)) -> bool:
        """Write edges to database.

        Either takes an iterable of tuples (if given, translates to
        ``BioCypherEdge`` objects) or an iterable of ``BioCypherEdge`` objects.

        Args:
        ----
            edges (iterable): An iterable of edges to write to the database.

        Returns:
        -------
            bool: True if successful.

        """
        return self._add_edges(edges, batch_size=batch_size)

    def add(self, entities) -> None:
        """Add entities to the in-memory database.

        Accepts an iterable of tuples (if given, translates to
        ``BioCypherNode`` or ``BioCypherEdge`` objects) or an iterable of
        ``BioCypherNode`` or ``BioCypherEdge`` objects.

        Args:
        ----
            entities (iterable): An iterable of entities to add to the database.
                Can be 3-tuples (nodes) or 5-tuples (edges); also accepts
                4-tuples for edges (deprecated).

        Returns:
        -------
            None

        """
        return self._add_nodes(entities)

    def merge_nodes(self, nodes) -> bool:
        """Merge nodes into database.

        Either takes an iterable of tuples (if given, translates to
        ``BioCypherNode`` objects) or an iterable of ``BioCypherNode`` objects.

        Args:
        ----
            nodes (iterable): An iterable of nodes to merge into the database.

        Returns:
        -------
            bool: True if successful.

        """
        return self._add_nodes(nodes)

    def merge_edges(self, edges) -> bool:
        """Merge edges into database.

        Either takes an iterable of tuples (if given, translates to
        ``BioCypherEdge`` objects) or an iterable of ``BioCypherEdge`` objects.

        Args:
        ----
            edges (iterable): An iterable of edges to merge into the database.

        Returns:
        -------
            bool: True if successful.

        """
        return self._add_edges(edges)

    def get_kg(self):
        """Get the in-memory KG instance.

        Depending on the specified `dbms` this could either be a list of Pandas
        dataframes or a NetworkX DiGraph.
        """
        if not self._is_online_and_in_memory():
            msg = (f"Getting the in-memory KG is only available in online mode for {IN_MEMORY_DBMS}.",)
            raise ValueError(msg)
        if not self._in_memory_kg:
            msg = "No in-memory KG instance found. Please call `add()` first."
            raise ValueError(msg)

        if not self._in_memory_kg:
            self._initialize_in_memory_kg()
        return self._in_memory_kg.get_kg()

    # DOWNLOAD AND CACHE MANAGEMENT METHODS ###

    def _get_downloader(self, cache_dir: str | None = None):
        """Create downloader if not exists."""
        if not self._downloader:
            self._downloader = Downloader(self._cache_directory)

    def download(self, *resources) -> None:
        """Download or load from cache the resources given by the adapter.

        Args:
        ----
            resources (iterable): An iterable of resources to download or load
                from cache.

        Returns:
        -------
            None

        """
        self._get_downloader()
        return self._downloader.download(*resources)

    # OVERVIEW AND CONVENIENCE METHODS ###

    def log_missing_input_labels(self) -> dict[str, list[str]] | None:
        """Log missing input labels.

        Get the set of input labels encountered without an entry in the
        `schema_config.yaml` and print them to the logger.

        Returns
        -------
            Optional[Dict[str, List[str]]]: A dictionary of Biolink types
            encountered without an entry in the `schema_config.yaml` file.

        """
        mt = self._translator.get_missing_biolink_types()

        if mt:
            msg = (
                "Input entities not accounted for due to them not being "
                f"present in the schema configuration file {self._schema_config_path} "
                "(this is not necessarily a problem, if you did not intend "
                "to include them in the database; see the log for details): \n"
            )
            for k, v in mt.items():
                msg += f"    {k}: {v} \n"

            logger.info(msg)
            return mt

        else:
            logger.info("No missing labels in input.")
            return None

    def log_duplicates(self) -> None:
        """Log duplicate nodes and edges.

        Get the set of duplicate nodes and edges encountered and print them to
        the logger.
        """
        dn = self._deduplicator.get_duplicate_nodes()

        if dn:
            ntypes = dn[0]
            nids = dn[1]

            msg = "Duplicate node types encountered (IDs in log): \n"
            for typ in ntypes:
                msg += f"    {typ}\n"

            logger.info(msg)

            idmsg = "Duplicate node IDs encountered: \n"
            for _id in nids:
                idmsg += f"    {_id}\n"

            logger.debug(idmsg)

        else:
            logger.info("No duplicate nodes in input.")

        de = self._deduplicator.get_duplicate_edges()

        if de:
            etypes = de[0]
            eids = de[1]

            msg = "Duplicate edge types encountered (IDs in log): \n"
            for typ in etypes:
                msg += f"    {typ}\n"

            logger.info(msg)

            idmsg = "Duplicate edge IDs encountered: \n"
            for _id in eids:
                idmsg += f"    {_id}\n"

            logger.debug(idmsg)

        else:
            logger.info("No duplicate edges in input.")

    def show_ontology_structure(self, **kwargs) -> None:
        """Show the ontology structure using treelib or write to GRAPHML file.

        Args:
        ----
            to_disk (str): If specified, the ontology structure will be saved
                to disk as a GRAPHML file, to be opened in your favourite
                graph visualisation tool.

            full (bool): If True, the full ontology structure will be shown,
                including all nodes and edges. If False, only the nodes and
                edges that are relevant to the extended schema will be shown.

        """
        if not self._ontology:
            self._get_ontology()

        return self._ontology.show_ontology_structure(**kwargs)

    def write_import_call(self) -> str:
        """Write a shell script to import the database.

        Shell script is written depending on the chosen DBMS.

        Returns
        -------
            str: path toward the file holding the import call.

        """
        if not self._offline:
            msg = "Cannot write import call in online mode."
            raise NotImplementedError(msg)

        return self._writer.write_import_call()

    def write_schema_info(self, as_node: bool = False) -> None:
        """Write an extended schema info to file or node.

        Creates a YAML file or KG node that extends the `schema_config.yaml`
        with run-time information of the built KG. For instance, include
        information on whether something present in the actual knowledge graph,
        whether it is a relationship (which is important in the case of
        representing relationships as nodes) and the actual sources and
        targets of edges. Since this file can be used in place of the original
        `schema_config.yaml` file, it indicates that it is the extended schema
        by setting `is_schema_info` to `true`.

        We start by using the `extended_schema` dictionary from the ontology
        class instance, which contains all expanded entities and relationships.
        The information of whether something is a relationship can be gathered
        from the deduplicator instance, which keeps track of all entities that
        have been seen.

        Args:
        ----
            as_node (bool): If True, the schema info is written as a KG node.
                If False, the schema info is written to a YAML file.

        """
        if (not self._offline) and self._dbms not in IN_MEMORY_DBMS:
            msg = "Cannot write schema info in online mode."
            raise NotImplementedError(msg)

        ontology = self._get_ontology()
        schema = ontology.mapping.extended_schema.copy()
        schema["is_schema_info"] = True

        deduplicator = self._get_deduplicator()
        for node in deduplicator.entity_types:
            if node in schema:
                schema[node]["present_in_knowledge_graph"] = True
                schema[node]["is_relationship"] = False
            else:
                logger.info(
                    f"Node {node} not present in extended schema. Skipping schema info.",
                )

        # find 'label_as_edge' cases in schema entries
        changed_labels = {}
        for k, v in schema.items():
            if not isinstance(v, dict):
                continue
            if "label_as_edge" in v:
                if v["label_as_edge"] in deduplicator.seen_relationships:
                    changed_labels[v["label_as_edge"]] = k

        for edge in deduplicator.seen_relationships:
            if edge in changed_labels:
                edge = changed_labels[edge]
            if edge in schema:
                schema[edge]["present_in_knowledge_graph"] = True
                schema[edge]["is_relationship"] = True
                # TODO information about source and target nodes
            else:
                logger.info(
                    f"Edge {edge} not present in extended schema. Skipping schema info.",
                )

        # write to output directory as YAML file
        path = os.path.join(self._output_directory, "schema_info.yaml")
        with open(path, "w") as f:
            f.write(yaml.dump(schema))

        if as_node:
            # write as node
            node = BioCypherNode(
                node_id="schema_info",
                node_label="schema_info",
                properties={"schema_info": json.dumps(schema)},
            )
            self.write_nodes([node], force=True)

            # override import call with added schema info node
            self.write_import_call()

        return schema

    # TRANSLATION METHODS ###

    def translate_term(self, term: str) -> str:
        """Translate a term to its BioCypher equivalent.

        Args:
        ----
            term (str): The term to translate.

        Returns:
        -------
            str: The BioCypher equivalent of the term.

        """
        # instantiate adapter if not exists
        self.start_ontology()

        return self._translator.translate_term(term)

    def summary(self) -> None:
        """Call convenience and reporting methods.

        Shows ontology structure and logs duplicates and missing input types.
        """
        self.show_ontology_structure()
        self.log_duplicates()
        self.log_missing_input_labels()

    def reverse_translate_term(self, term: str) -> str:
        """Reverse translate a term from its BioCypher equivalent.

        Args:
        ----
            term (str): The BioCypher term to reverse translate.

        Returns:
        -------
            str: The original term.

        """
        # instantiate adapter if not exists
        self.start_ontology()

        return self._translator.reverse_translate_term(term)

    def translate_query(self, query: str) -> str:
        """Translate a query to its BioCypher equivalent.

        Args:
        ----
            query (str): The query to translate.

        Returns:
        -------
            str: The BioCypher equivalent of the query.

        """
        # instantiate adapter if not exists
        self.start_ontology()

        return self._translator.translate(query)

    def reverse_translate_query(self, query: str) -> str:
        """Reverse translate a query from its BioCypher equivalent.

        Args:
        ----
            query (str): The BioCypher query to reverse translate.

        Returns:
        -------
            str: The original query.

        """
        # instantiate adapter if not exists
        self.start_ontology()

        return self._translator.reverse_translate(query)
