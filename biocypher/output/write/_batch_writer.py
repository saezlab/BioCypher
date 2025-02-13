"""Abstract base class for all batch writers."""

import glob
import os
import re

from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from types import GeneratorType

from more_itertools import peekable

from biocypher._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from biocypher._deduplicate import Deduplicator
from biocypher._logger import logger
from biocypher._translate import Translator
from biocypher.output.write._writer import _Writer


class _BatchWriter(_Writer, ABC):
    """Abstract batch writer class."""

    @abstractmethod
    def _quote_string(self, value: str) -> str:
        """Quote a string.

        Escaping is handled by the database-specific writer.
        """
        msg = "Database writer must override '_quote_string'"
        logger.error(msg)
        raise NotImplementedError(msg)

    @abstractmethod
    def _get_default_import_call_bin_prefix(self):
        """Provide the default string for the import call bin prefix.

        Returns
        -------
            str: The database-specific string for the path to the import call bin prefix

        """
        msg = "Database writer must override '_get_default_import_call_bin_prefix'"
        logger.error(msg)
        raise NotImplementedError(msg)

    @abstractmethod
    def _write_array_string(self, string_list):
        """Write the string representation of an array into a .csv file.

        Different databases require different formats of array to optimize
        import speed.

        Args:
        ----
            string_list (list): list of ontology strings

        Returns:
        -------
            str: The database-specific string representation of an array

        """
        msg = "Database writer must override '_write_array_string'"
        logger.error(msg)
        raise NotImplementedError(msg)

    @abstractmethod
    def _write_node_headers(self):
        """Write header files for nodes.

        Write header files (node properties) for nodes as per the
        definition in the `schema_config.yaml`.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        msg = "Database writer must override '_write_node_headers'"
        logger.error(msg)
        raise NotImplementedError(msg)

    @abstractmethod
    def _write_edge_headers(self):
        """Write a database import-file for an edge.

        Write a database import-file for an edge as per the definition in
        the `schema_config.yaml`, containing only the header for this type
        of edge.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        msg = "Database writer must override '_write_edge_headers'"
        logger.error(msg)
        raise NotImplementedError(msg)

    @abstractmethod
    def _construct_import_call(self) -> str:
        """Construct the import call.

        Construct the import call detailing folder and individual node and
        edge headers and data files, as well as delimiters and database name.
        Built after all data has been processed to ensure that nodes are
        called before any edges.

        Returns
        -------
            str: A bash command for csv import.

        """
        msg = "Database writer must override '_construct_import_call'"
        logger.error(msg)
        raise NotImplementedError(msg)

    @abstractmethod
    def _get_import_script_name(self) -> str:
        """Return the name of the import script.

        The name will be chosen based on the used database.

        Returns
        -------
            str: The name of the import script (ending in .sh)

        """
        msg = "Database writer must override '_get_import_script_name'"
        logger.error(msg)
        raise NotImplementedError(msg)

    def __init__(
        self,
        translator: "Translator",
        deduplicator: "Deduplicator",
        delimiter: str,
        array_delimiter: str = ",",
        quote: str = '"',
        output_directory: str | None = None,
        db_name: str = "neo4j",
        import_call_bin_prefix: str | None = None,
        import_call_file_prefix: str | None = None,
        wipe: bool = True,
        strict_mode: bool = False,
        skip_bad_relationships: bool = False,
        skip_duplicate_nodes: bool = False,
        db_user: str = None,
        db_password: str = None,
        db_host: str = None,
        db_port: str = None,
        rdf_format: str = None,
        rdf_namespaces: dict = {},
        labels_order: str = "Ascending",
        **kwargs,
    ):
        """Write node and edge representations to disk.

        Abstract parent class for writing node and edge representations to disk
        using the format specified by each database type. The database-specific
        functions are implemented by the respective child-classes. This abstract
        class contains all methods expected by a bach writer instance, some of
        which need to be overwritten by the child classes.

        Each batch writer instance has a fixed representation that needs to be
        passed at instantiation via the :py:attr:`schema` argument. The instance
        also expects an ontology adapter via :py:attr:`ontology_adapter` to be
        able to convert and extend the hierarchy.

        Requires the following methods to be overwritten by database-specific
        writer classes:

            - _write_node_headers
            - _write_edge_headers
            - _construct_import_call
            - _write_array_string
            - _get_import_script_name

        Args:
        ----
            translator:
                Instance of :py:class:`Translator` to enable translation of
                nodes and manipulation of properties.

            deduplicator:
                Instance of :py:class:`Deduplicator` to enable deduplication
                of nodes and edges.

            delimiter:
                The delimiter to use for the CSV files.

            array_delimiter:
                The delimiter to use for array properties.

            quote:
                The quote character to use for the CSV files.

            output_directory:
                Path for exporting CSV files.

            db_name:
                Name of the database that will be used in the generated
                commands.

            import_call_bin_prefix:
                Path prefix for the admin import call binary.

            import_call_file_prefix:
                Path prefix for the data files (headers and parts) in the import
                call.

            wipe:
                Whether to force import (removing existing DB content).
                    (Specific to Neo4j.)

            strict_mode:
                Whether to enforce source, version, and license properties.

            skip_bad_relationships:
                Whether to skip relationships that do not have a valid
                start and end node. (Specific to Neo4j.)

            skip_duplicate_nodes:
                Whether to skip duplicate nodes. (Specific to Neo4j.)

            db_user:
                The database user.

            db_password:
                The database password.

            db_host:
                The database host. Defaults to localhost.

            db_port:
                The database port.

            rdf_format:
                The format of RDF.

            rdf_namespaces:
                The namespaces for RDF.

            labels_order:
                The order of labels, to reflect the hierarchy (or not).
                Default: "Ascending" (from more specific to more generic).

        """
        super().__init__(
            translator=translator,
            deduplicator=deduplicator,
            output_directory=output_directory,
            strict_mode=strict_mode,
        )
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host or "localhost"
        self.db_port = db_port
        self.rdf_format = rdf_format
        self.rdf_namespaces = rdf_namespaces

        self.delim, self.escaped_delim = self._process_delimiter(delimiter)
        self.adelim, self.escaped_adelim = self._process_delimiter(array_delimiter)
        self.quote = quote
        self.skip_bad_relationships = skip_bad_relationships
        self.skip_duplicate_nodes = skip_duplicate_nodes

        if import_call_bin_prefix is None:
            self.import_call_bin_prefix = self._get_default_import_call_bin_prefix()
        else:
            self.import_call_bin_prefix = import_call_bin_prefix

        self.wipe = wipe
        self.strict_mode = strict_mode

        self.translator = translator
        self.deduplicator = deduplicator
        self.node_property_dict = {}
        self.edge_property_dict = {}
        self.import_call_nodes = set()
        self.import_call_edges = set()

        self.outdir = output_directory

        self._import_call_file_prefix = import_call_file_prefix

        self.parts = {}  # dict to store the paths of part files for each label

        self._labels_orders = ["Alphabetical", "Ascending", "Descending", "Leaves"]
        if labels_order not in self._labels_orders:
            msg = (
                f"neo4j's 'labels_order' parameter cannot be '{labels_order}',"
                "must be one of: {' ,'.join(self._labels_orders)}",
            )
            raise ValueError(msg)
        self.labels_order = labels_order

        # TODO not memory efficient, but should be fine for most cases; is
        # there a more elegant solution?

    @property
    def import_call_file_prefix(self):
        """Property for output directory path."""
        if self._import_call_file_prefix is None:
            return self.outdir
        else:
            return self._import_call_file_prefix

    def _process_delimiter(self, delimiter: str) -> str:
        """Process a delimited to escape correctly.

        Args:
        ----
            delimiter (str): The delimiter to process.

        Returns:
        -------
            tuple: The delimiter and its escaped representation.

        """
        if delimiter == "\\t":
            return "\t", "\\t"

        else:
            return delimiter, delimiter

    def write_nodes(self, nodes, batch_size: int = int(1e6), force: bool = False):
        """Write nodes and their headers.

        Args:
        ----
            nodes (BioCypherNode): a list or generator of nodes in
                :py:class:`BioCypherNode` format

            batch_size (int): The batch size for writing nodes.

            force (bool): Whether to force writing nodes even if their type is
                not present in the schema.


        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        # TODO check represented_as

        # write node data
        passed = self._write_node_data(nodes, batch_size, force)
        if not passed:
            logger.error("Error while writing node data.")
            return False
        # pass property data to header writer per node type written
        passed = self._write_node_headers()
        if not passed:
            logger.error("Error while writing node headers.")
            return False

        return True

    def write_edges(
        self,
        edges: list | GeneratorType,
        batch_size: int = int(1e6),
    ) -> bool:
        """Write edges and their headers.

        Args:
        ----
            edges (BioCypherEdge): a list or generator of edges in
                :py:class:`BioCypherEdge` or :py:class:`BioCypherRelAsNode`
                format

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        passed = False
        edges = list(edges)  # force evaluation to handle empty generator
        if edges:
            nodes_flat = []
            edges_flat = []
            for edge in edges:
                if isinstance(edge, BioCypherRelAsNode):
                    # check if relationship has already been written, if so skip
                    if self.deduplicator.rel_as_node_seen(edge):
                        continue

                    nodes_flat.append(edge.get_node())
                    edges_flat.append(edge.get_source_edge())
                    edges_flat.append(edge.get_target_edge())

                else:
                    # check if relationship has already been written, if so skip
                    if self.deduplicator.edge_seen(edge):
                        continue

                    edges_flat.append(edge)

            if nodes_flat and edges_flat:
                passed = self.write_nodes(nodes_flat) and self._write_edge_data(
                    edges_flat,
                    batch_size,
                )
            else:
                passed = self._write_edge_data(edges_flat, batch_size)

        else:
            # is this a problem? if the generator or list is empty, we
            # don't write anything.
            logger.debug(
                "No edges to write, possibly due to no matched Biolink classes.",
            )

        if not passed:
            logger.error("Error while writing edge data.")
            return False
        # pass property data to header writer per edge type written
        passed = self._write_edge_headers()
        if not passed:
            logger.error("Error while writing edge headers.")
            return False

        return True

    def _write_node_data(self, nodes, batch_size, force: bool = False):
        """Write biocypher nodes to CSV.

        Conforms to the headers created with `_write_node_headers()`, and
        is actually required to be run before calling `_write_node_headers()`
        to set the :py:attr:`self.node_property_dict` for passing the node
        properties to the instance. Expects list or generator of nodes from
        the :py:class:`BioCypherNode` class.

        Args:
        ----
            nodes (BioCypherNode): a list or generator of nodes in
                :py:class:`BioCypherNode` format

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        if isinstance(nodes, GeneratorType | peekable):
            logger.debug("Writing node CSV from generator.")

            bins = defaultdict(list)  # dict to store a list for each
            # label that is passed in
            bin_l = {}  # dict to store the length of each list for
            # batching cutoff
            reference_props = defaultdict(
                dict,
            )  # dict to store a dict of properties
            # for each label to check for consistency and their type
            # for now, relevant for `int`
            labels = {}  # dict to store the additional labels for each
            # primary graph constituent from biolink hierarchy
            for node in nodes:
                # check if node has already been written, if so skip
                if self.deduplicator.node_seen(node):
                    continue

                _id = node.get_id()
                label = node.get_label()

                # check for non-id
                if not _id:
                    logger.warning(f"Node {label} has no id; skipping.")
                    continue

                if label not in bins.keys():
                    # start new list
                    all_labels = None
                    bins[label].append(node)
                    bin_l[label] = 1

                    # get properties from config if present
                    if label in self.translator.ontology.mapping.extended_schema:
                        cprops = self.translator.ontology.mapping.extended_schema.get(label).get(
                            "properties",
                        )
                    else:
                        cprops = None
                    if cprops:
                        d = dict(cprops)

                        # add id and preferred id to properties; these are
                        # created in node creation (`_create.BioCypherNode`)
                        d["id"] = "str"
                        d["preferred_id"] = "str"

                        # add strict mode properties
                        if self.strict_mode:
                            d["source"] = "str"
                            d["version"] = "str"
                            d["licence"] = "str"

                    else:
                        d = dict(node.get_properties())
                        # encode property type
                        for k, v in d.items():
                            if d[k] is not None:
                                d[k] = type(v).__name__
                    # else use first encountered node to define properties for
                    # checking; could later be by checking all nodes but much
                    # more complicated, particularly involving batch writing
                    # (would require "do-overs"). for now, we output a warning
                    # if node properties diverge from reference properties (in
                    # write_single_node_list_to_file) TODO if it occurs, ask
                    # user to select desired properties and restart the process

                    reference_props[label] = d

                    # get label hierarchy
                    # multiple labels:
                    if not force:
                        all_labels = self.translator.ontology.get_ancestors(label)
                    else:
                        all_labels = None

                    if all_labels:
                        # convert to pascal case
                        all_labels = [self.translator.name_sentence_to_pascal(label) for label in all_labels]
                        # remove duplicates
                        all_labels = list(OrderedDict.fromkeys(all_labels))
                        match self.labels_order:
                            case "Ascending":
                                pass  # Default from get_ancestors.
                            case "Alphabetical":
                                all_labels.sort()
                            case "Descending":
                                all_labels.reverse()
                            case "Leaves":
                                if len(all_labels) < 1:
                                    msg = "Labels list cannot be empty when using 'Leaves' order."
                                    raise ValueError(msg)
                                all_labels = [all_labels[0]]
                            case _:
                                # In case someone touched _label_orders after constructor.
                                if self.labels_order not in self._labels_orders:
                                    msg = (
                                        f"Invalid labels_order: {self.labels_order}. "
                                        f"Must be one of {self._labels_orders}"
                                    )
                                    raise ValueError(msg)
                        # concatenate with array delimiter
                        all_labels = self._write_array_string(all_labels)
                    else:
                        all_labels = self.translator.name_sentence_to_pascal(label)

                    labels[label] = all_labels

                else:
                    # add to list
                    bins[label].append(node)
                    bin_l[label] += 1
                    if not bin_l[label] < batch_size:
                        # batch size controlled here
                        passed = self._write_single_node_list_to_file(
                            bins[label],
                            label,
                            reference_props[label],
                            labels[label],
                        )

                        if not passed:
                            return False

                        bins[label] = []
                        bin_l[label] = 0

            # after generator depleted, write remainder of bins
            for label, nl in bins.items():
                passed = self._write_single_node_list_to_file(
                    nl,
                    label,
                    reference_props[label],
                    labels[label],
                )

                if not passed:
                    return False

            # use complete bin list to write header files
            # TODO if a node type has varying properties
            # (ie missingness), we'd need to collect all possible
            # properties in the generator pass

            # save config or first-node properties to instance attribute
            for label in reference_props.keys():
                self.node_property_dict[label] = reference_props[label]

            return True
        elif not isinstance(nodes, list):
            logger.error("Nodes must be passed as list or generator.")
            return False
        else:

            def gen(nodes):
                yield from nodes

            return self._write_node_data(gen(nodes), batch_size=batch_size)

    def _write_single_node_list_to_file(
        self,
        node_list: list,
        label: str,
        prop_dict: dict,
        labels: str,
    ):
        """Write a list of biocypher nodes to a CSV file.

        This function takes one list of biocypher nodes and writes them
        to a Neo4j admin import compatible CSV file.

        Args:
        ----
            node_list (list): list of BioCypherNodes to be written
            label (str): the primary label of the node
            prop_dict (dict): properties of node class passed from parsing
                function and their types
            labels (str): string of one or several concatenated labels
                for the node class

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        if not all(isinstance(n, BioCypherNode) for n in node_list):
            logger.error("Nodes must be passed as type BioCypherNode.")
            return False

        # from list of nodes to list of strings
        lines = []

        for n in node_list:
            # check for deviations in properties
            # node properties
            n_props = n.get_properties()
            n_keys = list(n_props.keys())
            # reference properties
            ref_props = list(prop_dict.keys())

            # compare lists order invariant
            if set(ref_props) != set(n_keys):
                onode = n.get_id()
                oprop1 = set(ref_props).difference(n_keys)
                oprop2 = set(n_keys).difference(ref_props)
                logger.error(
                    f"At least one node of the class {n.get_label()} "
                    f"has more or fewer properties than another. "
                    f"Offending node: {onode!r}, offending property: "
                    f"{max([oprop1, oprop2])}. "
                    f"All reference properties: {ref_props}, "
                    f"All node properties: {n_keys}.",
                )
                return False

            line = [n.get_id()]

            if ref_props:
                plist = []
                # make all into strings, put actual strings in quotes
                for k, v in prop_dict.items():
                    p = n_props.get(k)
                    if p is None:  # TODO make field empty instead of ""?
                        plist.append("")
                    elif v in [
                        "int",
                        "integer",
                        "long",
                        "float",
                        "double",
                        "dbl",
                        "bool",
                        "boolean",
                    ]:
                        plist.append(str(p))
                    elif isinstance(p, list):
                        plist.append(self._write_array_string(p))
                    else:
                        plist.append(f"{self.quote}{p!s}{self.quote}")

                line.append(self.delim.join(plist))
            line.append(labels)

            lines.append(self.delim.join(line) + "\n")

        # avoid writing empty files
        if lines:
            self._write_next_part(label, lines)

        return True

    def _write_edge_data(self, edges, batch_size):
        """Write biocypher edges to CSV.

        Writes biocypher edges to CSV conforming to the headers created
        with `_write_edge_headers()`, and is actually required to be run
        before calling `_write_node_headers()` to set the
        :py:attr:`self.edge_property_dict` for passing the edge
        properties to the instance. Expects list or generator of edges
        from the :py:class:`BioCypherEdge` class.

        Args:
        ----
            edges (BioCypherEdge): a list or generator of edges in
                :py:class:`BioCypherEdge` format

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        Todo:
        ----
            - currently works for mixed edges but in practice often is
              called on one iterable containing one type of edge only

        """
        if isinstance(edges, GeneratorType):
            logger.debug("Writing edge CSV from generator.")

            bins = defaultdict(list)  # dict to store a list for each
            # label that is passed in
            bin_l = {}  # dict to store the length of each list for
            # batching cutoff
            reference_props = defaultdict(
                dict,
            )  # dict to store a dict of properties
            # for each label to check for consistency and their type
            # for now, relevant for `int`
            for edge in edges:
                if not (edge.get_source_id() and edge.get_target_id()):
                    logger.error(
                        f"Edge must have source and target node. Caused by: {edge}",
                    )
                    continue

                label = edge.get_label()

                if label not in bins.keys():
                    # start new list
                    bins[label].append(edge)
                    bin_l[label] = 1

                    # get properties from config if present

                    # check whether label is in ontology_adapter.leaves
                    # (may not be if it is an edge that carries the
                    # "label_as_edge" property)
                    cprops = None
                    if label in self.translator.ontology.mapping.extended_schema:
                        cprops = self.translator.ontology.mapping.extended_schema.get(label).get(
                            "properties",
                        )
                    else:
                        # try via "label_as_edge"
                        for (
                            k,
                            v,
                        ) in self.translator.ontology.mapping.extended_schema.items():
                            if isinstance(v, dict):
                                if v.get("label_as_edge") == label:
                                    cprops = v.get("properties")
                                    break
                    if cprops:
                        d = cprops

                        # add strict mode properties
                        if self.strict_mode:
                            d["source"] = "str"
                            d["version"] = "str"
                            d["licence"] = "str"

                    else:
                        d = dict(edge.get_properties())
                        # encode property type
                        for k, v in d.items():
                            if d[k] is not None:
                                d[k] = type(v).__name__
                    # else use first encountered edge to define
                    # properties for checking; could later be by
                    # checking all edges but much more complicated,
                    # particularly involving batch writing (would
                    # require "do-overs"). for now, we output a warning
                    # if edge properties diverge from reference
                    # properties (in write_single_edge_list_to_file)
                    # TODO

                    reference_props[label] = d

                else:
                    # add to list
                    bins[label].append(edge)
                    bin_l[label] += 1
                    if not bin_l[label] < batch_size:
                        # batch size controlled here
                        passed = self._write_single_edge_list_to_file(
                            bins[label],
                            label,
                            reference_props[label],
                        )

                        if not passed:
                            return False

                        bins[label] = []
                        bin_l[label] = 0

            # after generator depleted, write remainder of bins
            for label, nl in bins.items():
                passed = self._write_single_edge_list_to_file(
                    nl,
                    label,
                    reference_props[label],
                )

                if not passed:
                    return False

            # use complete bin list to write header files
            # TODO if a edge type has varying properties
            # (ie missingness), we'd need to collect all possible
            # properties in the generator pass

            # save first-edge properties to instance attribute
            for label in reference_props.keys():
                self.edge_property_dict[label] = reference_props[label]

            return True
        elif not isinstance(edges, list):
            logger.error("Edges must be passed as list or generator.")
            return False
        else:

            def gen(edges):
                yield from edges

            return self._write_edge_data(gen(edges), batch_size=batch_size)

    def _write_single_edge_list_to_file(
        self,
        edge_list: list,
        label: str,
        prop_dict: dict,
    ):
        """Write a list of biocypher edges to a CSV file.

        This function takes one list of biocypher edges and writes them
        to a Neo4j admin import compatible CSV file.

        Args:
        ----
            edge_list (list): list of BioCypherEdges to be written

            label (str): the label (type) of the edge

            prop_dict (dict): properties of node class passed from parsing
                function and their types

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        if not all(isinstance(n, BioCypherEdge) for n in edge_list):
            logger.error("Edges must be passed as type BioCypherEdge.")
            return False

        # from list of edges to list of strings
        lines = []
        for e in edge_list:
            # check for deviations in properties
            # edge properties
            e_props = e.get_properties()
            e_keys = list(e_props.keys())
            ref_props = list(prop_dict.keys())

            # compare list order invariant
            if set(ref_props) != set(e_keys):
                oedge = f"{e.get_source_id()}-{e.get_target_id()}"
                oprop1 = set(ref_props).difference(e_keys)
                oprop2 = set(e_keys).difference(ref_props)
                logger.error(
                    f"At least one edge of the class {e.get_label()} "
                    f"has more or fewer properties than another. "
                    f"Offending edge: {oedge!r}, offending property: "
                    f"{max([oprop1, oprop2])}. "
                    f"All reference properties: {ref_props}, "
                    f"All edge properties: {e_keys}.",
                )
                return False

            plist = []
            # make all into strings, put actual strings in quotes
            for k, v in prop_dict.items():
                p = e_props.get(k)
                if p is None:  # TODO make field empty instead of ""?
                    plist.append("")
                elif v in [
                    "int",
                    "integer",
                    "long",
                    "float",
                    "double",
                    "dbl",
                    "bool",
                    "boolean",
                ]:
                    plist.append(str(p))
                elif isinstance(p, list):
                    plist.append(self._write_array_string(p))
                else:
                    plist.append(self.quote + str(p) + self.quote)

            entries = [e.get_source_id()]

            skip_id = False
            schema_label = None

            if label in ["IS_SOURCE_OF", "IS_TARGET_OF", "IS_PART_OF"]:
                skip_id = True
            elif not self.translator.ontology.mapping.extended_schema.get(label):
                # find label in schema by label_as_edge
                for (
                    k,
                    v,
                ) in self.translator.ontology.mapping.extended_schema.items():
                    if v.get("label_as_edge") == label:
                        schema_label = k
                        break
            else:
                schema_label = label

            if schema_label:
                if (
                    self.translator.ontology.mapping.extended_schema.get(
                        schema_label,
                    ).get("use_id")
                    == False  # noqa: E712 (seems to not work with 'not')
                ):
                    skip_id = True

            if not skip_id:
                entries.append(e.get_id() or "")

            if ref_props:
                entries.append(self.delim.join(plist))

            entries.append(e.get_target_id())
            entries.append(
                self.translator.name_sentence_to_pascal(
                    e.get_label(),
                ),
            )

            lines.append(
                self.delim.join(entries) + "\n",
            )

        # avoid writing empty files
        if lines:
            self._write_next_part(label, lines)

        return True

    def _write_next_part(self, label: str, lines: list):
        """Write a list of strings to a new part file.

        Args:
        ----
            label (str): the label (type) of the edge; internal
            representation sentence case -> needs to become PascalCase
            for disk representation

            lines (list): list of strings to be written

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        # translate label to PascalCase
        label_pascal = self.translator.name_sentence_to_pascal(parse_label(label))

        # list files in self.outdir
        files = glob.glob(os.path.join(self.outdir, f"{label_pascal}-part*.csv"))
        # find file with highest part number
        if not files:
            next_part = 0

        else:
            next_part = (
                max(
                    [int(f.split(".")[-2].split("-")[-1].replace("part", "")) for f in files],
                )
                + 1
            )

        # write to file
        padded_part = str(next_part).zfill(3)
        logger.info(
            f"Writing {len(lines)} entries to {label_pascal}-part{padded_part}.csv",
        )

        # store name only in case import_call_file_prefix is set
        part = f"{label_pascal}-part{padded_part}.csv"
        file_path = os.path.join(self.outdir, part)

        with open(file_path, "w", encoding="utf-8") as f:
            # concatenate with delimiter
            f.writelines(lines)

        if not self.parts.get(label):
            self.parts[label] = [part]
        else:
            self.parts[label].append(part)

    def get_import_call(self) -> str:
        """Eeturn the import call.

        Return the import call detailing folder and individual node and
        edge headers and data files, as well as delimiters and database name.

        Returns
        -------
            str: a bash command for the database import

        """
        return self._construct_import_call()

    def write_import_call(self) -> str:
        """Write the import call.

        Function to write the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name, to the export folder as txt.

        Returns
        -------
            str: The path of the file holding the import call.

        """
        file_path = os.path.join(self.outdir, self._get_import_script_name())
        logger.info(f"Writing {self.db_name + ' ' if self.db_name else ''}import call to `{file_path}`.")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(self._construct_import_call())

        return file_path


def parse_label(label: str) -> str:
    """Check if the label is compliant with Neo4j naming conventions.

    Check against https://neo4j.com/docs/cypher-manual/current/syntax/naming/,
    and if not compliant, remove non-compliant characters.

    Args:
    ----
        label (str): The label to check
    Returns:
        str: The compliant label

    """
    # Check if the name contains only alphanumeric characters, underscore, or dollar sign
    # and dot (for class hierarchy of BioCypher)
    allowed_chars = r"a-zA-Z0-9_$ ."
    matches = re.findall(f"[{allowed_chars}]", label)
    non_matches = re.findall(f"[^{allowed_chars}]", label)
    if non_matches:
        non_matches = list(set(non_matches))
        logger.warning(
            f"Label is not compliant with Neo4j naming rules. Removed non compliant characters: {non_matches}",
        )

    def first_character_compliant(character: str) -> bool:
        return character.isalpha() or character == "$"

    if not first_character_compliant(matches[0]):
        for c in matches:
            if first_character_compliant(c):
                matches = matches[matches.index(c) :]
                break
        logger.warning("Label does not start with an alphabetic character or with $. Removed non compliant characters.")
    return "".join(matches).strip()
