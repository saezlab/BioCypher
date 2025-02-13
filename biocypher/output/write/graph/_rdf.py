"""Module to provide the RDF writer class."""

import os

from types import GeneratorType

from rdflib import (
    DC,
    DCTERMS,
    RDF,
    RDFS,
    SKOS,
    Graph,
    Literal,
    Namespace,
    URIRef,
)
from rdflib.namespace import (
    _NAMESPACE_PREFIXES_CORE,
    _NAMESPACE_PREFIXES_RDFLIB,
)

from biocypher._create import BioCypherEdge, BioCypherNode
from biocypher._deduplicate import Deduplicator
from biocypher._logger import logger
from biocypher._translate import Translator
from biocypher.output.write._batch_writer import _BatchWriter


class _RDFWriter(_BatchWriter):
    """Write BioCypher's property graph into an RDF format.

    Uses `rdflib` and all the extensions it supports (RDF/XML, N3, NTriples,
    N-Quads, Turtle, TriX, Trig and JSON-LD). By default the conversion
    is done keeping only the minimum information about node and edges,
    skipping all properties.
    """

    def __init__(
        self,
        translator: Translator,
        deduplicator: Deduplicator,
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
        super().__init__(
            translator=translator,
            deduplicator=deduplicator,
            delimiter=delimiter,
            array_delimiter=array_delimiter,
            quote=quote,
            output_directory=output_directory,
            db_name=db_name,
            import_call_bin_prefix=import_call_bin_prefix,
            import_call_file_prefix=import_call_file_prefix,
            wipe=wipe,
            strict_mode=strict_mode,
            skip_bad_relationships=skip_bad_relationships,
            skip_duplicate_nodes=skip_duplicate_nodes,
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port,
            rdf_format=rdf_format,
            rdf_namespaces=rdf_namespaces,
            labels_order=labels_order,
        )
        if not self.rdf_namespaces:
            # For some reason, the config can pass
            # the None object.
            self.rdf_namespaces = {}

        self.namespaces = {}

    def _get_import_script_name(self) -> str:
        """Return the name of the RDF admin import script.

        This function is used for RDF export.

        Returns
        -------
            str: The name of the import script (ending in .sh)

        """
        return "rdf-import-call.sh"

    def _get_default_import_call_bin_prefix(self):
        """Provide the default string for the import call bin prefix.

        Returns
        -------
            str: The default location for the RDF admin import location

        """
        return "bin/"

    def _is_rdf_format_supported(self, rdf_format: str) -> bool:
        """Check if the specified RDF format is supported.

        Args:
        ----
            rdf_format (str): The RDF format to check.

        Returns:
        -------
            bool: Returns True if rdf format supported, False otherwise.

        """
        supported_formats = [
            "xml",
            "n3",
            "turtle",
            "nt",
            "pretty-xml",
            "trix",
            "trig",
            "nquads",
            "json-ld",
        ]
        if rdf_format not in supported_formats:
            logger.error(
                f"{rdf_format}; Incorrect or unsupported RDF format, use one of the following: "
                f'"xml", "n3", "turtle", "nt", "pretty-xml", "trix", "trig", "nquads", "json-ld" ',
            )
            return False
        else:
            # RDF graph does not support 'ttl' format, only 'turtle' format.
            # however, the preferred file extension is always '.ttl'
            if self.rdf_format == "turtle":
                self.extension = "ttl"
            elif self.rdf_format == "ttl":
                self.rdf_format = "turtle"
                self.extension = "ttl"
            else:
                self.extension = self.rdf_format
            return True

    def _write_single_edge_list_to_file(
        self,
        edge_list: list,
        label: str,
        prop_dict: dict,
    ):
        """Write a list of BioCypherEdges to an RDF file.

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

        # NOTE: prop_dict is not used.

        if not all(isinstance(n, BioCypherEdge) for n in edge_list):
            logger.error("Edges must be passed as type BioCypherEdge.")
            return False

        # translate label to PascalCase
        label_pascal = self.translator.name_sentence_to_pascal(label)

        # create file name
        file_name = os.path.join(self.outdir, f"{label_pascal}.{self.extension}")

        # write data in graph
        graph = Graph()
        self._init_namespaces(graph)

        for edge in edge_list:
            rdf_subject = edge.get_source_id()
            rdf_object = edge.get_target_id()
            rdf_predicate = edge.get_id()
            rdf_properties = edge.get_properties()
            if rdf_predicate is None:
                rdf_predicate = rdf_subject + rdf_object

            edge_label = self.translator.name_sentence_to_pascal(edge.get_label())
            edge_uri = self.as_uri(edge_label, "biocypher")
            graph.add((edge_uri, RDF.type, RDFS.Class))
            graph.add(
                (
                    self.as_uri(rdf_predicate, "biocypher"),
                    RDF.type,
                    edge_uri,
                ),
            )
            graph.add(
                (
                    self.as_uri(rdf_predicate, "biocypher"),
                    self.as_uri("subject", "biocypher"),
                    self.to_uri(rdf_subject),
                ),
            )
            graph.add(
                (
                    self.as_uri(rdf_predicate, "biocypher"),
                    self.as_uri("object", "biocypher"),
                    self.to_uri(rdf_object),
                ),
            )

            # add properties to the transformed edge --> node
            for key, value in rdf_properties.items():
                # only write value if it exists.
                if value:
                    self.add_property_to_graph(graph, rdf_predicate, value, key)

        graph.serialize(destination=file_name, format=self.rdf_format)

        logger.info(
            f"Writing {len(edge_list)} entries to {label_pascal}.{self.rdf_format}",
        )

        return True

    def add_property_to_graph(
        self,
        graph: Graph,
        rdf_subject: str,
        rdf_object: str,
        rdf_predicate: str,
    ):
        """Add the properties to an RDF node.

        It takes the graph, the subject, object, and predicate of the RDF
        triple. It checks if the property is a list and adds it to the graph
        accordingly. Otherwise it checks if the string represents a list. If it
        does, it transforms it to a list and adds it to the graph. If not, it
        adds the property to the graph as a literal. If the property is neither
        a list or string, it will also be added as a literal.

        Args:
        ----
            graph (RDFLib.Graph): The RDF graph to add the nodes to.

            rdf_subject (str): The subject of the RDF triple.

            rdf_object (str): The object of the RDF triple.

            rdf_predicate (str): The predicate of the RDF triple.

        Returns:
        -------
            None

        """
        if isinstance(rdf_object, list):
            for obj in rdf_object:
                graph.add(
                    (
                        self.to_uri(rdf_subject),
                        self.property_to_uri(rdf_predicate),
                        Literal(obj),
                    ),
                )
        elif isinstance(rdf_object, str):
            if rdf_object.startswith("[") and rdf_object.endswith("]"):
                self.add_property_to_graph(
                    graph,
                    rdf_subject,
                    self.transform_string_to_list(rdf_object),
                    rdf_predicate,
                )
            else:
                graph.add(
                    (
                        self.to_uri(rdf_subject),
                        self.property_to_uri(rdf_predicate),
                        Literal(rdf_object),
                    ),
                )
        else:
            graph.add(
                (
                    self.to_uri(rdf_subject),
                    self.property_to_uri(rdf_predicate),
                    Literal(rdf_object),
                ),
            )

    def transform_string_to_list(self, string_list: str) -> list:
        """Transform a string representation of a list into a list.

        Args:
        ----
            string_list (str): The string representation of the list.

        Returns:
        -------
            list: The list representation of the input string.

        """
        return string_list.replace("[", "").replace("]", "").replace("'", "").split(", ")

    def _write_single_node_list_to_file(
        self,
        node_list: list,
        label: str,
        prop_dict: dict,
        labels: str,
    ):
        """Write a list of BioCypherNodes to an RDF file.

        Args:
        ----
            node_list (list): A list of BioCypherNodes to be written.

            label (str): The label (type) of the nodes.

            prop_dict (dict): A dictionary of properties and their types for the node class.

            labels (str): string of one or several concatenated labels

        Returns:
        -------
            bool: True if the writing is successful, False otherwise.

        """

        # NOTE: labels and prop_dict are not used.

        if not all(isinstance(n, BioCypherNode) for n in node_list):
            logger.error("Nodes must be passed as type BioCypherNode.")
            return False

        # translate label to PascalCase
        label_pascal = self.translator.name_sentence_to_pascal(label)

        # create file name
        file_name = os.path.join(self.outdir, f"{label_pascal}.{self.extension}")

        # write data in graph
        graph = Graph()
        self._init_namespaces(graph)

        for n in node_list:
            rdf_subject = n.get_id()
            rdf_object = n.get_label()
            properties = n.get_properties()
            class_name = self.translator.name_sentence_to_pascal(rdf_object)
            graph.add(
                (
                    self.as_uri(class_name, "biocypher"),
                    RDF.type,
                    RDFS.Class,
                ),
            )
            graph.add(
                (
                    self.to_uri(rdf_subject),
                    RDF.type,
                    self.as_uri(class_name, "biocypher"),
                ),
            )
            for key, value in properties.items():
                # only write value if it exists.
                if value:
                    self.add_property_to_graph(graph, rdf_subject, value, key)

        graph.serialize(destination=file_name, format=self.rdf_format)

        logger.info(
            f"Writing {len(node_list)} entries to {label_pascal}.{self.rdf_format}",
        )

        return True

    def write_nodes(self, nodes, batch_size: int = int(1e6), force: bool = False) -> bool:
        """Write nodes in RDF format.

        Args:
        ----
            nodes (list or generator): A list or generator of nodes in
                BioCypherNode format.
            batch_size (int): The number of nodes to write in each batch.
            force (bool): Flag to force the writing even if the output file
                already exists.

        Returns:
        -------
            bool: True if the writing is successful, False otherwise.

        """
        # check if specified output format is correct
        passed = self._is_rdf_format_supported(self.rdf_format)
        if not passed:
            logger.error("Error while writing node data, wrong RDF format")
            return False
        # write node data using _write_node_data method
        passed = self._write_node_data(nodes, batch_size, force)
        if not passed:
            logger.error("Error while writing node data.")
            return False
        return True

    def write_edges(
        self,
        edges: list | GeneratorType,
        batch_size: int = int(1e6),
    ) -> bool:
        """Write edges in RDF format.

        Args:
        ----
            edges (BioCypherEdge): a list or generator of edges in
                :py:class:`BioCypherEdge` format
            batch_size (int): The number of edges to write in each batch.

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        # check if specified output format is correct
        passed = self._is_rdf_format_supported(self.rdf_format)
        if not passed:
            logger.error("Error while writing edge data, wrong RDF format")
            return False
        # write edge data using _write_edge_data method
        passed = self._write_edge_data(edges, batch_size=batch_size)
        if not passed:
            logger.error("Error while writing edge data.")
            return False

        return True

    def _construct_import_call(self) -> bool:
        """Write the import call.

        This function is not applicable for RDF.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        return ""

    def _quote_string(self, value: str) -> str:
        """Quote a string."""
        return f"{self.quote}{value}{self.quote}"

    def _write_array_string(self, string_list):
        """Write the string representation of an array into a .csv file.

        This function is not applicable for RDF.

        Args:
        ----
            string_list (list): list of ontology strings

        Returns:
        -------
            str: The string representation of an array for the neo4j admin import

        """
        return True

    def _write_node_headers(self):
        """Import properties of a graph entity.

        This function is not applicable for RDF.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        return True

    def _write_edge_headers(self):
        """Write a database import-file for a graph entity.

        This function is not applicable for RDF.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        return True

    def as_uri(self, name: str, namespace: str = "") -> str:
        """Return an RDFlib object with the given namespace as a URI.

        There is often a default for empty namespaces, which would have been
        loaded with the ontology, and put in `self.namespace` by
        `self._init_namespaces`.

        Args:
        ----
            name (str): The name to be transformed.
            namespace (str): The namespace to be used.

        Returns:
        -------
            str: The URI for the given name and namespace.

        """
        if namespace in self.namespaces:
            return URIRef(self.namespaces[namespace][name])
        else:
            assert "biocypher" in self.namespaces
            # If no default empty NS, use the biocypher one,
            # which is always there.
            logger.debug(f"I'll consider '{name}' as part of 'biocypher' namespace.")
            return URIRef(self.namespaces["biocypher"][name])

    def to_uri(self, subject: str) -> str:
        """Extract the namespace from the given subject.

        Split the subject's string on ":". Then convert the subject to a
        proper URI, if the namespace is known. If namespace is unknown,
        defaults to the default prefix of the ontology.

        Args:
        ----
            subject (str): The subject to be converted to a URI.

        Returns:
        -------
            str: The corresponding URI for the subject.

        """
        pref_id = subject.split(":")
        if len(pref_id) == 2:
            pref, id = pref_id
            return self.as_uri(id, pref)
        else:
            return self.as_uri(subject)

    def find_uri(self, regexp: str) -> str:
        query = f'SELECT DISTINCT ?s WHERE {{ ?s ?p ?o . FILTER regex(str(?s), "{regexp}")}}'
        gen = self.graph.query(query)
        uris = list(gen)
        if len(uris) > 1:
            logger.warning(
                f"Found several terms matching `{regexp}`, I will consider only the first one: `{uris[0][0]}`",
            )
            logger.debug("\tothers:")
            for u in uris[1:]:
                logger.debug(f"\t{u[0]}")
        if uris:
            logger.debug(f"Found {len(uris)} terms, returning: `{uris[0][0]}`")
            return uris[0][0]
        else:
            logger.debug(f"Found no term matching: `{query}`")
            return None

    def property_to_uri(self, property_name: str) -> dict[str, str]:
        """Convert a property name to its corresponding URI.

        This function takes a property name and searches for its corresponding
        URI in various namespaces. It first checks the core namespaces for
        rdflib, including owl, rdf, rdfs, xsd, and xml.

        Args:
        ----
            property_name (str): The property name to be converted to a URI.

        Returns:
        -------
            str: The corresponding URI for the input property name.

        """
        # These namespaces are core for rdflib; owl, rdf, rdfs, xsd and xml
        for namespace in _NAMESPACE_PREFIXES_CORE.values():
            if property_name in namespace:
                return namespace[property_name]

        # If the property name is not found in the core namespaces, search in
        # the SKOS, DC, and DCTERMS namespaces
        for namespace in [SKOS, DC, DCTERMS]:
            if property_name in namespace:
                return namespace[property_name]

        # If the property name is still not found, try other namespaces from
        # rdflib.
        for namespace in _NAMESPACE_PREFIXES_RDFLIB.values():
            if property_name in namespace:
                return namespace[property_name]

        # If the property name is "licence", it recursively calls the function
        # with "license" as the input.
        if property_name == "licence":
            return self.property_to_uri("license")

        # TODO: add an option to search trough manually implemented namespaces

        # If the input is not found in any of the namespaces, it returns
        # the corresponding URI from the biocypher namespace.
        # TODO: give a warning and try to prevent this option altogether
        return self.as_uri(property_name, "biocypher")

    def _init_namespaces(self, graph: Graph):
        """Initialise the namespaces for the RDF graph.

        This function adds the biocypher standard namespace to the `namespaces`
        attribute of the class. If `namespaces` is empty, it sets it to the
        biocypher standard namespace. Otherwise, it merges the biocypher
        standard namespace with the namespaces defined in the
        biocypher_config.yaml.

        Args:
        ----
            graph (RDFLib.Graph): The RDF graph to bind the namespaces to.

        Returns:
        -------
            None

        """
        # Bind and keep the biocypher namespace.
        bcns = Namespace("https://biocypher.org/biocypher#")
        bck = "biocypher"
        self.namespaces = {bck: bcns}
        graph.bind(bck, bcns)

        # Keep track of namespaces loaded with the ontologies in the given graph.
        logger.debug("Bind namespaces:")
        for prefix, ns in graph.namespaces():
            if prefix in self.namespaces and str(ns) != str(self.namespaces[prefix]):
                logger.warning(
                    f"Namespace '{prefix}' was already loaded"
                    f"as '{self.namespaces[prefix]}',"
                    f"I will overwrite it with '{ns}'.",
                )
            logger.debug(f"\t'{prefix}'\t=>\t'{ns}'")
            self.namespaces[prefix] = Namespace(ns)

        # Bind and keep the namespaces given in the config.
        for prefix, ns in self.rdf_namespaces.items():
            assert prefix not in self.namespaces
            self.namespaces[prefix] = Namespace(ns)
            logger.debug(f"\t'{prefix}'\t->\t{ns}")
            graph.bind(prefix, self.namespaces[prefix])
