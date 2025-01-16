"""BioCypher 'offline' module. Handles the writing of node and edge representations
suitable for import into a DBMS.
"""

import os

from types import GeneratorType

from rdflib import DC, DCTERMS, RDF, RDFS, SKOS, Graph, Literal, Namespace
from rdflib.namespace import (
    _NAMESPACE_PREFIXES_CORE,
    _NAMESPACE_PREFIXES_RDFLIB,
)

from biocypher._create import BioCypherEdge, BioCypherNode
from biocypher._logger import logger
from biocypher.output.write._batch_writer import _BatchWriter


class _RDFWriter(_BatchWriter):
    """Class to write BioCypher's property graph into an RDF format using
    rdflib and all the extensions it supports (RDF/XML, N3, NTriples,
    N-Quads, Turtle, TriX, Trig and JSON-LD). By default the conversion
    is done keeping only the minimum information about node and edges,
    skipping all properties.
    """

    def _get_import_script_name(self) -> str:
        """Returns the name of the RDF admin import script.
        This function applicable for RDF export.

        Returns
        -------
            str: The name of the import script (ending in .sh)

        """
        return "rdf-import-call.sh"

    def _get_default_import_call_bin_prefix(self):
        """Method to provide the default string for the import call bin prefix.

        Returns
        -------
            str: The default location for the RDF admin import location

        """
        return "bin/"

    def _is_rdf_format_supported(self, rdf_format: str) -> bool:
        """Function to check if the specified RDF format is supported.

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
        """This function takes one list of biocypher edges and writes them
        to an RDF file with the given format.

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
            edge_uri = self.rdf_namespaces["biocypher"][edge_label]
            graph.add((edge_uri, RDF.type, RDFS.Class))
            graph.add(
                (
                    self.rdf_namespaces["biocypher"][rdf_predicate],
                    RDF.type,
                    edge_uri,
                ),
            )
            graph.add(
                (
                    self.rdf_namespaces["biocypher"][rdf_predicate],
                    self.rdf_namespaces["biocypher"]["subject"],
                    self.subject_to_uri(rdf_subject),
                ),
            )
            graph.add(
                (
                    self.rdf_namespaces["biocypher"][rdf_predicate],
                    self.rdf_namespaces["biocypher"]["object"],
                    self.subject_to_uri(rdf_object),
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
                        self.subject_to_uri(rdf_subject),
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
                        self.subject_to_uri(rdf_subject),
                        self.property_to_uri(rdf_predicate),
                        Literal(rdf_object),
                    ),
                )
        else:
            graph.add(
                (
                    self.subject_to_uri(rdf_subject),
                    self.property_to_uri(rdf_predicate),
                    Literal(rdf_object),
                ),
            )

    def transform_string_to_list(self, string_list: str) -> list:
        """Function to transform a string representation of a list into a list.

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
        """This function takes a list of BioCypherNodes and writes them
        to an RDF file in the specified format.

        Args:
        ----
            node_list (list): A list of BioCypherNodes to be written.

            label (str): The label (type) of the nodes.

            prop_dict (dict): A dictionary of properties and their types for the node class.

        Returns:
        -------
            bool: True if the writing is successful, False otherwise.

        """
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
                    self.rdf_namespaces["biocypher"][class_name],
                    RDF.type,
                    RDFS.Class,
                ),
            )
            graph.add(
                (
                    self.subject_to_uri(rdf_subject),
                    RDF.type,
                    self.rdf_namespaces["biocypher"][class_name],
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
        """Wrapper for writing nodes in RDF format. It calls the _write_node_data() function, specifying the node data.

        Args:
        ----
            nodes (list or generator): A list or generator of nodes in BioCypherNode format.
            batch_size (int): The number of nodes to write in each batch.
            force (bool): Flag to force the writing even if the output file already exists.

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
        """Wrapper for writing edges in RDF format. It calls _write_edge_data()
        functions specifying it's edge data.

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
        """Function to write the import call.
        This function is not applicable for RDF.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        return ""

    def _write_array_string(self, string_list):
        """Abstract method to write the string representation of an array into a .csv file
        as required by the RDF admin-import.
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
        """Abstract method that takes care of importing properties of a graph entity that is represented
        as a node as per the definition in the `schema_config.yaml`
        This function is not applicable for RDF.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        return True

    def _write_edge_headers(self):
        """Abstract method to write a database import-file for a graph entity that is represented
        as an edge as per the definition in the `schema_config.yaml`,
        containing only the header for this type of edge.
        This function is not applicable for RDF.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        return True

    def subject_to_uri(self, subject: str) -> str:
        """Converts the subject to a proper URI using the available namespaces.
        If the conversion fails, it defaults to the biocypher prefix.

        Args:
        ----
            subject (str): The subject to be converted to a URI.

        Returns:
        -------
            str: The corresponding URI for the subject.

        """
        try:
            _pref, _id = subject.split(":")

            if _pref in self.rdf_namespaces.keys():
                return self.rdf_namespaces[_pref][_id]
            else:
                return self.rdf_namespaces["biocypher"][subject]
        except ValueError:
            return self.rdf_namespaces["biocypher"][subject]

    def property_to_uri(self, property_name: str) -> dict[str, str]:
        """Converts a property name to its corresponding URI.

        This function takes a property name and searches for its corresponding URI in various namespaces.
        It first checks the core namespaces for rdflib, including owl, rdf, rdfs, xsd, and xml.

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
        return self.rdf_namespaces["biocypher"][property_name]

    def _init_namespaces(self, graph: Graph):
        """Initialise the namespaces for the RDF graph.

        These namespaces are used to convert nodes to URIs. This function adds
        the biocypher standard namespace to the `rdf_namespaces` attribute of
        the class. If `rdf_namespaces` is empty, it sets it to the biocypher
        standard namespace. Otherwise, it merges the biocypher standard
        namespace with the namespaces defined in the biocypher_config.yaml.

        Args:
        ----
            graph (RDFLib.Graph): The RDF graph to bind the namespaces to.

        Returns:
        -------
            None

        """
        # add biocypher standard to self.rdf_namespaces
        biocypher_standard = {"biocypher": "https://biocypher.org/biocypher#"}
        if not self.rdf_namespaces:
            self.rdf_namespaces = biocypher_standard
        else:
            self.rdf_namespaces = self.rdf_namespaces | biocypher_standard

        for key, value in self.rdf_namespaces.items():
            namespace = Namespace(value)
            self.rdf_namespaces[key] = namespace
            graph.bind(key, namespace)
