#!/usr/bin/env python
#
# Copyright 2021, Heidelberg University Clinic
# Copyright 2025, Institut Pasteur
#
# File author(s):  Loes van den Biggelaar
#                  Sebastian Lobentanzer
#                  Johann Dreo
#
# Distributed under MIT licence, see the file `LICENSE`.
#
"""Module to provide the OWL writer class."""
import os

from types import GeneratorType
from urllib.parse import quote_plus as url_quote

from rdflib import (
    OWL,
    RDF,
    RDFS,
    Literal,
)

from biocypher._create import BioCypherEdge, BioCypherNode
from biocypher._deduplicate import Deduplicator
from biocypher._logger import logger
from biocypher._translate import Translator
from biocypher.output.write.graph._rdf import _RDFWriter


class _OWLWriter(_RDFWriter):
    """Write BioCypher's graph into a self-contained OWL file.

    The resulting OWL file contains both the input vocabulary and
    the output instances.

    The behavior relies mainly on the `edge_model` parameter,
    which can take two values:

    - "ObjectProperty", which translates BioCypher's edges into
      OWL's object properties (if they are available under the
      selected root term). Object properties are the natural way
      to model edges in OWL, but they do not support annotation,
      thus being incompatible with having BioCypher's properties
      on edges.
      As most OWL files do not model a common term on top of both
      owl:topObjectProperty and owl:Thing, you may need to ensure
      that the input OWL contains a common ancestor honoring both:

      - owl:Thing rdfs:subClassOf <root_node>
      - owl:topObjectProperty rdfs:subPropertyOf <root_node>

      and that you select it in your BioCypher configuration.

    - "Association" (the default), which translates BioCypher's
      edges into OWL's class instances. Those edges instances are
      inserted in between the instances coming from BioCypher's nodes.
      This allows to keep edge properties, but adds OWL instances
      to model relationships, which does not follow the classical
      OWL model. In this approach, all OWL instances are linked
      with a generic "edge_source" (linking source instance to
      the association instance) and "edge_target" (linking the association
      instance to the target instance). Both of which inherit from "edge",
      and are in the biocypher namespace.

    This class takes care of keeping the vocabulary underneath the
    selected root node and exports it along the instances in the
    resulting OWL file. It discards whatever terms are not in the
    tree below the selected root node.

    To output a valid self-contained OWL file, it is required that
    you call *both* `write_nodes` *and* `write_edges`.

    This class heavily relies on the _RDFWriter class interface and code.
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
        edge_model: str = "Association",
        file_stem: str = "biocypher",
        **kwargs,
    ):
        """Initialize the OWL writer.

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

            edge_model:
                Whether to model an edge as OWL's "ObjectProperty" (discards
                edges properties) or "Association" (adds an intermediate node
                that holds the edge properties).

            file_stem:
                The stem (name without the path and extension) of the output
                OWL file. The extension is determined from `rdf_format`.

        """
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

        # Starts with the loaded ontologies RDF graph,
        # so as to keep the declared vocabulary.
        self.graph = self.translator.ontology.get_rdf_graph()
        self._init_namespaces(self.graph)

        # Write guards because Biocypher has `write_nodes` and `write_edges`,
        # but not `write`, so we need to ensure to call both.
        self._has_nodes = False
        self._has_edges = False

        self.edge_models = ["Association", "ObjectProperty"]
        if edge_model not in self.edge_models:
            msg = f"`edge_model` cannot be '{edge_model}', but should be either: {' or '.join(self.edge_models)}"
            logger.error(msg)
            raise ValueError(msg)
        self.edge_model = edge_model

        self.file_stem = file_stem

    def _write_single_node_list_to_file(
        self,
        node_list: list,
        label: str,
        prop_dict: dict,
        labels: str,
    ) -> bool:
        """Save a list of BioCypherNodes in the graph.

        This function takes a list of BioCypherNodes and saves them in
        `self.graph`. It re-uses RDFWriter's machinery, hence the misleading
        name.

        Nodes are modelled as class instances, being also
        owl:NamedIndividual.

        Args:
        ----
            node_list (list): A list of BioCypherNodes to be written.

            label (str): The label (type) of the nodes.

            prop_dict (dict): A dictionary of properties and their types for the
                node class.

            labels (str): string of one or several concatenated labels

        Returns:
        -------
            bool: True for success, False otherwise.
        """

        # NOTE: despite its name, this function does not write to file,
        #       but to self.graph.
        # NOTE: labels and prop_dict are not used.

        if not all(isinstance(n, BioCypherNode) for n in node_list):
            logger.error("Nodes must be passed as type BioCypherNode.")
            return False

        # Cache for terms with specific namespaces.
        already_found = {}

        for n in node_list:
            rdf_subject = url_quote(n.get_id())
            properties = n.get_properties()
            logger.debug(f"Node Class: [{rdf_subject}]")

            all_labels = list(reversed(list(self.translator.ontology.get_ancestors(n.get_label()).nodes)))
            logger.debug(f"\tVocabulary ancestors: {all_labels}")

            # Create types in ancestors that would not exist in the vocabulary.
            # For those that exists, get the URI (and thus the correct namespace).
            for ancestor, current_class in zip(all_labels, all_labels[1:], strict=False):
                logger.debug(f"\t\t'{current_class}' is_a '{ancestor}'")
                ancestor_label = self.translator.name_sentence_to_pascal(ancestor)
                current_label = self.translator.name_sentence_to_pascal(current_class)

                # Fast search using default (or biocypher, if no default) namespace.
                rdf_currents = list(
                    self.graph.triples(
                        (
                            self.to_uri(current_label),
                            RDFS.subClassOf,
                            self.to_uri(ancestor_label),
                        ),
                    ),
                )

                if not rdf_currents:
                    # Slow search with SPARQL queries.

                    # Use cache if term has been found already.
                    if ancestor_label in already_found:
                        uri_ancestor = already_found[ancestor_label]
                    else:
                        # Use SPARQL queries to get a term with an existing namespace.
                        # Because the missing term may be just in another namespace.
                        # But we don't want to SPARQL before, because it is so slow.
                        # FIXME this is VERY slow, maybe we can recover the namespaces
                        # from some BioCypher data structure?

                        # Note: using \\b in the regexp does not seems to work.
                        uri_ancestor = self.find_uri(f"#{ancestor_label}$")
                        if not uri_ancestor:
                            msg = f"I found no term with subject URI matching `#{ancestor_label}$`, but it should exist"
                            logger.error(msg)
                            raise RuntimeError(msg)

                        already_found[ancestor_label] = uri_ancestor

                    if current_label not in already_found:
                        uri_current = self.find_uri(f"#{current_label}$")
                        if not uri_current:
                            uri_current = self.as_uri(current_label, "biocypher")
                            # Create the term in biocypher namespace.
                            self.graph.add(
                                (
                                    uri_current,
                                    RDF.type,
                                    uri_ancestor,
                                ),
                            )
                            logger.debug(f"\t\t\t[{uri_current}]--(type)->[{uri_ancestor}]")

                        already_found[current_label] = uri_current

                else:  # Found by fast search.
                    assert len(rdf_currents) > 0
                    uri_current = rdf_currents[0][0]

            # Add the instance.
            self.graph.add(
                (
                    self.to_uri(rdf_subject),
                    RDF.type,
                    uri_current,
                ),
            )
            logger.debug(f"\t[{rdf_subject}]--(type)->[{uri_current}]")

            # The instance is also a NamedIndividual, in OWL.
            self.graph.add(
                (
                    self.to_uri(rdf_subject),
                    RDF.type,
                    OWL.NamedIndividual,
                ),
            )
            logger.debug(f"\t[{rdf_subject}]--(type)->[NamedIndividual]")

            # Add a readable label.
            self.graph.add(
                (
                    self.to_uri(rdf_subject),
                    RDFS.label,
                    Literal(n.get_id()),
                ),
            )
            logger.debug(f"\t[{rdf_subject}]--(label)->[{n.get_id()}]")

            # Add properties.
            for key, value in properties.items():
                # only write value if it exists.
                if value:
                    self.add_property_to_graph(self.graph, rdf_subject, value, key)

        self._has_nodes = True
        return True

    def _write_single_edge_list_to_file(
        self,
        edge_list: list,
        label: str,
        prop_dict: dict,
    ):
        """Save a list of BioCypherEdges in the graph.

        This function takes a list of BioCypherEdges and saves them in
        `self.graph`. It re-uses RDFWriter's machinery, hence the misleading
        name.

        Args:
        ----
            edge_list (list): list of BioCypherEdges to be written

            label (str): the label (type) of the edge

            prop_dict (dict): properties of node class passed from parsing
                function and their types

        Returns:
        -------
            bool: True for success, False otherwise.

        """

        # NOTE: despite its name, this function does not write to file,
        #       but to self.graph.
        # NOTE: prop_dict is not used.

        if not all(isinstance(n, BioCypherEdge) for n in edge_list):
            logger.error("Edges must be passed as type BioCypherEdge.")
            return False

        for edge in edge_list:
            rdf_subject = url_quote(edge.get_source_id())
            rdf_object = url_quote(edge.get_target_id())
            rdf_properties = edge.get_properties()

            edge_label = url_quote(edge.get_label())
            edge_uri = self.to_uri(edge_label)

            if self.edge_model == "ObjectProperty":
                # Add to the subject the property toward the object.
                self.graph.add(
                    (
                        self.to_uri(rdf_subject),
                        edge_uri,
                        self.to_uri(rdf_object),
                    ),
                )
                logger.debug(f"Edge ObjectProperty: [{rdf_subject}]--({edge_label})->[{rdf_object}]")

            elif self.edge_model == "Association":
                # Modelling edges as Association allows for attaching
                # data properties to an intermediate node.
                logger.debug(f"EDGE Association: [{rdf_subject}]--({edge_label})->[{rdf_object}]")

                if edge.get_id():
                    rdf_id = url_quote(edge.get_id())
                else:
                    # We need an instance to attach properties.
                    rdf_id = url_quote(f"{rdf_subject}--{edge.get_label()}--{rdf_object}")

                # Add object class modelling the edge.
                # NOTE (from https://www.w3.org/TR/owl-ref/):
                # owl:Class is defined as a subclass of rdfs:Class. The rationale for
                #  having a separate OWL class construct lies in the restrictions on
                #  OWL DL (and thus also on OWL Lite), which imply that not all RDFS
                #  classes are legal OWL DL classes. In OWL Full these restrictions
                #  do not exist and therefore owl:Class and rdfs:Class are equivalent
                #  in OWL Full.
                self.graph.add((edge_uri, RDF.type, OWL.Class))
                logger.debug(f"\tEdge object: [{edge_label}]--(type)->[Class]")

                # Instantiate the edge object.
                self.graph.add(
                    (
                        self.to_uri(rdf_id),
                        RDF.type,
                        edge_uri,
                    ),
                )
                logger.debug(f"\tEdge object instance: [{rdf_id}]--(type)->[{edge_label}]")

                # ObjectProperties modelling the subject and object
                # parts of the links around the object.
                # edge_source and edge_target inherits from edge,
                # and are in the biocypher namespace.
                self.graph.add(
                    (
                        self.as_uri("edge", "biocypher"),
                        RDF.type,
                        OWL.ObjectProperty,
                    ),
                )
                logger.debug("\tBase ObjectProperty type: [edge]--(type)->[ObjectProperty]")

                self.graph.add(
                    (
                        self.as_uri("edge_source", "biocypher"),
                        RDFS.subPropertyOf,
                        self.as_uri("edge", "biocypher"),
                    ),
                )
                logger.debug("\tLeft ObjectProperty type: [edge_source]--(type)->[edge]")

                self.graph.add(
                    (
                        self.as_uri("edge_target", "biocypher"),
                        RDFS.subPropertyOf,
                        self.as_uri("edge", "biocypher"),
                    ),
                )
                logger.debug("\tRight ObjectProperty type: [edge_target]--(type)->[edge]")

                self.graph.add(
                    (
                        self.to_uri(rdf_subject),
                        self.as_uri("edge_source", "biocypher"),
                        self.as_uri(rdf_id, "biocypher"),
                    ),
                )
                logger.debug(f"\tLeft ObjectProperty: [{rdf_subject}]--(edge_source)->[{rdf_id}]")

                self.graph.add(
                    (
                        self.as_uri(rdf_id, "biocypher"),
                        self.as_uri("edge_target", "biocypher"),
                        self.to_uri(rdf_object),
                    ),
                )
                logger.debug(f"\tRight ObjectProperty: [{rdf_id}]--(edge_target)->[{rdf_object}]")

                # Add properties to the edge modelled as an instance.
                for key, value in rdf_properties.items():
                    # only write value if it exists.
                    if value:
                        self.add_property_to_graph(self.graph, rdf_id, value, key)

            else:
                logger.debug(f"{self.edge_model} not in {self.edge_models}")
                assert self.edge_model in self.edge_models

        self._has_edges = True
        return True

    def write_nodes(self, nodes, batch_size: int = int(1e6), force: bool = False) -> bool:
        """Insert nodes in `self.graph`.

        It calls _write_node_data, which calls _write_single_node_list_to_file.

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
        # Calls _write_single_node_list_to_file, which sets self.has_nodes.
        if not super().write_nodes(nodes, batch_size, force):
            return False

        # Attempt at writing the file.
        self._write_file()
        return True

    def write_edges(
        self,
        edges: list | GeneratorType,
        batch_size: int = int(1e6),
    ) -> bool:
        """Insert edges in `self.graph`.

        It calls _write_edge_data, which calls _write_single_edge_list_to_file.

        Args:
        ----
            edges (BioCypherEdge): a list or generator of edges in
                :py:class:`BioCypherEdge` format
            batch_size (int): The number of edges to write in each batch.

        Returns:
        -------
            bool: The return value. True for success, False otherwise.

        """
        # Calls _write_single_edge_list_to_file, which sets self.has_edges.
        if not super().write_edges(edges, batch_size):
            return False

        # Attempt at writing the file.
        self._write_file()
        return True

    def _write_file(self):
        """Write an OWL file if nodes and edges are ready in self.graph."""
        if self._has_nodes and self._has_edges:
            file_name = os.path.join(self.outdir, f"{self.file_stem}.{self.extension}")
            logger.info(f"Writing {len(self.graph)} terms to {file_name}")
            self.graph.serialize(destination=file_name, format=self.rdf_format)
