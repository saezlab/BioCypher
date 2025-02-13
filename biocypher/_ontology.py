"""BioCypher 'ontology' module to parse and represent ontologies.

Also performs ontology hybridisation and other advanced operations.
"""

import os

from datetime import datetime
from itertools import chain
from typing import Optional

import networkx as nx
import rdflib

from rdflib import Graph
from rdflib.extras.external_graph_libs import rdflib_to_networkx_digraph

from ._logger import logger
from ._mapping import OntologyMapping
from ._misc import (
    create_tree_visualisation,
    sentencecase_to_pascalcase,
    to_list,
    to_lower_sentence_case,
)

logger.debug(f"Loading module {__name__}.")


class OntologyAdapter:
    """Class that represents an ontology to be used in the Biocypher framework.

    Can read from a variety of formats, including OWL, OBO, and RDF/XML. The
    ontology is represented by a networkx.DiGraph object; an RDFlib graph is
    also kept. By default, the DiGraph reverses the label and identifier of the
    nodes, such that the node name in the graph is the human-readable label. The
    edges are oriented from child to parent. Labels are formatted in lower
    sentence case and underscores are replaced by spaces. Identifiers are taken
    as defined and the prefixes are removed by default.
    """

    def __init__(
        self,
        ontology_file: str,
        root_label: str,
        ontology_file_format: str | None = None,
        head_join_node_label: str | None = None,
        merge_nodes: bool | None = True,
        switch_label_and_id: bool = True,
        remove_prefixes: bool = True,
    ):
        """Initialize the OntologyAdapter class.

        Args:
        ----
            ontology_file (str): Path to the ontology file. Can be local or
                remote.

            root_label (str): The label of the root node in the ontology. In
                case of a tail ontology, this is the tail join node.

            ontology_file_format (str): The format of the ontology file (e.g. "application/rdf+xml")
                If format is not passed, it is determined automatically.

            head_join_node_label (str): Optional variable to store the label of the
                node in the head ontology that should be used to join to the
                root node of the tail ontology. Defaults to None.

            merge_nodes (bool): If True, head and tail join nodes will be
                merged, using the label of the head join node. If False, the
                tail join node will be attached as a child of the head join
                node.

            switch_label_and_id (bool): If True, the node names in the graph will be
                the human-readable labels. If False, the node names will be the
                identifiers. Defaults to True.

            remove_prefixes (bool): If True, the prefixes of the identifiers will
                be removed. Defaults to True.

        """
        logger.info(f"Instantiating OntologyAdapter class for {ontology_file}.")

        self._ontology_file = ontology_file
        self._root_label = root_label
        self._format = ontology_file_format
        self._merge_nodes = merge_nodes
        self._head_join_node = head_join_node_label
        self._switch_label_and_id = switch_label_and_id
        self._remove_prefixes = remove_prefixes

        self._rdf_graph = self._load_rdf_graph(ontology_file)

        self._nx_graph = self._rdf_to_nx(self._rdf_graph, root_label, switch_label_and_id)

    def _rdf_to_nx(
        self,
        _rdf_graph: rdflib.Graph,
        root_label: str,
        switch_label_and_id: bool,
        rename_nodes: bool = True,
    ) -> nx.DiGraph:
        one_to_one_triples, one_to_many_dict = self._get_relevant_rdf_triples(_rdf_graph)
        nx_graph = self._convert_to_nx(one_to_one_triples, one_to_many_dict)
        nx_graph = self._add_labels_to_nodes(nx_graph, switch_label_and_id)
        nx_graph = self._change_nodes_to_biocypher_format(nx_graph, switch_label_and_id, rename_nodes)
        nx_graph = self._get_all_ancestors(nx_graph, root_label, switch_label_and_id, rename_nodes)
        return nx.DiGraph(nx_graph)

    def _get_relevant_rdf_triples(self, g: rdflib.Graph) -> tuple:
        one_to_one_inheritance_graph = self._get_one_to_one_inheritance_triples(g)
        intersection = self._get_multiple_inheritance_dict(g)
        return one_to_one_inheritance_graph, intersection

    def _get_one_to_one_inheritance_triples(self, g: rdflib.Graph) -> rdflib.Graph:
        """Get the one to one inheritance triples from the RDF graph.

        Args:
        ----
            g (rdflib.Graph): The RDF graph

        Returns:
        -------
            rdflib.Graph: The one to one inheritance graph

        """
        one_to_one_inheritance_graph = Graph()
        # for s, p, o in g.triples((None, rdflib.RDFS.subClassOf, None)):
        for s, p, o in chain(
            g.triples((None, rdflib.RDFS.subClassOf, None)),  # Node classes
            g.triples((None, rdflib.RDF.type, rdflib.RDFS.Class)),  # Root classes
            g.triples((None, rdflib.RDFS.subPropertyOf, None)),  # OWL "edges" classes
            g.triples((None, rdflib.RDF.type, rdflib.OWL.ObjectProperty)),  # OWL "edges" root classes
        ):
            if self.has_label(s, g):
                one_to_one_inheritance_graph.add((s, p, o))
        return one_to_one_inheritance_graph

    def _get_multiple_inheritance_dict(self, g: rdflib.Graph) -> dict:
        """Get the multiple inheritance dictionary from the RDF graph.

        Args:
        ----
            g (rdflib.Graph): The RDF graph

        Returns:
        -------
            dict: The multiple inheritance dictionary

        """
        multiple_inheritance = g.triples((None, rdflib.OWL.intersectionOf, None))
        intersection = {}
        for (
            node,
            has_multiple_parents,
            first_node_of_intersection_list,
        ) in multiple_inheritance:
            parents = self._retrieve_rdf_linked_list(first_node_of_intersection_list)
            child_name = None
            for s_, _, _ in chain(
                g.triples((None, rdflib.RDFS.subClassOf, node)),
                g.triples((None, rdflib.RDFS.subPropertyOf, node)),
            ):
                child_name = s_

            # Handle Snomed CT post coordinated expressions
            if not child_name:
                for s_, _, _ in g.triples((None, rdflib.OWL.equivalentClass, node)):
                    child_name = s_

            if child_name:
                intersection[node] = {
                    "child_name": child_name,
                    "parent_node_names": parents,
                }
        return intersection

    def has_label(self, node: rdflib.URIRef, g: rdflib.Graph) -> bool:
        """Check if the node has a label in the graph.

        Args:
        ----
            node (rdflib.URIRef): The node to check
            g (rdflib.Graph): The graph to check in
        Returns:
            bool: True if the node has a label, False otherwise

        """
        return (node, rdflib.RDFS.label, None) in g

    def _retrieve_rdf_linked_list(self, subject: rdflib.URIRef) -> list:
        """Recursively retrieve a linked list from RDF.

        Example RDF list with the items [item1, item2]:
        list_node - first -> item1
        list_node - rest -> list_node2
        list_node2 - first -> item2
        list_node2 - rest -> nil

        Args:
        ----
            subject (rdflib.URIRef): One list_node of the RDF list

        Returns:
        -------
            list: The items of the RDF list

        """
        g = self._rdf_graph
        rdf_list = []
        for s, p, o in g.triples((subject, rdflib.RDF.first, None)):
            rdf_list.append(o)
        for s, p, o in g.triples((subject, rdflib.RDF.rest, None)):
            if o != rdflib.RDF.nil:
                rdf_list.extend(self._retrieve_rdf_linked_list(o))
        return rdf_list

    def _convert_to_nx(self, one_to_one: rdflib.Graph, one_to_many: dict) -> nx.DiGraph:
        """Convert the one to one and one to many inheritance graphs to networkx.

        Args:
        ----
            one_to_one (rdflib.Graph): The one to one inheritance graph
            one_to_many (dict): The one to many inheritance dictionary

        Returns:
        -------
            nx.DiGraph: The networkx graph

        """
        nx_graph = rdflib_to_networkx_digraph(one_to_one, edge_attrs=lambda s, p, o: {}, calc_weights=False)
        for key, value in one_to_many.items():
            nx_graph.add_edges_from([(value["child_name"], parent) for parent in value["parent_node_names"]])
            if key in nx_graph.nodes:
                nx_graph.remove_node(key)
        return nx_graph

    def _add_labels_to_nodes(self, nx_graph: nx.DiGraph, switch_label_and_id: bool) -> nx.DiGraph:
        """Add labels to the nodes in the networkx graph.

        Args:
        ----
            nx_graph (nx.DiGraph): The networkx graph
            switch_label_and_id (bool): If True, id and label are switched

        Returns:
        -------
            nx.DiGraph: The networkx graph with labels

        """
        for node in list(nx_graph.nodes):
            nx_id, nx_label = self._get_nx_id_and_label(node, switch_label_and_id)
            if nx_id == "none":
                # remove node if it has no id
                nx_graph.remove_node(node)
                continue

            nx_graph.nodes[node]["label"] = nx_label
        return nx_graph

    def _change_nodes_to_biocypher_format(
        self,
        nx_graph: nx.DiGraph,
        switch_label_and_id: bool,
        rename_nodes: bool = True,
    ) -> nx.DiGraph:
        """Change the nodes in the networkx graph to BioCypher format.

        This involves:
            - removing the prefix of the identifier
            - switching the id and label if requested
            - adapting the labels (replace _ with space and convert to lower
                sentence case)
        Args:
        ----
            nx_graph (nx.DiGraph): The networkx graph
            switch_label_and_id (bool): If True, id and label are switched
            rename_nodes (bool): If True, the nodes are renamed

        Returns:
        -------
            nx.DiGraph: The networkx ontology graph in BioCypher format

        """
        mapping = {
            node: self._get_nx_id_and_label(node, switch_label_and_id, rename_nodes)[0] for node in nx_graph.nodes
        }
        renamed = nx.relabel_nodes(nx_graph, mapping, copy=False)
        return renamed

    def _get_all_ancestors(
        self,
        renamed: nx.DiGraph,
        root_label: str,
        switch_label_and_id: bool,
        rename_nodes: bool = True,
    ) -> nx.DiGraph:
        """Get all ancestors of the root node in the networkx graph.

        Args:
        ----
            renamed (nx.DiGraph): The renamed networkx graph
            root_label (str): The label of the root node in the ontology
            switch_label_and_id (bool): If True, id and label are switched
            rename_nodes (bool): If True, the nodes are renamed

        Returns:
        -------
            nx.DiGraph: The filtered networkx graph

        """
        root = self._get_nx_id_and_label(
            self._find_root_label(self._rdf_graph, root_label),
            switch_label_and_id,
            rename_nodes,
        )[0]
        ancestors = nx.ancestors(renamed, root)
        ancestors.add(root)
        filtered_graph = renamed.subgraph(ancestors)
        return filtered_graph

    def _get_nx_id_and_label(self, node, switch_id_and_label: bool, rename_nodes: bool = True) -> tuple[str, str]:
        """Rename node id and label for nx graph.

        Args:
        ----
            node (str): The node to rename
            switch_id_and_label (bool): If True, switch id and label

        Returns:
        -------
            tuple[str, str]: The renamed node id and label

        """
        node_id_str = self._remove_prefix(str(node))
        node_label_str = str(self._rdf_graph.value(node, rdflib.RDFS.label))
        if rename_nodes:
            node_label_str = node_label_str.replace("_", " ")
            node_label_str = to_lower_sentence_case(node_label_str)
        nx_id = node_label_str if switch_id_and_label else node_id_str
        nx_label = node_id_str if switch_id_and_label else node_label_str
        return nx_id, nx_label

    def _find_root_label(self, g, root_label):
        # Loop through all labels in the ontology
        for label_subject, _, label_in_ontology in g.triples((None, rdflib.RDFS.label, None)):
            # If the label is the root label, set the root node to the label's subject
            if str(label_in_ontology) == root_label:
                root = label_subject
                break
        else:
            labels_in_ontology = []
            for label_subject, _, label_in_ontology in g.triples((None, rdflib.RDFS.label, None)):
                labels_in_ontology.append(str(label_in_ontology))
            msg = (
                f"Could not find root node with label '{root_label}'. "
                f"The ontology contains the following labels: {labels_in_ontology}"
            )
            logger.error(msg)
            raise ValueError(msg)
        return root

    def _remove_prefix(self, uri: str) -> str:
        """Remove the prefix of a URI.

        URIs can contain either "#" or "/" as a separator between the prefix
        and the local name. The prefix is everything before the last separator.

        Args:
        ----
            uri (str): The URI to remove the prefix from

        Returns:
        -------
            str: The URI without the prefix

        """
        if self._remove_prefixes:
            return uri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
        else:
            return uri

    def _load_rdf_graph(self, ontology_file):
        """Load the ontology into an RDFlib graph.

        The ontology file can be in OWL, OBO, or RDF/XML format.

        Args:
        ----
            ontology_file (str): The path to the ontology file

        Returns:
        -------
            rdflib.Graph: The RDFlib graph

        """
        g = rdflib.Graph()
        g.parse(ontology_file, format=self._get_format(ontology_file))
        return g

    def _get_format(self, ontology_file):
        """Get the format of the ontology file."""
        if self._format:
            if self._format == "owl":
                return "application/rdf+xml"
            elif self._format == "obo":
                raise NotImplementedError("OBO format not yet supported")
            elif self._format == "rdf":
                return "application/rdf+xml"
            elif self._format == "ttl":
                return self._format
            else:
                msg = f"Could not determine format of ontology file {ontology_file}"
                logger.error(msg)
                raise ValueError(msg)

        if ontology_file.endswith(".owl"):
            return "application/rdf+xml"
        elif ontology_file.endswith(".obo"):
            msg = "OBO format not yet supported"
            logger.error(msg)
            raise NotImplementedError(msg)
        elif ontology_file.endswith(".rdf"):
            return "application/rdf+xml"
        elif ontology_file.endswith(".ttl"):
            return "ttl"
        else:
            msg = f"Could not determine format of ontology file {ontology_file}"
            logger.error(msg)
            raise ValueError(msg)

    def get_nx_graph(self):
        """Get the networkx graph representing the ontology."""
        return self._nx_graph

    def get_rdf_graph(self):
        """Get the RDFlib graph representing the ontology."""
        return self._rdf_graph

    def get_root_node(self):
        """Get root node in the ontology.

        Returns
        -------
            root_node: If _switch_label_and_id is True, the root node label is
                returned, otherwise the root node id is returned.

        """
        root_node = None
        root_label = self._root_label.replace("_", " ")

        if self._switch_label_and_id:
            root_node = to_lower_sentence_case(root_label)
        elif not self._switch_label_and_id:
            for node, data in self.get_nx_graph().nodes(data=True):
                if "label" in data and data["label"] == to_lower_sentence_case(root_label):
                    root_node = node
                    break

        return root_node

    def get_ancestors(self, node_label):
        """Get the ancestors of a node in the ontology."""
        return nx.dfs_preorder_nodes(self._nx_graph, node_label)

    def get_head_join_node(self):
        """Get the head join node of the ontology."""
        return self._head_join_node


class Ontology:
    """A class that represents the ontological "backbone" of a KG.

    The ontology can be built from a single resource, or hybridised from a
    combination of resources, with one resource being the "head" ontology, while
    an arbitrary number of other resources can become "tail" ontologies at
    arbitrary fusion points inside the "head" ontology.
    """

    def __init__(
        self,
        head_ontology: dict,
        ontology_mapping: Optional["OntologyMapping"] = None,
        tail_ontologies: dict | None = None,
    ):
        """Initialize the Ontology class.

        Args:
        ----
            head_ontology (OntologyAdapter): The head ontology.

            tail_ontologies (list): A list of OntologyAdapters that will be
                added to the head ontology. Defaults to None.

        """
        self._head_ontology_meta = head_ontology
        self.mapping = ontology_mapping
        self._tail_ontology_meta = tail_ontologies

        self._tail_ontologies = None
        self._nx_graph = None

        # keep track of nodes that have been extended
        self._extended_nodes = set()

        self._main()

    def _main(self) -> None:
        """Instantiate the ontology.

        Loads the ontologies, joins them, and returns the hybrid ontology.
        Loads only the head ontology if nothing else is given. Adds user
        extensions and properties from the mapping.
        """
        self._load_ontologies()

        if self._tail_ontologies:
            for adapter in self._tail_ontologies.values():
                head_join_node = self._get_head_join_node(adapter)
                self._join_ontologies(adapter, head_join_node)
        else:
            self._nx_graph = self._head_ontology.get_nx_graph()

        if self.mapping:
            self._extend_ontology()

            # experimental: add connections of disjoint classes to entity
            # self._connect_biolink_classes()

            self._add_properties()

    def _load_ontologies(self) -> None:
        """For each ontology, load the OntologyAdapter object.

        Store it as an instance variable (head) or in an instance dictionary
        (tail).
        """
        logger.info("Loading ontologies...")

        self._head_ontology = OntologyAdapter(
            ontology_file=self._head_ontology_meta["url"],
            root_label=self._head_ontology_meta["root_node"],
            ontology_file_format=self._head_ontology_meta.get("format", None),
            switch_label_and_id=self._head_ontology_meta.get("switch_label_and_id", True),
        )

        if self._tail_ontology_meta:
            self._tail_ontologies = {}
            for key, value in self._tail_ontology_meta.items():
                self._tail_ontologies[key] = OntologyAdapter(
                    ontology_file=value["url"],
                    root_label=value["tail_join_node"],
                    head_join_node_label=value["head_join_node"],
                    ontology_file_format=value.get("format", None),
                    merge_nodes=value.get("merge_nodes", True),
                    switch_label_and_id=value.get("switch_label_and_id", True),
                )

    def _get_head_join_node(self, adapter: OntologyAdapter) -> str:
        """Try to find the head join node of the given ontology adapter.

        Find the node in the head ontology that is the head join node. If the
        join node is not found, the method will raise an error.

        Args:
        ----
            adapter (OntologyAdapter): The ontology adapter of which to find the
                join node in the head ontology.

        Returns:
        -------
            str: The head join node in the head ontology.

        Raises:
        ------
            ValueError: If the head join node is not found in the head ontology.

        """
        head_join_node = None
        user_defined_head_join_node_label = adapter.get_head_join_node()
        head_join_node_label_in_bc_format = to_lower_sentence_case(user_defined_head_join_node_label.replace("_", " "))

        if self._head_ontology._switch_label_and_id:
            head_join_node = head_join_node_label_in_bc_format
        elif not self._head_ontology._switch_label_and_id:
            for node_id, data in self._head_ontology.get_nx_graph().nodes(data=True):
                if "label" in data and data["label"] == head_join_node_label_in_bc_format:
                    head_join_node = node_id
                    break

        if head_join_node not in self._head_ontology.get_nx_graph().nodes:
            head_ontology = self._head_ontology._rdf_to_nx(
                self._head_ontology.get_rdf_graph(),
                self._head_ontology._root_label,
                self._head_ontology._switch_label_and_id,
                rename_nodes=False,
            )
            msg = (
                f"Head join node '{head_join_node}' not found in head ontology. "
                f"The head ontology contains the following nodes: {head_ontology.nodes}."
            )
            logger.error(msg)
            raise ValueError(msg)
        return head_join_node

    def _join_ontologies(self, adapter: OntologyAdapter, head_join_node) -> None:
        """Join the present ontologies.

        Join two ontologies by adding the tail ontology as a subgraph to the
        head ontology at the specified join nodes.

        Args:
        ----
            adapter (OntologyAdapter): The ontology adapter of the tail ontology
                to be added to the head ontology.

        """
        if not self._nx_graph:
            self._nx_graph = self._head_ontology.get_nx_graph().copy()

        tail_join_node = adapter.get_root_node()
        tail_ontology = adapter.get_nx_graph()

        # subtree of tail ontology at join node
        tail_ontology_subtree = nx.dfs_tree(tail_ontology.reverse(), tail_join_node).reverse()

        # transfer node attributes from tail ontology to subtree
        for node in tail_ontology_subtree.nodes:
            tail_ontology_subtree.nodes[node].update(tail_ontology.nodes[node])

        # if merge_nodes is False, create parent of tail join node from head
        # join node
        if not adapter._merge_nodes:
            # add head join node from head ontology to tail ontology subtree
            # as parent of tail join node
            tail_ontology_subtree.add_node(
                head_join_node,
                **self._head_ontology.get_nx_graph().nodes[head_join_node],
            )
            tail_ontology_subtree.add_edge(tail_join_node, head_join_node)

        # else rename tail join node to match head join node if necessary
        elif tail_join_node != head_join_node:
            tail_ontology_subtree = nx.relabel_nodes(tail_ontology_subtree, {tail_join_node: head_join_node})

        # combine head ontology and tail subtree
        self._nx_graph = nx.compose(self._nx_graph, tail_ontology_subtree)

    def _extend_ontology(self) -> None:
        """Add the user extensions to the ontology.

        Tries to find the parent in the ontology, adds it if necessary, and adds
        the child and a directed edge from child to parent. Can handle multiple
        parents.
        """
        if not self._nx_graph:
            self._nx_graph = self._head_ontology.get_nx_graph().copy()

        for key, value in self.mapping.extended_schema.items():
            # If this class is either a root or a synonym.
            if not value.get("is_a"):
                # If it is a synonym.
                if self._nx_graph.has_node(value.get("synonym_for")):
                    continue

                # If this class is in the schema, but not in the loaded vocabulary.
                if not self._nx_graph.has_node(key):
                    msg = (
                        f"Node {key} not found in ontology, but also has no inheritance definition. Please check your "
                        "schema for spelling errors, first letter not in lower case, use of underscores, a missing "
                        "`is_a` definition (SubClassOf a root node), or missing labels in class or super-classes."
                    )
                    logger.error(msg)
                    raise ValueError(msg)

                # It is a root and it is in the loaded vocabulary.
                continue

            # It is not a root.
            parents = to_list(value.get("is_a"))
            child = key

            while parents:
                parent = parents.pop(0)

                if parent not in self._nx_graph.nodes:
                    self._nx_graph.add_node(parent)
                    self._nx_graph.nodes[parent]["label"] = sentencecase_to_pascalcase(parent)

                    # mark parent as user extension
                    self._nx_graph.nodes[parent]["user_extension"] = True
                    self._extended_nodes.add(parent)

                if child not in self._nx_graph.nodes:
                    self._nx_graph.add_node(child)
                    self._nx_graph.nodes[child]["label"] = sentencecase_to_pascalcase(child)

                    # mark child as user extension
                    self._nx_graph.nodes[child]["user_extension"] = True
                    self._extended_nodes.add(child)

                self._nx_graph.add_edge(child, parent)

                child = parent

    def _connect_biolink_classes(self) -> None:
        """Experimental: Adds edges from disjoint classes to the entity node."""
        if not self._nx_graph:
            self._nx_graph = self._head_ontology.get_nx_graph().copy()

        if "entity" not in self._nx_graph.nodes:
            return

        # biolink classes that are disjoint from entity
        disjoint_classes = [
            "frequency qualifier mixin",
            "chemical entity to entity association mixin",
            "ontology class",
            "relationship quantifier",
            "physical essence or occurrent",
            "gene or gene product",
            "subject of investigation",
        ]

        for node in disjoint_classes:
            if not self._nx_graph.nodes.get(node):
                self._nx_graph.add_node(node)
                self._nx_graph.nodes[node]["label"] = sentencecase_to_pascalcase(node)

            self._nx_graph.add_edge(node, "entity")

    def _add_properties(self) -> None:
        """Add properties to the ontology.

        For each entity in the mapping, update the ontology with the properties
        specified in the mapping. Updates synonym information in the graph,
        setting the synonym as the primary node label.
        """
        for key, value in self.mapping.extended_schema.items():
            if key in self._nx_graph.nodes:
                self._nx_graph.nodes[key].update(value)

            if value.get("synonym_for"):
                # change node label to synonym
                if value["synonym_for"] not in self._nx_graph.nodes:
                    msg = f"Node {value['synonym_for']} not found in ontology."
                    logger.error(msg)
                    raise ValueError(msg)

                self._nx_graph = nx.relabel_nodes(self._nx_graph, {value["synonym_for"]: key})

    def get_ancestors(self, node_label: str) -> list:
        """Get the ancestors of a node in the ontology.

        Args:
        ----
            node_label (str): The label of the node in the ontology.

        Returns:
        -------
            list: A list of the ancestors of the node.

        """
        return nx.dfs_tree(self._nx_graph, node_label)

    def show_ontology_structure(self, to_disk: str = None, full: bool = False):
        """Show the ontology structure using treelib or write to GRAPHML file.

        Args:
        ----
            to_disk (str): If specified, the ontology structure will be saved
                to disk as a GRAPHML file at the location (directory) specified
                by the `to_disk` string, to be opened in your favourite graph
                visualisation tool.

            full (bool): If True, the full ontology structure will be shown,
                including all nodes and edges. If False, only the nodes and
                edges that are relevant to the extended schema will be shown.

        """
        if not full and not self.mapping.extended_schema:
            msg = (
                "You are attempting to visualise a subset of the loaded"
                "ontology, but have not provided a schema configuration. "
                "To display a partial ontology graph, please provide a schema "
                "configuration file; to visualise the full graph, please use "
                "the parameter `full = True`.",
            )
            logger.error(msg)
            raise ValueError(msg)

        if not self._nx_graph:
            msg = "Ontology not loaded."
            logger.error(msg)
            raise ValueError(msg)

        if not self._tail_ontologies:
            msg = f"Showing ontology structure based on {self._head_ontology._ontology_file}"

        else:
            msg = f"Showing ontology structure based on {len(self._tail_ontology_meta) + 1} ontologies: "

        logger.info(msg)

        if not full:
            # set of leaves and their intermediate parents up to the root
            filter_nodes = set(self.mapping.extended_schema.keys())

            for node in self.mapping.extended_schema.keys():
                filter_nodes.update(self.get_ancestors(node).nodes)

            # filter graph
            G = self._nx_graph.subgraph(filter_nodes)

        else:
            G = self._nx_graph

        if not to_disk:
            # create tree
            tree = create_tree_visualisation(G)

            # add synonym information
            for node in self.mapping.extended_schema:
                if not isinstance(self.mapping.extended_schema[node], dict):
                    continue
                if self.mapping.extended_schema[node].get("synonym_for"):
                    tree.nodes[node].tag = f"{node} = {self.mapping.extended_schema[node].get('synonym_for')}"

            logger.info(f"\n{tree}")

            return tree

        else:
            # convert lists/dicts to strings for vis only
            for node in G.nodes:
                # rename node and use former id as label
                label = G.nodes[node].get("label")

                if not label:
                    label = node

                G = nx.relabel_nodes(G, {node: label})
                G.nodes[label]["label"] = node

                for attrib in G.nodes[label]:
                    if type(G.nodes[label][attrib]) in [list, dict]:
                        G.nodes[label][attrib] = str(G.nodes[label][attrib])

            path = os.path.join(to_disk, "ontology_structure.graphml")

            logger.info(f"Writing ontology structure to {path}.")

            nx.write_graphml(G, path)

            return True

    def get_dict(self) -> dict:
        """Return a dictionary representation of the ontology.

        The dictionary is compatible with a BioCypher node for compatibility
        with the Neo4j driver.
        """
        d = {
            "node_id": self._get_current_id(),
            "node_label": "BioCypher",
            "properties": {
                "schema": "self.ontology_mapping.extended_schema",
            },
        }

        return d

    def _get_current_id(self):
        """Instantiate a version ID for the current session.

        For now does simple versioning using datetime.

        Can later implement incremental versioning, versioning from
        config file, or manual specification via argument.
        """
        now = datetime.now()
        return now.strftime("v%Y%m%d-%H%M%S")

    def get_rdf_graph(self):
        """Return the merged RDF graph.

        Return the merged graph of all loaded ontologies (head and tails).
        """
        graph = self._head_ontology.get_rdf_graph()
        if self._tail_ontologies:
            for key, onto in self._tail_ontologies.items():
                assert type(onto) == OntologyAdapter
                # RDFlib uses the + operator for merging.
                graph += onto.get_rdf_graph()
        return graph
