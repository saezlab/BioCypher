#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import obonet

from ._logger import logger
import biocypher._misc as _misc

if TYPE_CHECKING:

    from ._biolink import BiolinkAdapter


class Tree:

    def update_tree(self):

        raise NotImplementedError


    def ensure_tree(self):
        """
        Create the ontology tree if not available yet.
        """

        if not getattr(self, '_tree', None):

            self.update_tree()


    def nested_tree(self) -> dict[str, dict]:
        """
        Ontology tree in nested representation, suitable for NetworkX.
        """

        self.ensure_tree()

        return _misc.nested_tree(self._tree)


    @property
    def tree(self) -> 'treelib.Tree':
        """
        Ontology tree as an ASCII printable string.
        """

        self.ensure_tree()

        return _misc.tree_figure(self._tree)


    def show(self):
        """
        Show the ontology tree using treelib.
        """

        logger.info(
            'Showing ontology structure, '
            f'based on Biolink {self.biolink_version}:'
        )

        self.tree.show()


    def networkx_tree(self) -> 'nx.DiGraph':
        """
        The ontology tree as a directed NetworkX graph.
        """

        nx = _misc.try_import('networkx')

        return nx.Graph(self.nested_tree()) if nx else None


class OntologyAdapter(Tree):
    """
    Generic ontology adapter class.
    """

    def __init__(
        self,
        head_join_node: str | None = None,
        tail_join_node: str | None = None,
        tail_ontology_url: str | None = None,
        head_ontology_url: str | None = None,
        biolink_adapter: BiolinkAdapter | None = None,
    ):
        """
        Build a hybrid ontology from two OBO files.

        Uses Biolink as the default head ontology if no URL is given.

        Args:
            head_ontology_url:
                URL to the head ontology.
            tail_ontology_url:
                URL to the tail ontology.
            head_join_node:
                The node in the head ontology to which the tail ontology will
                be joined.
            tail_join_node:
                The node in the tail ontology that will be joined to the head
                ontology.
            biolink_adapter:
                A BiolinkAdapter instance. To be supplied if no head
                ontology URL is given.

        TODO:
            - Build visualisation only for parts of the schema_config also for
              tail ontology
            - Update show ontology structure to print also tail ontology info
            - Genericise leaves creation beyond biolink
        """

        if not head_ontology_url and not biolink_adapter:

            raise ValueError(
                'Either head_ontology_url or biolink_adapter must be supplied.'
            )

        self.head_ontology_url = head_ontology_url
        self.tail_ontology_url = tail_ontology_url
        self.head_join_node = head_join_node
        self.tail_join_node = tail_join_node
        self.biolink_adapter = biolink_adapter

        # pass on leaves from biolink adapter, only works for the case of
        # Biolink as head ontology; TODO generalise
        if self.biolink_adapter:

            self.schema = self.biolink_adapter.schema
            self.biolink_schema = self.biolink_adapter.biolink_schema

        self.head_ontology = None
        self.tail_ontology = None
        self.hybrid_ontology = None

        self.main()


    def main(self):
        """
        Main method to be run on instantiation.

        Loads the ontologies, joins them, and returns the hybrid ontology.
        The Biolink ontology is used as a default.
        """

        self.load_ontologies()

        if self.tail_ontology_url:

            self.find_join_nodes()
            self.join_ontologies()


    def load_ontologies(self):
        """
        Loads the ontologies using obonet.

        Importantly, obonet orients edges not
        from parent to child, but from child to parent, which goes against the
        assumptions in networkx. For instance, for subsetting the ontology, the
        `reverse` method needs to be called first. If head ontology is loaded
        from Biolink, it is reversed to be consistent with obonet. Currently,
        we use the names of the nodes instead of accessions, so we reverse the
        name and ID mapping. The accession becomes the 'id' attribute of the
        node data.
        """

        # use Biolink as the head ontology if no URL is given
        if self.head_ontology_url:
            self.head_ontology = self._load_ontology(self.head_ontology_url)
        else:
            logger.info('Loading ontology from Biolink adapter.')
            self.head_ontology = (
                self.biolink_adapter.get_networkx_graph().reverse()
            )

        # tail ontology is always loaded from URL
        if self.tail_ontology_url:
            self.tail_ontology = self._load_ontology(self.tail_ontology_url)


    @classmethod
    def _load_ontology(cls, url: str) -> "Idontknow":

        logger.info(f'Loading ontology from `{url}`.')
        return cls.reverse_name_and_ac(obonet.read_obo(url))


    @staticmethod
    def reverse_name_and_ac(ontology):
        """
        Reverses the name and ID of the ontology nodes.

        Reverses the name and ID of the ontology nodes. Replaces underscores in
        the node names with spaces. Currently standard for consistency with
        Biolink, although we lose the original ontology's spelling.
        """

        nx = _misc.try_import('networkx')

        id_to_name = {}

        for _id, data in ontology.nodes(data=True):

            data['ac'] = _id
            id_to_name[_id] = data['name'].replace('_', ' ')

        ontology = nx.relabel_nodes(ontology, id_to_name)

        return ontology


    def join_ontologies(self):
        """
        Joins the head and tail ontologies.

        Joins the ontologies by adding the tail ontology as a subgraph to the
        head ontology at the specified join nodes. Note that the tail ontology
        needs to be reversed before creating the subgraph, as obonet orients
        edges from child to parent.
        """

        logger.info('Creating ontology graph.')

        self.hybrid_ontology = self.head_ontology.copy()

        # subtree of tail ontology at join node
        tail_ontology_subtree = dfs_tree(
            self.tail_ontology.reverse(), self.tail_join_node
        ).reverse()

        # transfer node attributes from tail ontology to subtree
        for node in tail_ontology_subtree.nodes:

            tail_ontology_subtree.nodes[node].update(
                self.tail_ontology.nodes[node]
            )

        # rename tail join node to match head join node
        tail_ontology_subtree = nx.relabel_nodes(
            tail_ontology_subtree, {self.tail_join_node: self.head_join_node}
        )

        # combine head ontology and tail subtree
        self.hybrid_ontology = nx.compose(
            self.hybrid_ontology, tail_ontology_subtree
        )


    def find_join_nodes(self):
        """
        Finds the join nodes in the ontologies. If the join nodes are not
        found, the method will raise an error.
        """
        if self.head_join_node not in self.head_ontology.nodes:

            if self.head_ontology_url:

                self.head_join_node = self.find_join_node_by_name(
                    self.head_ontology, self.head_join_node
                )

            else:

                raise ValueError(
                    f'Head join node {self.head_join_node} not found in '
                    f'head ontology.'
                )

        if self.tail_join_node not in self.tail_ontology.nodes:

            self.tail_join_node = self.find_join_node_by_name(
                self.tail_ontology, self.tail_join_node
            )

            if not self.tail_join_node:

                raise ValueError(
                    f'Tail join node {self.tail_join_node} not found in '
                    f'tail ontology.'
                )


    def find_join_node_by_name(self, ontology, node_name):
        """
        Finds the join node in the ontology by name. If the join node is not
        found, the method will return None.
        """
        name_to_id = {
            data.get('name'): _id
            for _id, data in ontology.nodes(data=True)
        }

        return name_to_id.get(node_name)


    def update_tree(self):

        if self.hybrid_ontology:

            ontology = self.hybrid_ontology

        else:

            ontology = self.head_ontology

        tree = _misc.create_tree_visualisation(ontology)

        # add synonym information
        for class_name in self.schema:

            syn = self.schema[class_name].get('synonym_for')

            if syn:

                tree.nodes[class_name].tag = f'{class_name} = {syn}'

        self._tree = tree


     def get_node_ancestry(self, node: str) -> list | None:
        """
        Returns the ancestry of a node in the ontology.
        """

        ontology = self.hybrid_ontology or self.head_ontology

        # check if node in ontology
        if node not in ontology.nodes:

            return None

        return list(dfs_tree(ontology, node))
