#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#

"""
Biolink toolkit wiki:
https://biolink.github.io/biolink-model-toolkit/example_usage.html
"""

from ._logger import logger

logger.debug(f'Loading module {__name__}.')

from typing import Literal
import re
import json
import pickle
import hashlib

from linkml_runtime.linkml_model.meta import ClassDefinition
import bmt

from . import _misc
from ._config import module_data_path
from . import _cache

__all__ = ['BiolinkAdapter']


class BiolinkAdapter:
    """
    Performs various functions to integrate the Biolink ontology.

    Stores model mappings to allow (reverse) translation of terms and
    queries.

    Todo:
        - refer to pythonised biolink model from YAML
    """

    _DATA_ATTRS = (
        'schema',
        'model',
        'model_name',
        'mappings',
        'reverse_mappings',
        '_ad_hoc_inheritance',
        '_tree',
    )


    def __init__(
        self,
        schema: dict,
        model: Literal['biocypher', 'biolink'] | str | dict | None = None,
        use_cache: bool = True,
    ):
        """
        Args:
            schema:
                A dictionary representing the constituents of the graph
                to be built. These are the "schema" of the ontology
                hierarchy tree.
            model:
                Either a label referring to a built-in model, or a path
                to a YAML file with the model. If not provided, the default
                built-in model will be used.
            use_cache:
                Read the model from the cache if available.
        """

        self.schema = schema
        self.model = model
        self.model_name = None
        self.biolink_schema = None
        self._ad_hoc_inheritance = []

        # mapping functionality for translating terms and queries
        self.mappings = {}
        self.reverse_mappings = {}

        self._use_cache = use_cache

        logger.info('Instantiating Biolink Adapter.')

        self.main()


    def main(self):
        """
        Populate the data structures of the object.
        """

        self._from_cache() or self._load()


    def _load(self):

        logger.info('Building Biolink model (might take a few minutes!).')
        # select with model to use
        self.set_model()
        # initialise biolink toolkit
        self.init_toolkit()
        # translate schema
        self.translate_schema_to_biolink()
        self.update_tree()
        self.save_to_cache()


    def _from_cache(self) -> bool:

        if not self._use_cache:

            return False

        data = _cache.cache.load(self.schema)

        if data:

            logger.info('Loading Biolink model from cache.')

            for attr in self._DATA_ATTRS:

                setattr(self, attr, data[attr])


            self._log_ad_hoc_inheritance(from_ = 'cache')
            self._update_model_version()

            return True

        logger.info('Could not find Biolink model in cache.')

        return False


    def _log_ad_hoc_inheritance(self, from_: str):

        if self._ad_hoc_inheritance:

            msg = (
                f'Ad hoc inheritance (from {from_}):\n\t' +
                '\n\t'.join(
                    f'{k} -> {v}'
                    for k, v in self._ad_hoc_inheritance
                )
            )
            logger.info(msg)


    def save_to_cache(cachedir: str | None = None):
        """
        Save the data currently contained in the object into the cache.
        """

        data = {k: getattr(self, a) for k, a in self._DATA_ATTRS.items()}

        logger.info('Saving Biolink model into cache.')

        _cache.cache.save(obj = data, self.schema, cachedir = cachedir)


    def set_model(self):

        model_builtin = {
            'biocypher': 'biocypher-biolink-model',
            'biolink': 'biolink-model',
        }

        self.model = self.model or 'biocypher'

        self.model_name = (
            self.model if isinstance(self.model, str) else 'custom'
        )

        if self.model in model_builtin:

            label = model_builtin[self.model]
            self.model = module_data_path(label)

    def init_toolkit(self):
        """ """

        # TODO explain: isn't schma_yaml automatically at least
        # 'biocypher' after running set_model? How would we get default?
        # - yes it is, we should default to biocypher, isn't it?
        logger.info(
            f'Creating BioLink model toolkit from `{self.model_name}` model.',
        )

        self.toolkit = (
            bmt.Toolkit(self.model) if self.model else bmt.Toolkit()
        )
        self._update_model_version()


    def _update_model_version(self):

        self.biolink_version = self.toolkit.get_model_version()
        logger.info(f'Bioloink model version: `{self.biolink_version}`.')

    def translate_schema_to_biolink(self):
        """
        Translates the schema (direct constituents of the graph) given
        in the `model_config.yaml` to Biolink-conforming nomenclature.
        Simultaneously get the structure in the form of the parents of
        each leaf.

        Additionally adds child schema for each leaf that has multiple
        identifiers.

        TODO: where do we use sentence case, which is the
        official internal biolink representation, and where do
        we switch to pascal case?
        """

        logger.info('Translating BioCypher config schema to Biolink.')

        self.biolink_schema = {}

        # ontology parents first
        for entity, values in self.schema.items():

            # check whether valid biolink entity should be called by a synonym
            # in the KG (e.g. for readability reasons)
            if not values.get('synonym_for'):
                name_or_synonym = entity
            else:
                name_or_synonym = values['synonym_for']

            entity_biolink_class = self.toolkit.get_element(
                name_or_synonym)  # element name

            if entity_biolink_class:

                # find ancestors of biolink type in PascalCase
                ancestors = self.trim_biolink_ancestry(
                    self.toolkit.get_ancestors(entity, formatted=True),
                )

                if values.get('synonym_for'):
                    # add synonym to ancestors
                    ancestors.insert(0, self.name_sentence_to_pascal(entity))

                input_label = values.get('label_in_input')

                # add translation mappings
                bc_name = (
                    values.get('label_as_edge')
                    if values.get('label_as_edge')
                    else entity
                )
                self._add_translation_mappings(input_label, bc_name)

                # create dict of biolink class definition and biolink
                # ancestors, add to biolink schema
                self.biolink_schema[entity] = {
                    'class_definition': entity_biolink_class,
                    'ancestors': ancestors,
                }

        # secondly check explicit children
        for entity, values in self.schema.items():

            if values.get('is_a') and not values.get('virtual'):

                # build class definition for explicit child
                self._build_biolink_class(entity, values)

        # lastly check virtual schema (implicit children)
        for entity, values in self.schema.items():

            if values.get('virtual'):

                # build class definition for virtual leaf
                self._build_biolink_class(entity, values)

        self._log_ad_hoc_inheritance()


    def translate_term(self, term):
        """
        Translate a single term.
        """

        return self.mappings.get(term, None)

    def reverse_translate_term(self, term):
        """
        Reverse translate a single term.
        """

        return self.reverse_mappings.get(term, None)

    def translate(self, query):
        """
        Translate a cypher query. Only translates labels as of now.
        """
        for key in self.mappings:
            query = query.replace(':' + key, ':' + self.mappings[key])
        return query

    def reverse_translate(self, query):
        """
        Reverse translate a cypher query. Only translates labels as of
        now.
        """
        for key in self.reverse_mappings:

            a = ':' + key + ')'
            b = ':' + key + ']'
            # TODO this conditional probably does not cover all cases
            if a in query or b in query:
                if isinstance(self.reverse_mappings[key], list):
                    raise NotImplementedError(
                        'Reverse translation of multiple inputs not '
                        'implemented yet. Many-to-one mappings are '
                        'not reversible. '
                        f'({key} -> {self.reverse_mappings[key]})',
                    )
                else:
                    query = query.replace(
                        a, ':' + self.reverse_mappings[key] + ')',
                    ).replace(b, ':' + self.reverse_mappings[key] + ']')
        return query

    def _add_translation_mappings(self, original_name, biocypher_name):
        """
        Add translation mappings for a label and name. We use here the
        PascalCase version of the BioCypher name, since sentence case is
        not useful for Cypher queries.
        """

        for on in _misc.to_list(original_name):
            self.mappings[on] = self.name_sentence_to_pascal(
                biocypher_name,
            )

        for bn in _misc.to_list(biocypher_name):
            self.reverse_mappings[
                self.name_sentence_to_pascal(bn)
            ] = original_name

    def _build_biolink_class(self, entity, values):
        """
        Build a Biolink class definition from a Biolink entity name and
        property dict.
        """
        if values.get('represented_as') == 'node':
            return self._build_biolink_node_class(entity, values)
        else:
            return self._build_biolink_edge_class(entity, values)

    def _build_biolink_node_class(self, entity: str, values: dict) -> None:
        """
        Build a Biolink node class definition from a Biolink entity name
        and property dict.
        """

        input_label = values.get('label_in_input')
        parents = _misc.to_list(values.get('is_a'))
        ancestors = []

        logger.info(
            'Received ad hoc multiple inheritance '
            'information; updating pseudo-Biolink node '
            f'by setting `{entity}` as a child of `{parents[0]}`.',
        )

        self._ad_hoc_inheritance.append((parents[0], entity))

        while parents:
            parent = parents.pop(0)
            if self.biolink_schema.get(parent):
                ancestors += self.biolink_schema.get(parent).get('ancestors')
                break
            elif self.toolkit.get_ancestors(parent):
                bla = _misc.to_list(
                    self.trim_biolink_ancestry(
                        self.toolkit.get_ancestors(parent, formatted=True),
                    ),
                )
                ancestors += bla
                break
            else:
                ancestors += [self.name_sentence_to_pascal(parent)]

        if ancestors:
            ancestors.insert(0, self.name_sentence_to_pascal(entity))
        else:
            raise ValueError(
                f'Parent `{parent}` of `{entity}` not found in Biolink '
                'model.',
            )

        # create class definition
        se = ClassDefinition(entity)
        se.is_a = parent
        self.biolink_schema[entity] = {
            'class_definition': se,
            'ancestors': ancestors,
        }

        # add translation mappings
        self._add_translation_mappings(input_label, entity)

    def _build_biolink_edge_class(self, entity: str, values: dict) -> None:
        """
        Build a Biolink edge class definition from a Biolink entity name
        and property dict.
        """

        input_label = values.get('label_in_input')
        parents = _misc.to_list(values.get('is_a'))
        ancestors = []


        logger.info(
            'Received ad hoc multiple inheritance '
            'information; updating pseudo-Biolink edge '
            f'by setting `{entity}` as a child of `{parents[0]}`.',
        )

        self._ad_hoc_inheritance.append((parents[0], entity))

        while parents:
            parent = parents.pop(0)
            if self.biolink_schema.get(parent):
                ancestors += self.biolink_schema.get(parent).get('ancestors')
                break
            elif self.toolkit.get_ancestors(parent):
                bla = _misc.to_list(
                    self.trim_biolink_ancestry(
                        self.toolkit.get_ancestors(parent, formatted=True),
                    ),
                )
                ancestors += bla
                break
            else:
                ancestors += [self.name_sentence_to_pascal(parent)]

        if ancestors:
            ancestors.insert(0, self.name_sentence_to_pascal(entity))
        else:
            raise ValueError(
                f'Parent `{parent}` of `{entity}` not found in Biolink '
                'model.',
            )

        # create class definition
        se = ClassDefinition(entity)
        se.is_a = parent
        self.biolink_schema[entity] = {
            'class_definition': se,
            'ancestors': ancestors,
        }

        # add translation mappings
        bc_name = (
            values.get('label_as_edge')
            if values.get('label_as_edge')
            else entity
        )
        self._add_translation_mappings(input_label, bc_name)

    @staticmethod
    def trim_biolink_ancestry(ancestry: list[str]) -> list[str]:
        """
        Trims "biolink:" prefix from Biolink ancestry elements.
        """

        # replace 'biolink:' with ''
        return [re.sub('^biolink:', '', a) for a in ancestry]

    @staticmethod
    def name_sentence_to_pascal(name: str) -> str:
        """
        Converts a name in sentence case to pascal case.
        """
        # split on dots if dot is present
        if '.' in name:
            return '.'.join(
                [_misc.cc(n) for n in name.split('.')],
            )
        else:
            return _misc.cc(name)


    def update_tree(self):
        """
        Create a tree representation of the ontology including the ancestors
        of the leaves.
        """

        # refactor inheritance tree to be compatible with treelib
        tree = {
            'entity': None,  # root node, named itself to stop while loop
            'mixin': 'entity',
        }

        for class_name, properties in self.biolink_schema.items():

            if isinstance(properties['class_definition'], bmt.TypeDefinition):

                logger.warning(
                    f'Leaf `{class_name}` is a type definition, not a class. '
                    'This is not supported yet.'
                )
                continue

            if properties['class_definition']['is_a'] is not None:

                parent = properties['class_definition']['is_a']

                # add to tree
                tree[class_name] = parent

        # find parents that are not in tree (apart from root node)
        parents = set(tree.values())
        parents.discard(None)
        children = set(tree.keys())

        # while there are still parents that are not in the tree
        while parents - children:
            missing = parents - children

            # add missing parents to tree
            for child in missing:
                parent = self.toolkit.get_parent(child)
                if parent:
                    tree[child] = parent

                # remove root and mixins
                if self.toolkit.is_mixin(child):
                    tree[child] = 'mixin'

            parents = set(tree.values())
            parents.discard(None)
            children = set(tree.keys())

        self._tree = tree


    @property
    def tree(self) -> 'treelib.Tree':
        """
        Ontology tree as an ASCII printable string.
        """

        if not getattr(self, '_tree', None):

            self.update_tree()

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
