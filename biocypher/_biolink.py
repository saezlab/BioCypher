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

from bmt.utils import sentencecase_to_camelcase
from linkml_runtime.linkml_model.meta import ClassDefinition
import bmt

from . import _misc
from ._config import module_data_path

__all__ = ['BiolinkAdapter']


class BiolinkAdapter:
    """
    Performs various functions to integrate the Biolink ontology.

    Stores schema mappings to allow (reverse) translation of terms and
    queries.

    Todo:
        - refer to pythonised biolink model from YAML
    """

    def __init__(
        self,
        schema: dict,
        schema: Literal['biocypher', 'biolink'] | str | dict | None = None,
    ):
        """
        Args:
            schema:
                A dictionary representing the constituents of the graph
                to be built. These are the "schema" of the ontology
                hierarchy tree.
            schema:
                Either a label referring to a built-in schema, or a path
                to a YAML file with the schema. If not provided, the default
                built-in schema will be used.
        """

        self.schema = schema
        self.schema = schema
        self.schema_name = None
        self.biolink_schema = None

        # mapping functionality for translating terms and queries
        self.mappings = {}
        self.reverse_mappings = {}

        logger.debug('Instantiating Biolink Adapter.')

        self.main()

    def main(self):
        # select with schema to use
        self.set_schema()
        # initialise biolink toolkit
        self.init_toolkit()
        # translate schema
        self.translate_schema_to_biolink()

    def set_schema(self):

        schemata_builtin = {
            'biocypher': 'biocypher-biolink-model',
            'biolink': 'biolink-model',
        }

        self.schema = self.schema or 'biocypher'

        self.schema_name = (
            self.schema if isinstance(self.schema, str) else 'custom'
        )

        if self.schema in schemata_builtin:

            label = schemata_builtin[self.schema]
            self.schema = module_data_path(label)

    def init_toolkit(self):
        """ """

        # TODO explain: isn't schma_yaml automatically at least
        # 'biocypher' after running set_schema? How would we get default?
        # - yes it is, we should default to biocypher, isn't it?
        logger.info(
            f'Creating BioLink model toolkit from `{self.schema_name}` model.',
        )

        self.toolkit = (
            bmt.Toolkit(self.schema) if self.schema else bmt.Toolkit()
        )

    def translate_schema_to_biolink(self):
        """
        Translates the schema (direct constituents of the graph) given
        in the `schema_config.yaml` to Biolink-conforming nomenclature.
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

            entity_biolink_class = self.toolkit.get_element(
                entity,
            )  # element name

            if entity_biolink_class:

                # find ancestors of biolink type in PascalCase
                ancestors = self.trim_biolink_ancestry(
                    self.toolkit.get_ancestors(entity, formatted=True),
                )

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
                [sentencecase_to_camelcase(n) for n in name.split('.')],
            )
        else:
            return sentencecase_to_camelcase(name)
