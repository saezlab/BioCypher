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
Lookup and storage of entity IDs that are part of the BioCypher schema.

Todo:

    - genericise: standardise input data to BioCypher specifications or,
      optionally, user specifications.

        - if the database exists, read biocypher info node
        - if newly created, ask for user input (?) as to which IDs to
          use etc
        - default scenario -> YAML?
        - the consensus representation ("target" of translation) is
          the literal Biolink class, which is assigned to database
          content using user input for each class to be represented
          in the graph ("source" of translation). currently,
          implemented by assigning source nomenclature explicitly in
          the schema_config.yaml file ("label_in_input").

    - type checking: use biolink classes for typing directly?

    - import ID types from pypath dictionary (later, externalised
      dictionary)? biolink?
"""
from __future__ import annotations

from collections.abc import Iterable, Generator

from ._logger import logger

logger.debug(f'Loading module {__name__}.')

from typing import Any, Literal
import importlib as imp
import collections

from more_itertools import peekable

from . import _misc
from ._config import config, argconf
from ._entity import BC_TYPES, Edge, Node, RelAsNode

__all__ = ['PROP_SYNONYMS', 'Translator']

PROP_SYNONYMS = {
    'licence': 'license',
}


class Translator:
    """
    Translate components to their biocypher representations.

    Executes the translation process as it is  configured in the
    schema_config.yaml file. Creates a mapping dictionary from that file,
    and, given nodes and edges, translates them into BioCypherNodes and
    BioCypherEdges. During this process, can also filter the properties of the
    entities if the schema_config.yaml file specifies a property whitelist or
    blacklist.

    Provides utility functions for translating between input and output labels
    and cypher queries.
    """

    INPUT_TYPES = (
        tuple[str, str, dict] |
        tuple[str, str, str, dict] |
        tuple[str, str, str, str, dict]
    )

    def __init__(
            self,
            schema: dict[str, dict],
            strict_mode: bool | None = None,
    ):
        """
        Create a translator object for one database schema.

        Args:
            schema:
                Dictionary detailing the schema of the hierarchy
                tree representing the structure of the graph; the schema are
                the entities that will be direct components of the graph,
                while the intermediary nodes are additional labels for
                filtering purposes.
            strict_mode:
                Fail on missing mandatory properties.
        """

        self._required_props = _misc.to_set(config('required_props') or ())
        self.strict_mode = argconf('strict_mode')
        self.schema = schema
        self._update_ontology_types()

        # record nodes without biolink type configured in schema_config.yaml
        self.notype = collections.defaultdict(int)

        # mapping functionality for translating terms and queries
        self.mappings = {}
        self.reverse_mappings = {}

    def reload(self):
        """
        Reloads the object from the module level.
        """

        modname = self.__class__.__module__
        mod = __import__(modname, fromlist = [modname.split('.')[0]])
        imp.reload(mod)
        new = getattr(mod, self.__class__.__name__)
        setattr(self, '__class__', new)

    def translate(
            self,
            items: INPUT_TYPES | BC_TYPES | Iterable[INPUT_TYPES | BC_TYPES],
    ) -> Generator[BC_TYPES, None, None]:
        """
        Translate graph components to the current schema.

        Translates input node representation to a representation that
        conforms to the schema of the given BioCypher graph. For now
        requires explicit statement of node type on pass.

        Args:
            items:
                Tuples representing graph components.

        Yields:
            Graph components as objects that are suitable to be inserted
            into the database.
        """

        self._log_begin_translate(items, 'components')

        items = peekable(items)
        first = items.peek()

        if isinstance(first, _misc.SIMPLE_TYPES):

            items = (first,)

        for i in items:

            bc_item = (
                i
                    if isinstance(i, BC_TYPES) else
                self.node(*i)
                    if len(i) < 4 else
                self.edge(*i)
                    if len(i) == 4 else
                self.edge(*i[1:], _id = i[0])
            )

            if bc_item: yield bc_item

        self._log_finish_translate('components')

    def edge(
            self,
            source: str,
            target: str,
            _type: str,
            props: dict,
            _id: str = None,
    ) -> Edge | RelAsNode | None:
        """
        Creates one Edge.

        Args:
            source:
                ID of the source node.
            target:
                ID of the target node.
            _type:
                Type of the entity represented by the edge.
            props:
                Arbitrary properties.
            _id:
                ID property of the edge. If not provided, the source,
                target, type and all properties will be concatenated to
                create a unique ID for the edge. Used only if the relation
                is represented as a node.

        Returns:
            An edge in BioCypher representation, if the entity type can be
            found in the schema.
        """

        # first, check strict mode properties
        if self.strict_mode:

            self._check_strict_props(props, _type)

        # match the input label (_type) to
        # a Biolink label from schema_config
        ontology_class = self._get_ontology_mapping(_type)

        if not ontology_class:

            self._record_no_type(_type, (source, target))

        else:

            filtered_props = self._filter_props(ontology_class, props)
            rep = self.schema[ontology_class]['represented_as']

            if rep == 'node':

                return self._rel_as_node(
                    source = source,
                    target = target,
                    ontology_class = ontology_class,
                    _id = _id,
                    props = filtered_props,
                )

            edge_label = (
                self.schema[ontology_class].get('label_as_edge') or
                ontology_class
            )

            return Edge(
                source = source,
                target = target,
                label = edge_label,
                props = filtered_props,
                id = _id,
            )

    def _rel_as_node(
            self,
            source: str,
            target: str,
            ontology_class: str,
            props: dict,
            _id: str | None = None,
    ) -> RelAsNode:
        """
        Create node representation of a record represented by edge by default.

        Args:
            source:
                ID of the source node.
            target:
                ID of the target node.
            ontology_class:
                The ontology node to be used as node label.
            props:
                Arbitrary properties, already filtered by ``_filter_props``.
            _id:
                ID property of the node. If not provided, the source,
                target, type and all properties will be concatenated to
                create a unique ID for the edge.

        Returns:
            A triplet of one node and two edges in BioCypher representation.
        """

        # first, check strict mode properties
        if self.strict_mode:

            self._check_strict_props(props, ontology_class)

        if _id:
            # if it brings its own ID, use it
            node_id = _id

        else:

            props_str = _misc.dict_str(dct = props, sep = '_')
            # source target concat
            node_id = f'{source}_{target}_{props_str}'

        n = Node(
            id = node_id,
            label = ontology_class,
            props = props,
        )

        # directionality check TODO generalise to account for
        # different descriptions of directionality or find a
        # more consistent solution for indicating directionality
        if props.get('directed'):

            reltype1 = 'IS_SOURCE_OF'
            reltype2 = 'IS_TARGET_OF'

        else:

            reltype1 = props.get('src_role') or 'IS_PART_OF'
            reltype2 = props.get('tar_role') or 'IS_PART_OF'

        e_s = Edge(
            source = source,
            target = node_id,
            label = reltype1,
        )

        e_t = Edge(
            source = target,
            target = node_id,
            label = reltype2,
        )

        return RelAsNode(node = n, source = e_s, target = e_t)


    def node(
            self,
            _id: str,
            _type: str,
            props: dict,
    ) -> Node | None:
        """
        Creates one Node.

        Args:
            _id:
                The node ID.
            _type:
                Type of the represented entity.
            props:
                Arbitrary properties.

        Returns:
            A node in BioCypher representation, if the entity type can be
            found in the schema.
        """

        # first check for strict mode properties
        if self.strict_mode:

            self._check_strict_props(props, _type)

        # find the node in schema that represents biolink node type
        ontology_class = self._get_ontology_mapping(_type)

        if not ontology_class:

            self._record_no_type(_type, _id)

        else:

            # filter properties for those specified in schema_config if any
            filtered_props = self._filter_props(ontology_class, props)
            id_type = self._id_type(ontology_class)

            return Node(
                id = _id,
                label = ontology_class,
                id_type = id_type,
                props = filtered_props,
            )

    def _check_strict_props(self, props: dict, _type: str) -> None:
        """
        Checks if all required properties are present in the record. Raises
        value error if not.
        """

        # rename 'license' to 'licence' if present
        if 'license' in props:

            props['licence'] = props.pop('license')

        # which of _required_props are missing in props?
        missing_keys = self._required_props - set(props.keys())

        if missing_keys:
            err = (
                'Missing mandatory properties for entity of type '
                f'{_type}: {", ".join(missing_keys)}'
            )
            logger.error(err)
            raise ValueError(err)

    def _id_type(self, _ontology_class: str) -> str:
        """
        Returns the preferred id for the given Biolink type.
        """

        return self.schema.get(_ontology_class, {}).get('id_type', 'id')


    @staticmethod
    def _property_synonyms(props: dict[str, str]) -> dict[str, str]:

        return {PROP_SYNONYMS.get(k, k): v for k, v in props.items()}


    def _filter_props(self, ontology_class: str, props: dict) -> dict:
        """
        Filters properties for those specified in schema_config if any.
        """

        filter_props = self.schema[ontology_class].get('properties', {})

        if self.strict_mode:

            filter_props.update({p: 'str' for p in self._required_props})

        exclude_props = set(
            _misc.to_list(
                self.schema[ontology_class].get('exclude_properties', []),
            ),
        )

        props = self._property_synonyms(props)

        prop_keys = (
            (set(props.keys()) - exclude_props) &
            set(filter_props.keys())
        )

        props = {
            k: props[k]
            for k in prop_keys
            if props[k].__class__.__name__ == filter_props[k]
        }

        missing_keys = (
            set(filter_props.keys()) -
            exclude_props -
            set(props.keys())
        )

        # # due to _check_strict_props, this may not be needed any more, here
        # if self.strict_mode and missing_keys & self._required_props:
        #     missing_required = missing_keys & self._required_props
        #     err = (
        #         'Missing mandatory properties for entity of BioLink type '
        #         f'{ontology_class}: {", ".join(missing_required)}'
        #     )
        #     logger.error(err)
        #     raise ValueError(err)

        # add missing properties with default values
        props.update({k: None for k in missing_keys})

        return props


    def _record_no_type(self, _type: Any, what: Any) -> None:
        """
        Records the type of a node or edge that is not represented in the
        schema_config.
        """

        logger.debug(f'No Biolink type defined for `{_type}`: {what}')

        self.notype[_type] += 1


    def get_missing_ontology_classes(self) -> dict:
        """
        Returns a dictionary of types that were not represented in the
        schema_config.
        """

        return self.notype


    @staticmethod
    def _log_begin_translate(_input: Iterable, what: str):

        n = f'{len(_input)} ' if hasattr(_input, '__len__') else ''

        logger.debug(f'Translating {n}{what} to BioCypher')


    @staticmethod
    def _log_finish_translate(what: str):

        logger.debug(f'Finished translating {what} to BioCypher.')


    def _update_ontology_types(self):
        """
        Creates a dictionary to translate from input labels to ontology labels.

        If multiple input labels, creates mapping for each.
        """

        self._ontology_mapping = {}

        for key, value in self.schema.items():

            if isinstance(value.get('label_in_input'), str):
                self._ontology_mapping[value.get('label_in_input')] = key

            elif isinstance(value.get('label_in_input'), list):
                for label in value['label_in_input']:
                    self._ontology_mapping[label] = key


    def _get_ontology_mapping(self, label: str) -> str | None:
        """
        For each given input type ("label_in_input"), find the corresponding
        Biolink type in the schema dictionary (from the `schema_config.yam`).

        Args:
            label:
                The input type to find (`label_in_input` in
                `schema_config.yaml`).
        """

        return self._ontology_mapping.get(label, None)


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

    def translate_cypher(self, query):
        """
        Translate a cypher query. Only translates labels as of now.
        """

        for key in self.mappings:

            query = query.replace(f':{key}', f':{self.mappings[key]}')

        return query

    def reverse_translate_cypher(self, query):
        """
        Reverse translate a cypher query.

        Only translates labels as of now.
        """

        for key in self.reverse_mappings:

            key_par = ':' + key + ')'
            key_sqb = ':' + key + ']'

            # TODO this conditional probably does not cover all cases
            if key_par in query or key_sqb in query:

                rev_key = self.reverse_mappings[key]

                if isinstance(rev_key, list):

                    raise NotImplementedError(
                        'Reverse translation of multiple inputs not '
                        'implemented yet. Many-to-one mappings are '
                        'not reversible. '
                        f'({key} -> {rev_key})',
                    )

                else:

                    query = query.replace(key_par, f':{rev_key})')
                    query = query.replace(key_sqb, f':{rev_key}]')

        return query

    def _add_translation_mappings(self, original_name, biocypher_name):
        """
        Add translation mappings for a label and name. We use here the
        PascalCase version of the BioCypher name, since sentence case is
        not useful for Cypher queries.
        """

        for on in _misc.to_list(original_name):

            self.mappings[on] = self.name_sentence_to_pascal(biocypher_name)

        for bn in _misc.to_list(biocypher_name):

            name = self.name_sentence_to_pascal(bn)
            self.reverse_mappings[name] = original_name

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
