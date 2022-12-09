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

from ._logger import logger

logger.debug(f'Loading module {__name__}.')

from typing import Any, Generator, Iterable, Literal
import collections
import importlib as imp

from more_itertools import peekable

from . import _misc
from ._entity import BC_TYPES, Edge, Node, RelAsNode
from ._config import config, argconf

__all__ = ['Translator']

PROP_SYNONYMS = {
    'licence': 'license',
}


class Translator:
    """
    Translate components to their biocypher representations.
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

        self._required_props = config('required_props')
        self.strict_mode = argconf('strict_mode')
        self.schema = schema
        self._update_bl_types()

        # record nodes without biolink type configured in schema_config.yaml
        self.notype = collections.defaultdict(int)

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

        # match the input label (_type) to
        # a Biolink label from schema_config
        bl_type = self._get_bl_type(_type)

        if not bl_type:

            self._record_no_type(_type, (source, target))

        else:

            filtered_props = self._filter_props(bl_type, props)
            rep = self.schema[bl_type]['represented_as']

            if rep == 'node':

                return self._rel_as_node(
                    source = source,
                    target = target,
                    bl_type = bl_type,
                    _id = _id,
                    props = filtered_props,
                )

            edge_label = self.schema[bl_type].get('label_as_edge') or bl_type

            return Edge(
                source = source,
                target = target,
                label = edge_label,
                props = filtered_props,
            )

    def _rel_as_node(
            self,
            source: str,
            target: str,
            bl_type: str,
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
            bl_type:
                The Biolink type to be used as node label.
            props:
                Arbitrary properties, already filtered by ``_filter_props``.
            _id:
                ID property of the node. If not provided, the source,
                target, type and all properties will be concatenated to
                create a unique ID for the edge.

        Returns:
            A triplet of one node and two edges in BioCypher representation.
        """

        if _id:
            # if it brings its own ID, use it
            node_id = _id

        else:

            props_str = _misc.dict_str(dct = props, sep = '_')
            # source target concat
            node_id = f'{source}_{target}_{props_str}'

        n = Node(
            id = node_id,
            label = bl_type,
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

        # find the node in schema that represents biolink node type
        bl_type = self._get_bl_type(_type)

        if not bl_type:

            self._record_no_type(_type, _id)

        else:

            # filter properties for those specified in schema_config if any
            filtered_props = self._filter_props(bl_type, props)
            id_type = self._id_type(bl_type)

            return Node(
                id = _id,
                label = bl_type,
                id_type = id_type,
                props = filtered_props,
            )


    def _id_type(self, _bl_type: str) -> str:
        """
        Returns the preferred id for the given Biolink type.
        """

        return self.schema.get(_bl_type, {}).get('id_type', 'id')


    def _property_synonyms(props: dict[str, str]) -> dict[str, str]:

        return {PROP_SYNONYMS.get(k, k): v for k, v in props.items()}


    def _filter_props(self, bl_type: str, props: dict) -> dict:
        """
        Filters properties for those specified in schema_config if any.
        """

        filter_props = self.schema[bl_type].get('properties', {})

        if self.strict_mode:

            filter_props.update({p: 'str' for p in self._required_props})

        exclude_props = set(
            _misc.to_list(
                self.schema[bl_type].get('exclude_properties', []),
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

        if self.strict_mode and missing_keys & self._required_props:

            missing_required = missing_keys & self._required_props
            err = (
                'Missing mandatory properties for entity of BioLink type '
                f'{bl_type}: {", ".join(missing_required)}'
            )
            logger.error(err)
            raise ValueError(err)

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

    def get_missing_bl_types(self) -> dict:
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

    def _update_bl_types(self):
        """
        Creates a dictionary to translate from input labels to Biolink labels.

        If multiple input labels, creates mapping for each.
        """

        self._bl_types = {}

        for key, value in self.schema.items():

            if isinstance(value.get('label_in_input'), str):
                self._bl_types[value.get('label_in_input')] = key

            elif isinstance(value.get('label_in_input'), list):
                for label in value['label_in_input']:
                    self._bl_types[label] = key

    def _get_bl_type(self, label: str) -> str | None:
        """
        For each given input type ("label_in_input"), find the corresponding
        Biolink type in the schema dictionary.

        Args:
            label:
                The input type to find (`label_in_input` in
                `schema_config.yaml`).
        """

        # commented out until behaviour of _update_bl_types is fixed
        return self._bl_types.get(label, None)
