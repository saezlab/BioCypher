#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#

from typing import TYPE_CHECKING, Literal
from datetime import datetime
import copy
import json

import yaml

from . import _misc
from . import _config as config
from ._entity import Edge, Node
from ._logger import logger

if TYPE_CHECKING:

    from biocypher._driver import Driver

__all__ = [
    'VersionNode',
]


class VersionNode:
    """
    Versioning and graph structure information meta node.

    Similar to Node but fixes label to ":BioCypher" and sets
    version by using the current date and time (meaning it overrides both
    mandatory args from Node).

    Is created upon establishment of connection with the database and remains
    fixed for each BioCypher "session" (ie, the entire duration from starting
    the connection to the termination of the BioCypher adapter instance). Is
    connected to MetaNodes and MetaEdges via ":CONTAINS" relationships.
    """

    def __init__(
            self,
            offline: bool = False,
            from_config: bool = False,
            config_file: str = None,
            label: str = 'BioCypher',
            id: str | None = None,
            bcy_driver: 'Driver' = None,
    ):
        """
        Create a node with schema and version information.

        Args:
            offline:
                No connection to server.
            from_config:
                Read the parameters from config, instead of an existing
                node in the database.
            config_file:
                Path to config file.
            label:
                Label of the version node.
            bcy_driver:
                A driver instance that supports the connection and already
                carries config data.
        """

        # if we do not have a driver, then likely we are offline, right?
        self.offline = offline or getattr(bcy_driver, 'offline', True)
        self.from_config = from_config
        self.config_file = config_file
        self.label = label
        self.bcy_driver = bcy_driver
        self.id = id or f'v{self._timestamp}'
        self._setup()

    def _setup(self):
        """
        Populates the data structures of this instance.
        """

        self.update_state()
        self.update_schema()

        if self.out_of_sync:

            self._sync()

    def _sync(self):
        """
        Makes sure this instance and the meta graph are in agreement.
        """

        self._new_state()
        self._new_version()

    def _new_state(self):
        """
        Create a new state that reflects the current schema.
        """

        self._state = {
            'id': self.id,
            'previous': self._state.get('id', 'none'),
            'created': self._timestamp,
            'updated': self._timestamp,
            'schema': self._serialize(self.schema),
        }

    def _new_version(self):
        """
        Create a new version node in the database that reflects this instance.
        """

        if self.offline:

            return

        logger.info('Updating biocypher meta graph.')
        # add version node
        self.bcy_driver.add_biocypher_nodes(self)

        if self._state.get('previous', 'none') != 'none':

            precedes = Edge(
                source = self._state['previous'],
                target = self.id,
                label = 'PRECEDES',
            )
            self.bcy_driver.add_biocypher_edges(precedes)

        self.sync_meta()

    def sync_meta(self):
        """
        Makes sure the meta graph has the same structure as the schema.
        """

        if self.offline:

            return

        self.bcy_driver.query('MATCH ()-[r:CONTAINS]-() DELETE r')
        self.bcy_driver.query('MATCH (n:MetaNode) DETACH DELETE n')

        # add structure nodes
        # leaves of the hierarchy specified in schema yaml
        meta_nodes = [
            Node(
                id = entity,
                label = 'MetaNode',
                # id_type = params.get('preferred_id'),
                # props = params,
            )
            for entity, params in self._schema.items()
            if params.get('preferred_id')
        ]

        self.bcy_driver.add_biocypher_nodes(meta_nodes)

        # connect structure nodes to version node
        contains = [
            Edge(
                source = self._state['id'],
                target = entity,
                label = 'CONTAINS',
            )
            for entity in self._schema.keys()
        ]

        self.bcy_driver.add_biocypher_edges(contains)

        # add graph structure between MetaNodes
        meta_rel = [
            Edge(
                source = mn.id,
                target = mn.props.get(side),
                label = f'IS_{side.upper()}_OF',
            )
            for mn in meta_nodes
            for side in ('source', 'target')
            if mn.props.get(side)
        ]

        self.bcy_driver.add_biocypher_edges(meta_rel)

    @property
    def out_of_sync(self):
        """
        The current schema doesn't match the latest existing node.
        """

        return self._state.get('schema') != self._serialize(self.schema)

    def _asdict(self) -> dict:
        """
        Node data for database insertion.

        Returns:
            Data directly suitable for creation of a node in the database.
        """
        return {
            'id': self.id,
            'label': self.label,
            'props': self.props,
        }

    @property
    def _timestamp(self):
        """
        A timestampt that serves as unique ID for the current session.

        Instantiate a version ID for the current session. For now does
        versioning using datetime.

        Can later implement incremental versioning, versioning from
        config file, or manual specification via argument.
        """

        now = datetime.now()
        return now.strftime('%Y%m%d-%H%M%S')

    @property
    def id(self):
        """
        Unique ID of the current session.
        """

        return self._id

    @id.setter
    def id(self, id: str) -> str:
        """
        Unique ID of the current session.
        """

        if hasattr(self, '_id'):

            raise TypeError('Changing `node_id` is not supported.')

        else:

            self._id = id

    @property
    def props(self) -> dict:
        """
        Node properties for database storage.
        """

        props = self.state.copy()
        props['schema'] = self._serialize(self.schema)

        return props

    @staticmethod
    def _serialize(obj) -> str:
        """
        Serialize an object for storage as string.
        """

        return json.dumps(obj, separators = (',', ':'), sort_keys = True)

    @staticmethod
    def _deserialize(s: str) -> list | dict:
        """
        Restore an object from stored string.
        """

        return json.loads(s)

    def update_state(self):
        """
        Set the state using metadata in the database or initialize new state.
        """

        self._state = self.state_from_db()

    @property
    def state(self) -> dict:
        """
        Variables defining the graph state.
        """

        return copy.deepcopy(self._state)

    def state_from_db(self) -> dict:
        """
        Fetch the current state (metadata) from the database if available.

        Check in active DBMS connection for existence of VersionNodes,
        return the most recent VersionNode as representation of the
        graph state.

        Returns:
            All data from the latest version node.
        """

        if self.offline:
            logger.info('Offline mode: no graph state to return.')
            return {}

        if self.bcy_driver:

            logger.info('Getting graph state.')

            result, summary = self.bcy_driver.query(
                f'MATCH (meta:{self.label})'
                f'WHERE NOT (meta)-[:PRECEDES]->(:{self.label})'
                'RETURN meta',
            )

            if result:

                version = result[0]['meta']['id']
                logger.info(f'Found graph state at {version}.')
                return result[0]['meta']

        logger.info('No existing metadata found.')
        return {}

    def update_schema(
        self,
        from_config: bool | None = None,
        config_file: str | None = None,
    ):
        """
        Read the schema either from the graph or from a config file.

        Args:
            from_config:
                Load the schema from the config file even if schema
                in the current database exists.
            config_file:
                Path to a config file. If not provided here or at the
                instance level, the built-in default will be used.
        """

        from_config = _misc.if_none(from_config, self.from_config)

        self._schema = (
            {}
                if from_config else
            self.schema_from_db() or
            self.schema_from_state()
        )

        if not self._schema:

            self._schema = self.schema_from_config(config_file = config_file)
            self._schema = self.find_leaves(schema = self._schema)

    @property
    def schema(self) -> dict:
        """
        Graph schema information.

        From the meta graph if it exists, or create new schema information
        properties from configuration file.
        """

        return copy.deepcopy(self._schema)

    def schema_from_db(self) -> dict:
        """
        Read the schema encoded in the graph meta nodes.
        """

        # TODO do we want information about actual structure here?
        res = self.bcy_driver.query(
            'MATCH (src:MetaNode) '
            # "OPTIONAL MATCH (src)-[r]->(tar)"
            'RETURN src',  # , type(r) AS type, tar"
        )

        return {r['src'].pop('id'): r['src'] for r in res[0]}

    def schema_from_state(self) -> dict:
        """
        Extract the schema from the state of previously existing node.
        """

        return self._deserialize(self._state.get('schema', '{}'))

    def schema_from_config(self, config_file: str | None = None) -> dict:
        """
        Read the schema from a config file.

        Args:
            config_file:
                Path to a config file. If not provided here or at the
                instance level, the built-in default will be used.
        """

        config_file = config_file or self.config_file

        if config_file:

            with open(config_file) as f:

                schema = yaml.safe_load(f)
        else:

            schema = config.module_data('schema_config')

        return schema

    @classmethod
    def find_leaves(cls, schema: dict) -> dict:
        """
        Leaves from schema.

        Args:
            schema:
                Database schema as loaded by ``update_schema``.

        Returns:
            Leaves in the database schema.
        """

        leaves = {}
        schema = copy.deepcopy(schema)

        # first pass: get parent leaves with direct representation in ontology
        leaves = {
            k: v
            for k, v in schema.items()
            if 'is_a' not in v and 'represented_as' in v
        }

        # second pass: "vertical" inheritance
        schema = cls._vertical_property_inheritance(schema = schema)

        # create leaves for all straight descendants (no multiple identifiers
        # or sources) -> explicit children
        # TODO do we need to order children by depth from real leaves?
        leaves.update({k: v for k, v in schema.items() if 'is_a' in v})

        for k, v in schema.items():

            # k is not an entity
            if 'represented_as' not in v:
                continue

            # preferred_id optional: if not provided, use `id`
            if 'preferred_id' not in v:

                v['preferred_id'] = 'id'

            for key in ('preferred_id', 'source'):

                # "horizontal" inheritance: create siblings
                # for multiple identifiers
                # or sources -> virtual leaves or implicit children
                if isinstance(v.get(key), list):

                    leaves.update(
                        cls._horizontal_inheritance(
                            key = k,
                            value = v,
                            by = key,
                        ),
                    )

        return leaves

    @staticmethod
    def _vertical_property_inheritance(schema: dict) -> dict:
        """
        Inherit properties from parents to children.
        """

        def copy_key(d0, d1, key):

            if key in d0:

                d1[key] = d0[key]


        schema = copy.deepcopy(schema)

        for k, v in schema.items():

            # k is not an entity or present in the ontology
            if 'represented_as' not in v or 'is_a' not in v:

                continue

            # "vertical" inheritance: inherit properties from parent
            if v.get('inherit_properties', False):

                # get direct ancestor
                parent = _misc.first(v['is_a'])
                # update properties of child
                copy_key(schema[parent], v, 'properties')
                copy_key(schema[parent], v, 'exclude_properties')

        return schema

    @staticmethod
    def _horizontal_inheritance(
            key: str,
            value: dict,
            by: Literal['source', 'preferred_id'],
    ) -> dict:
        """
        Create virtual leaves for multiple sources or preferred IDs.

        Args:
            key:

        """

        leaves = {}

        variables = (by, 'label_in_input', 'represented_as')

        length = (
            len(value['source'])
                if by == 'source' else
            max(len(_misc.to_list(value[v])) for v in variables)
        )

        values = tuple(
            [value[v]] * length
                if isinstance(value[v], str) else
            value[v]
            for v in variables
        )

        for _by, lab, rep in zip(*values):

            skey = f'{_by}.{key}'
            leaves[skey] = value.copy()
            leaves[skey].update({
                by: _by,
                'label_in_input': lab,
                'represented_as': rep,
                'virtual': True,
                'is_a': [key] + _misc.to_list(value.get('is_a', [])),
            })

        return leaves

    @classmethod
    def _horizontal_inheritance_pid(cls, key: str, value: dict) -> dict:
        """
        Create virtual leaves for multiple preferred id types or sources.

        If we create virtual leaves, label_in_input always has to be a list.
        """

        return cls._horizontal_inheritance(
            key = key,
            value = value,
            by = 'preferred_id',
        )

    @classmethod
    def _horizontal_inheritance_source(cls, key: str, value: dict) -> dict:
        """
        Create virtual leaves for multiple sources.

        If we create virtual leaves, label_in_input always has to be a list.
        """

        return cls._horizontal_inheritance(
            key = key,
            value = value,
            by = 'source',
        )
