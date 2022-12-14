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

"""
Biocypher specific database management and access methods.
"""
from collections.abc import Iterable

from ._logger import logger

logger.debug(f'Loading module {__name__}.')

from typing import TYPE_CHECKING, Any, Optional
import inspect
import itertools
import collections

import more_itertools as mit

if TYPE_CHECKING:

    import neo4j

import neo4j_utils

from . import _misc
from ._meta import VersionNode
from ._write import ENTITIES, BatchWriter
from ._config import config as _config
from ._config import argconf as _argconf
from ._config import neo4j_config as _neo4j_config
from ._entity import BC_TYPES, Edge, Node
from ._biolink import BiolinkAdapter
from ._ontology import OntologyAdapter
from ._translate import Translator

__all__ = ['Driver']

INPUT_BC_TYPES = Translator.INPUT_TYPES | BC_TYPES


class Driver(neo4j_utils.Driver):
    """
    Manages a connection to a biocypher database.
    """

    def __init__(
        self,
        driver: neo4j.Driver = None,
        db_name: str | None = None,
        db_uri: str | None = None,
        db_user: str | None = None,
        db_passwd: str | None = None,
        multi_db: bool | None = None,
        fetch_size: int = 1000,
        raise_errors: bool | None = None,
        wipe: bool = False,
        strict_mode: bool | None = None,
        offline: Optional[bool] = None,
        output_directory: str | None = None,
        increment_version: bool = True,
        user_schema_config_path: str | None = None,
        clear_cache: bool | None = None,
        delimiter: str | None = None,
        array_delimiter: str | None = None,
        quote_char: str | None = None,
        skip_bad_relationships: bool = False,
        skip_duplicate_nodes: bool = False,
        biolink_model: dict | str | None = None,
        biolink_use_cache: bool = True,
        tail_ontology_url: str | None = None,
        head_join_node: str | None = None,
        tail_join_node: str | None = None,
    ):
        """
        Set up a BioCypher database connection.

        The connection can be defined in three ways:
        * Providing a ready ``neo4j.Driver`` instance
        * By URI and authentication data
        * By a YAML config file

        Args:
            driver:
                A ``neo4j.Driver`` instance, created by, for example,
                ``neo4j.GraphDatabase.driver``.
            db_name:
                Name of the database (Neo4j graph) to use.
            db_uri:
                Protocol, host and port to access the Neo4j server.
            db_user:
                Neo4j user name.
            db_passwd:
                Password of the Neo4j user.
            fetch_size:
                Optional; the fetch size to use in database transactions.
            wipe:
                Wipe the database after connection, ensuring the data is
                loaded into an empty database.
            offline:
                Do not connect to the database, but use the provided
                schema to create a graph representation and write CSVs for
                admin import.
            output_directory:
                Directory to write CSV files to.
            strict_mode:
                Fail on missing mandatory properties.
            schema_config:
                Path to a custom database schema configuration file.
            delimiter:
                Delimiter for CSV export.
            array_delimiter:
                Array delimiter for CSV exported contents.
            quote_char:
                String quotation character for CSV export.
            skip_bad_relationships:
                Whether to skip relationships with missing source or target
                nodes in the admin import shell command.
            skip_duplicate_nodes:
                Whether to skip duplicate nodes in the admin import shell
                command.
            biolink_model:
                Either a Biolink model as a dict, or the name of a
                built in model, or path to the model YAML file to load.
            biolink_use_cache:
                Load the Biolink model from cache, if available.
            tail_ontology_url:
                URL of the ontology to hybridise to the head ontology.
            head_join_node:
                Biolink class of the node to join the tail ontology to.
            tail_join_node:
                Ontology class of the node to join the head ontology to.
        """

        neo4j_config = _neo4j_config()
        driver_args = {
            arg: _misc.if_none(locals().get(arg), _neo4j_config.get(arg))
            for arg in inspect.signature(neo4j_utils.Driver).parameters
        }

        self.csv_delim = delimiter or _config('csv_delimiter')
        self.csv_adelim = array_delimiter or _config('csv_array_delimiter')
        self.csv_quote = quote_char or _config('csv_quote_char')
        self.wipe = wipe
        self.skip_bad_relationships = skip_bad_relationships
        self.skip_duplicate_nodes = skip_duplicate_nodes

        # BioCypher options
        self.strict_mode = _argconf('strict_mode')
        self.output_directory = _argconf('output_directory')
        self.user_schema_config_path = _argconf('user_schema_config_path')
        self.clear_cache = _argconf('clear_cache')
        self._biolink_use_cache = biolink_use_cache
        self.tail_ontology_url = _argconf('tail_ontology_url')
        self.head_join_node = _argconf('head_join_node')
        self.tail_join_node = _argconf('tail_join_node')

        neo4j_utils.Driver.__init__(self, **driver_args)

        self._init_version_node()

        # likely this will be refactored soon
        self._create_constraints()

        self.ontology_adapter = None
        self.batch_writer = None
        self._update_translator()
        self._reset_insert_buffer()

        # TODO: implement passing a driver instance
        # Denes: I am not sure, but seems like it works already
        # by the base class

    def _init_version_node(self):

        if self.offline and not self.user_schema_config_path:

            raise ValueError(
                'Offline mode requires a schema configuration. '
                'Please provide one with the `user_schema_config_path` '
                'argument or set the `user_schema_config_path` '
                'configuration variable.'
            )

        # if db representation node does not exist or explicitly
        # asked for wipe, create new graph representation: default
        # yaml, interactive?
        # Denes: those are two different cases, if it's wiped, first
        # its contents should be read, if it does not exist, a new one
        # should be created.

        # get database version node ('check' module) immutable
        # variable of each instance (ie, each call from the
        # adapter to BioCypher); checks for existence of graph
        # representation and returns if found, else creates new
        # one
        self.db_meta = VersionNode(
            from_config=self.offline or self.wipe,
            config_file=self.user_schema_config_path,
            offline=self.offline,
            bcy_driver=self,
        )

    def _update_translator(self):

        self.translator = Translator(
            schema = self.db_meta.schema,
            strict_mode = self.strict_mdoe,
        )

    def _reset_insert_buffer(self):
        """
        The graph components queue here before insertion in batches.
        """

        self.flush()
        self._insert_buffer = collections.defaultdict(list)
        self._inserts = 0

    def _insert_queue(self, item: BC_TYPES):
        """
        Adds an item to the insert buffer, where it will stay until being
        inserted.
        """

        self._insert_buffer[item.key].append(item)
        self._inserts += 1

        if self._inserts % 1000 == 0:

            self._process_queue()

    def _process_queue(self):
        """
        Checks for full queues in the insert buffer and inserts their contents.
        """

        self.flush(batch_size = _config('insert_batch_size'))


    def init_db(self):
        """
        Wipes the database and creates constraints.

        Used to initialise a property graph database by deleting
        contents and constraints and setting up new constraints.

        Todo:
            - Set up constraint creation interactively depending on the
              need of the database
        """

        self.wipe_db()
        self._create_constraints()
        logger.info('Initialising database.')

    def _create_constraints(self):
        """
        Creates constraints on node types in the graph.

        Used for initial setup. Grabs leaves of the ``schema_config.yaml``
        file and creates constraints on the id of all entities represented as
        nodes.
        """

        logger.info('Creating constraints for node types in config.')

        # get structure
        for leaf in self.db_meta.schema.items():

            if leaf[1]['represented_as'] == 'node':

                label_cc = _misc.cc(leaf[0])
                label_sc = _misc.sc(leaf[0])
                s = (
                    f'CREATE CONSTRAINT `{label_sc}_id` '
                    f'IF NOT EXISTS ON (n:`{label_cc}`) '
                    'ASSERT n.id IS UNIQUE'
                )
                self.query(s)

    def add(self, items: Iterable[INPUT_BC_TYPES] | INPUT_BC_TYPES):
        """
        Add components to the database.

        Here first we translate the items to biocypher's representation and
        then insert them into the database.

        Args:
            items:
                Nodes and edges to be added to the database; can be anything
                suitable for :py:class:``Translator.translate``, or the
                objects from :py:mod:``biocypher._entity``.
        """

        for it in self.translator.translate(items):

            self._insert_queue(it)

    def __iadd__(self, other):

        self.add(other)

    def __add__(self, other):

        self.add(other)

        return self

    def flush(self, batch_size: int = 0):
        """
        Write out the contents of the insert buffer.

        Args:
            batch_size:
                Minimum length for queues to be flushed.
        """

        insert_buffer = getattr(self, '_insert_buffer', {})

        for (label, entity), queue in insert_buffer.items():

            if len(queue) > batch_size:

                getattr(self, f'add_{entity}s')(queue)

                self._inserts -= len(queue)

    def add_nodes(
            self,
            nodes: Iterable[
                tuple[
                    str,
                    str,
                    dict[str, Any],
                ]
            ],
    ) -> tuple:
        """
        Translate nodes and write them into the database.

        Generic node adder method to add any kind of input to the
        graph via the :class:`biocypher.create.Node` class. Employs
        translation functionality and calls the :meth:`add_biocypher_nodes()`
        method.

        Args:
            nodes:
                For each node to add to the biocypher graph, a 3-tuple with
                the following layout:
                * The (unique if constrained) ID of the node.
                * The type of the node, capitalised or PascalCase and in noun
                  form (Neo4j primary label, eg `:Protein`).
                * A dictionary of arbitrary properties the node should
                  possess (can be empty).

        Returns:
            2-tuple: the query result of :meth:`add_biocypher_nodes()`
            - first entry: data
            - second entry: Neo4j summary.
        """

        bn = self.translator.translate(nodes)
        return self.add_biocypher_nodes(bn)

    def add_edges(
            self,
            edges: Iterable[
                tuple[
                    str | None,
                    str,
                    str,
                    str,
                    dict[str, Any],
                ]
            ],
    ) -> tuple:
        """
        Translate edges and write them into the database.

        Generic edge adder method to add any kind of input to the graph
        via the :class:`biocypher.create.Edge` class. Employs
        translation functionality and calls the
        :meth:`add_biocypher_edges()` method.

        Args:
            edges:
                For each edge to add to the biocypher graph, a 5-tuple
                with the following layout:
                * The optional unique ID of the interaction. This can be
                  `None` if there is no systematic identifier (which for
                  many interactions is the case).
                * The (unique if constrained) ID of the source node of the
                  relationship.
                * Same for the target node.
                * The type of the relationship.
                * A dictionary of arbitrary properties the edge should
                  possess (can be empty).

        Returns:
            2-tuple: the query result of :meth:`add_biocypher_edges()`
            - first entry: data
            - second entry: Neo4j summary.
        """

        be = self.translator.translate(edges)
        return self.add_biocypher_edges(be)

    def add_biocypher_nodes(
            self,
            nodes: Iterable[Node],
            explain: bool = False,
            profile: bool = False,
    ) -> bool:
        """
        Write nodes into the database.

        Accepts a node type handoff class
        (:class:`biocypher.create.Node`) with id,
        label, and a dict of properties (passing on the type of
        property, ie, ``int``, ``str``, ...).

        The dict retrieved by the
        :meth:`biocypher.create.Node._asdict()` method is
        passed into Neo4j as a map of maps, explicitly encoding node id
        and label, and adding all other properties from the 'properties'
        key of the dict. The merge is performed via APOC, matching only
        on node id to prevent duplicates. The same properties are set on
        match and on create, irrespective of the actual event.

        Args:
            nodes:
                An iterable of :class:`biocypher.create.Node` objects.
            explain:
                Call ``EXPLAIN`` on the CYPHER query.
            profile:
                Do profiling on the CYPHER query.

        Returns:
            `True` for success, `False` otherwise.
        """

        try:

            entities = [
                node._asdict() for node in _misc.ensure_iterable_2(nodes)
            ]

        except AttributeError as e:

            msg = f'Nodes must have a `_asdict` method: {str(e)}'
            logger.error(msg)

            raise TypeError(msg)

        logger.info(f'Merging {len(entities)} nodes.')

        entity_query = (
            'UNWIND $entities AS ent '
            'CALL apoc.merge.node([ent.label], '
            '{id: ent.id}, ent.props, ent.props) '
            'YIELD node '
            'RETURN node'
        )

        method = 'explain' if explain else 'profile' if profile else 'query'

        result = getattr(self, method)(
            entity_query,
            parameters={'entities': entities},
        )

        logger.info('Finished merging nodes.')

        return result

    def add_biocypher_edges(
            self,
            edges: Iterable[Edge],
            explain: bool = False,
            profile: bool = False,
    ) -> bool:
        """
        Write edges into the database.

        Accepts an edge type handoff class
        (:class:`biocypher.create.Edge`) with source
        and target ids, label, and a dict of properties (passing on the
        type of property, ie, int, string ...).

        The individual edge is either passed as a singleton, in the case
        of representation as an edge in the graph, or as a 4-tuple, in
        the case of representation as a node (with two edges connecting
        to interaction partners).

        The dict retrieved by the
        :meth:`biocypher.create.Edge._asdict()` method is
        passed into Neo4j as a map of maps, explicitly encoding source
        and target ids and the relationship label, and adding all edge
        properties from the 'properties' key of the dict. The merge is
        performed via APOC, matching only on source and target id to
        prevent duplicates. The same properties are set on match and on
        create, irrespective of the actual event.

        Args:
            edges:
                An iterable of :class:`biocypher.create.Edge` objects.
            explain:
                Call ``EXPLAIN`` on the CYPHER query.
            profile:
                Do profiling on the CYPHER query.

        Returns:
            `True` for success, `False` otherwise.
        """

        edges = _misc.ensure_iterable_2(edges)
        edges = itertools.chain(*(_misc.ensure_iterable_2(i) for i in edges))

        nodes = []
        rels = []

        try:

            for e in edges:

                if hasattr(e, 'node'):

                    nodes.append(e.node)
                    rels.append(e.source._asdict())
                    rels.append(e.target._asdict())

                else:

                    rels.append(e._asdict())

        except AttributeError as e:

            msg = f'Edges and nodes must have a `_asdict` method: {str(e)}'
            logger.error(msg)

            raise TypeError(msg)

        self.add_biocypher_nodes(nodes)
        logger.info(f'Merging {len(rels)} edges.')

        # cypher query

        # merging only on the ids of the entities, passing the
        # properties on match and on create;
        # TODO add node labels?
        node_query = (
            'UNWIND $rels AS r '
            'MERGE (src {id: r.source}) '
            'MERGE (tar {id: r.target}) '
        )

        self.query(node_query, parameters={'rels': rels})

        edge_query = (
            'UNWIND $rels AS r '
            'MATCH (src {id: r.source}) '
            'MATCH (tar {id: r.target}) '
            'WITH src, tar, r '
            'CALL apoc.merge.relationship'
            '(src, r.label, NULL, '
            'r.props, tar, r.props) '
            'YIELD rel '
            'RETURN rel'
        )

        method = 'explain' if explain else 'profile' if profile else 'query'

        result = getattr(self, method)(edge_query, parameters={'rels': rels})

        logger.info('Finished merging edges.')

        return result


    def write_csv(
            self,
            items: Iterable[INPUT_BC_TYPES],
            dirname: str | None = None,
            db_name: str | None = None,
    ) -> bool:
        """
        Compile graph components and write them into CSV files.

        Here first we translate the items to biocypher's representation and
        from there we compile CSV files that correspond to the current
        database schema and suitable for *neo4j-admin import*.

        Args:
            items:
                Nodes and edges to be written in BioCypher-compatible CSV
                format; can be anything suitable for
                :py:class:``Translator.translate``, or the objects from
                :py:mod:``biocypher._entity``.
            dirname:
                Directory for CSV output files.
            db_name:
                Name of a Neo4j database. Used only for the CLI call of the
                *neo4j-admin import* command that is created by
                :py:class:``BatchWriter``. This command is not executed by
                biocypher, hence the database name can be easily modified
                after the export.

        Returns:
            Whether the write was successful.
        """

        # instantiate adapter on demand because it takes time to load
        # the biolink model toolkit
        self.start_ontology_adapter()
        self.start_batch_writer(dirname, db_name)

        items = self.translator.translate(items)

        # write
        return self.batch_writer.write(items)

    def start_batch_writer(
            self,
            dirname: str | None,
            db_name: str | None,
        ) -> None:
        """
        Instantiate the batch writer if it does not exist.

        Args:
            dirname:
                The directory to write the files to.
            db_name:
                The name of the database to write the files to.
        """

        dirname = dirname or self.output_directory

        if not self.batch_writer:

            self.batch_writer = BatchWriter(
                schema=self.db_meta.schema,
                ontology_adapter=self.ontology_adapter,
                translator=self.translator,
                delimiter=self.csv_delim,
                array_delimiter=self.csv_adelim,
                quote=self.csv_quote,
                dirname=dirname,
                db_name=db_name or self.current_db,
                skip_bad_relationships=self.skip_bad_relationships,
                skip_duplicate_nodes=self.skip_duplicate_nodes,
                wipe=self.wipe,
            )

        self.batch_writer.set_outdir(dirname)
        self.batch_writer.db_name = db_name


    def start_ontology_adapter(self):
        """
        Makes sure a Biolink adapter is available.

        Instantiate the :class:`biocypher._ontology.OntologyAdapter`.

        Attributes:
            ontology_adapter:
                An instance of :class:`biocypher._ontology.OntologyAdapter`.
        """

        if not self.ontology_adapter:

            biolink_adapter = BiolinkAdapter(
                schema = self.db_meta.schema,
                model = self._biolink_model,
                use_cache = self._biolink_use_cache,
                translator=self.translator,
                clear_cache=self.clear_cache,
            )
            # only simple one-hybrid case; TODO generalise
            self.ontology_adapter = OntologyAdapter(
                tail_ontology_url=self.tail_ontology_url,
                head_join_node=self.head_join_node,
                tail_join_node=self.tail_join_node,
                biolink_adapter=biolink_adapter,
            )


    def get_import_call(self) -> str:

        """
        Create a *neo4j-admin* CLI call that imports the generated CSV files.

        Upon using the batch writer for writing admin import CSV files,
        return a string containing the neo4j admin import call with
        delimiters, database name, and paths of node and edge files.

        Returns:
            A *neo4j-admin* import call.
        """

        return self.batch_writer.get_import_call()


    def write_import_call(self) -> bool:
        """
        Write the *neo4j-admin* CLI call into a file.

        Upon using the batch writer for writing admin import CSV files,
        write a string containing the neo4j admin import call with
        delimiters, database name, and paths of node and edge files, to
        the export directory.

        Returns:
            The write was successful.
        """

        return self.batch_writer.write_import_call()


    def log_missing_bl_types(self) -> Optional[set[str]]:
        """
        Send log message about Biolink types missing from the schema config.

        Get the set of Biolink types encountered without an entry in
        the `schema_config.yaml` and print them to the logger.

        Returns:
            A set of missing Biolink types
        """

        missing = self.translator.get_missing_bl_types()

        if missing:

            msg = (
                'Input entities not accounted for due to them not being '
                'present in the `schema_config.yaml` configuration file '
                '(not necessarily a problem, if you do not want to include '
                'them in the database): \n'
            )

            for k, v in missing.items():

                msg += f'    {k}: {v} \n'

            logger.warning(msg)

        else:

            logger.info('No missing Biolink types in input.')

        return missing


    def log_duplicates(self):
        """
        Get the set of duplicate nodes and edges encountered and print them to
        the logger.
        """

        dtypes = self.batch_writer.get_duplicate_node_types()

        if dtypes:
            logger.warning(
                'Duplicate nodes encountered in the following types '
                '(see log for details): \n'
                f'{dtypes}',
            )

            dn = self.batch_writer.get_duplicate_nodes()

            msg = 'Duplicate nodes encountered: \n'
            for k, v in dn.items():
                msg += f'    {k}: {v} \n'

            logger.debug(msg)

        else:
            logger.info('No duplicate nodes in input.')

        etypes = self.batch_writer.get_duplicate_edge_types()

        if etypes:

            logger.warning(
                'Duplicate edges encountered in the following types '
                '(see log for details): \n'
                f'{etypes}',
            )

            de = self.batch_writer.get_duplicate_edges()

            msg = 'Duplicate edges encountered: \n'
            for k, v in de.items():
                msg += f'    {k}: {v} \n'

            logger.debug(msg)

        else:
            logger.info('No duplicate edges in input.')


    def show_ontology(self) -> None:
        """
        Show the ontology structure of the database using the Biolink
        schema and treelib.
        """

        self.start_ontology_adapter()
        self.biolink_adapter.show()


    def translate_term(self, term: str) -> str:
        """
        Translate a term to its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.translate_term(term)

    def reverse_translate_term(self, term: str) -> str:
        """
        Reverse translate a term from its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.reverse_translate_term(term)

    def translate_query(self, query: str) -> str:
        """
        Translate a query to its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.translate(query)

    def reverse_translate_query(self, query: str) -> str:
        """
        Reverse translate a query from its BioCypher equivalent.
        """

        # instantiate adapter if not exists
        self.start_ontology_adapter()

        return self.translator.reverse_translate(query)

    def __repr__(self):

        return f'<BioCypher {neo4j_utils.Driver.__repr__(self)[1:]}'

    def __del__(self):

        if not self.offline:

            self.flush()
