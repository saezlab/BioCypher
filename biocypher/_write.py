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
Export of CSV files for the Neo4J admin import.

The admin import is able to quickly transfer large amounts of content into an
unused database. For more explanation, see
https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/.

Import like that:
https://community.neo4j.com/t/how-can-i-use-a-database-created-with-neo4j-\
admin-import-in-neo4j-desktop/40594

    - Can properties the node/relationship does not own be left blank?

Formatting: --delimiter=";"
            --array-delimiter="|"
            --quote="'"

The header contains information for each field, for ID and properties
in the format <name>: <field_type>. E.g.:
`UniProtKB:ID;genesymbol;entrez_id:int;:LABEL`. Multiple labels can
be given by separating with the array delimiter.

There are three mandatory fields for relationship data:
:START_ID — ID referring to a node.
:END_ID — ID referring to a node.
:TYPE — The relationship type.

E.g.: `:START_ID;relationship_id;residue;:END_ID;:TYPE`.

Headers would best be separate files, data files with similar name but
different ending. Example from Neo4j documentation:

.. code-block:: bash

   bin/neo4j-admin import --database=neo4j
   --nodes=import/entities-header.csv,import/entities-part1.csv,
    import/entities-part2.csv
   --nodes=import/interactions-header.csv,import/interactions-part1.csv,
    import/interaction-part2.csv
   --relationships=import/rels-header.csv,import/rels-part1.csv,
    import/rels-part2.csv

Can use regex, e.g., [..] import/rels-part*. In this case, use padding
for ordering of the earlier part files ("01, 02").

# How to import:

1. stop the db

2. shell command:

.. code-block:: bash

   bin/neo4j-admin import --database=neo4j
   # nodes per type, separate header, regex for parts:
   --nodes="<path>/<node_type>-header.csv,<path>/<node_type>-part.*"
   # edges per type, separate header, regex for parts:
   --relationships="<path>/<edge_type>-header.csv,<path>/<edge_type>-part.*"

3. start db, test for consistency
"""

from ._logger import logger

logger.debug(f'Loading module {__name__}.')

from typing import Any, Iterable, Literal, TYPE_CHECKING
from datetime import datetime
from collections import OrderedDict, defaultdict
import os
import re
import glob
import importlib as imp

import biocypher._misc as _misc
from biocypher._config import config as _config
from ._entity import BC_TYPES, Edge, Node, RelAsNode

__all__ = ['BatchWriter', 'ENTITIES']

if TYPE_CHECKING:

    from ._biolink import BiolinkAdapter

N4_ALL_TYPES = {
    'int',
    'long',
    'float',
    'double',
    'boolean',
    'byte',
    'short',
    'char',
    'string',
    'point',
    'date',
    'localtime',
    'time',
    'localdatetime',
    'datetime',
    'duration',
}

N4_TYPES = {
    'long': ('int', 'long'),
    'double': ('double', 'float', 'dbl'),
    'boolean': ('bool', 'boolean'),
    'string': ('str',),
}

PY_TYPES = {py: n4 for n4, pys in N4_TYPES.items() for py in pys}
BATCH_SIZE_FALLBACK = int(1e6)
ENTITIES = Literal['node', 'edge']

# TODO retrospective check of written csvs?


class BatchWriter:
    """
    Write node and edge representations to disk using the
    format specified by Neo4j for the use of admin import. Each batch
    writer instance has a fixed representation that needs to be passed
    at instantiation via the :py:attr:`schema` argument. The instance
    also expects a biolink adapter via :py:attr:`bl_adapter` to be able
    to convert and extend the hierarchy.
    """

    def __init__(
        self,
        schema: dict,
        bl_adapter: 'BiolinkAdapter',
        delimiter: str | None = None,
        array_delimiter: str | None = None,
        quote: str | None = None,
        dirname: str | None = None,
        db_name: str = 'neo4j',
        skip_bad_relationships: bool = False,
        skip_duplicate_nodes: bool = False,
        batch_size: int | None = None,
        wipe: bool = True,
    ):
        """
        Export data into CSV for *neo4j-admin* import.

        Args:
            schema:
                The BioCypher graph schema (from :py:class:`VersionNode`).
            bl_adapter:
                Instance of :py:class:`BiolinkAdapter` to enable translation
                and ontology queries
            delimiter:
                Character to separate the fields in CSV files.
            array_delimiter:
                Character to separate elements of arrays within CSV fields.
            quote:
                Character to enclose string fields in CSV files.
            dirname:
                Path for exporting CSV files.
            db_name:
                Name of the Neo4j database that will be used in the generated
                commands.
            skip_bad_relationships:
                Behaviour when encountering erroneous relationships.
            skip_duplicate_nodes:
                Behaviour when encountering duplicate nodes.
            batch_size:
                Number of records in one CSV. Override here the value defined
                in the config.
            wipe:
                Add the `--force` switch to the command line call, resulting
                the overwrite of pre-existing database contents.

        Attributes:
            seen_node_ids:
                Dictionary to store the ids of nodes that have already been
                written; to avoid duplicates. keys: ids, values: amount of
                duplicates
            duplicate_node_types:
                Set to store the types of nodes that have been found to have
                duplicates.
            seen_edges:
                Dict to store the set of edges that have already been written;
                to avoid duplicates; per edge type. Keys: ids, values: count.
            duplicate_edge_types:
                Set to store the types of nodes that have been found to have
                duplicates.
            property_types:
                Dict to store a dict of properties for each label to check for
                consistency and their type for now, relevant for `int`.
        """
        self.db_name = db_name

        self.delim = delimiter or _config('csv_delimiter')
        self.adelim = array_delimiter or _config('csv_array_delimiter')
        self.quote = quote or _config('csv_quote_char')
        self.batch_size = batch_size or _config('csv_batch_size')

        self.skip_bad_relationships = skip_bad_relationships
        self.skip_duplicate_nodes = skip_duplicate_nodes
        self.schema = schema
        self.wipe = wipe
        self.bl_adapter = bl_adapter
        self.set_outdir(dirname)
        self.reset()

    def reload(self):
        """
        Reloads the object from the module level.
        """

        modname = self.__class__.__module__
        mod = __import__(modname, fromlist = [modname.split('.')[0]])
        imp.reload(mod)
        new = getattr(mod, self.__class__.__name__)
        setattr(self, '__class__', new)

    def main(self):
        """
        Creates CSV files from the provided components.
        """

        raise NotImplementedError

    def set_outdir(self, path: str | None = None):
        """
        Set and create the output directory where CSV files will be written.

        Args:
            path:
                Use it to override the value from config.
        """

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        self.outdir = path or os.path.join(_config('outdir'), timestamp)
        self.outdir = os.path.abspath(self.outdir)

        logger.info(f'Creating output directory `{self.outdir}`.')
        os.makedirs(self.outdir, exist_ok=True)

    def reset(self):
        """
        Creates empty data structures for the writing process.

        This object works by preprocessing data and writing it into multiple
        CSV files in batches, and creating CLI commands that can be used to
        import the CSV files into a Neo4j database. This method drops all
        previously preprocessed data and creates a clean environment for
        further processing tasks.
        """

        by_entity = lambda val: {e: val() for e in ENTITIES.__args__}

        self.property_types = by_entity(lambda: defaultdict(dict))
        self.call = by_entity(lambda: [])
        self.seen = by_entity(lambda: defaultdict(int))
        self.dupl_by_type = by_entity(lambda: defaultdict(int))

    def _duplicate_types(self, what: ENTITIES) -> set[str]:
        """
        Node labels or edge types with duplicates.

        Args:
            what:
                Nodes or edges?

        Returns:
            The labels or types with at least one duplicate ID.
        """

        raise NotImplementedError(
            'Not sure if this function is necessary. '
            'If it is, will be easy to implement.'
        )

    @property
    def duplicate_node_labels(self) -> set[str]:
        """
        Node labels with duplicates.

        Returns:
            Node labels with at least one duplicate ID.
        """

        return self._duplicate_types('node')

    @property
    def duplicate_edge_types(self) -> set[str]:
        """
        Edge types with duplicates.

        Returns:
            Edge types with at least one duplicate ID.
        """

        return self._duplicate_types('edge')

    def _duplicates(self, what: ENTITIES) -> set[str]:
        """
        Node or edge IDs with duplicates.

        Args:
            what:
                Nodes or edges?

        Returns:
            The IDs encountered at least twice.
        """

        return set(self._count_duplicates(what).keys())

    @property
    def duplicate_nodes(self) -> set[str]:
        """
        Node IDs with duplicates.

        Returns:
            Node IDs encountered at least twice.
        """

        return self._duplicates('node')

    @property
    def duplicate_edges(self) -> set[str]:
        """
        Edge IDs with duplicates.

        Returns:
            Edge IDs encountered at least twice.
        """

        return self._duplicates('edge')

    def _count_duplicates(self, what: ENTITIES) -> dict[str, int]:
        """
        Number of duplicates encountered by ID.

        Args:
            what:
                Nodes or edges?

        Returns:
            Number of duplicates by ID.
        """

        return dict(it for it in self.seen[what].items() if it[1] > 1)

    def count_duplicate_nodes(self) -> dict[str, int]:
        """
        Number of duplicate nodes encountered by label.

        Returns:
            Number of duplicates by label.
        """

        return self._count_duplicates('node')

    def count_duplicate_edges(self) -> dict[str, int]:
        """
        Number of duplicate edges encountered by type.

        Returns:
            Number of duplicates by type.
        """

        return self._count_duplicates('edge')

    def write(
            self,
            entities: Iterable[BC_TYPES],
            batch_size: int | None = None,
        ) -> bool:
        """
        Top level method for writing graph components and their headers.

        Args:
            entities:
                Iterable of graph components.
            batch_size:
                Number of records in one CSV file.

        Returns:
            True for success, False otherwise.
        """
        # TODO check represented_as

        bs = self._batch_size(batch_size)

        entities = _misc.to_list(entities)
        nodes = (n for ee in entities for n in ee.nodes)
        edges = (e for ee in entities for e in ee.edges)

        return (
            self._write_records(nodes, batch_size = bs) and
            self._write_records(edges, batch_size = bs) and
            (self._write_headers('node') or True) and
            (self._write_headers('edge') or True)
        )

    def _property_types(
            self,
            label: str,
            instance: Node | Edge | None = None,
            what: ENTITIES | None = None,
        ):

        propt = {}
        what = what or getattr(instance, 'entity', 'node')

        # get properties from config if present
        from_conf = self.bl_adapter.schema.get(label, {}).get('properties')

        if what != 'node' and not from_conf:

            from_conf = self._lae_proptypes(label)

        if from_conf:

            propt = from_conf.copy()

            if what == 'node':
                # add id and preferred id to properties; these are
                # created in node creation (`_entity.Node`)
                propt.update({'id': 'str', 'preferred_id': 'str'})

        elif instance:

            propt = {
                k: v.__class__.__name__
                for k, v in instance.props.items()
                if k is not None  # why would be a key here None?
            }

        self.property_types[what][label] = propt

    def _lae_proptypes(self, label: str) -> dict[str, str] | None:
        """
        Looks up property types for label as edge schema.
        """

        for leaf in self.bl_adapter.schema.values():

            if (
                isinstance(leaf, dict) and
                leaf.get('label_as_edge') == label
            ):

                return leaf.get('properties')


    def _batch_size(self, override: int | None = None) -> int:
        """
        Batch size from the currently valid config.
        """

        return override or self.batch_size or BATCH_SIZE_FALLBACK

    @staticmethod
    def _col_type(py_type: str | type) -> str:
        """
        Neo4j column types as used in the CSV header notation.

        Args:
            py_type:
                A type or its name.

        Returns:
            A type annotation that can be appended to the CSV column label.
        """

        py_type = py_type.__name__ if isinstance(py_type, type) else py_type
        n4_type = PY_TYPES.get(
            py_type,
            py_type if py_type in N4_ALL_TYPES else None,
        )

        return f'{":" if n4_type else ""}{n4_type}'

    def _proc_prop(self, value: Any, py_type: str | type) -> str:
        """
        Process a property value.

        Converts a property value to string and adds quotes if the column
        type is string.
        """

        n4_type = self._col_type(py_type)
        q = self.quote if n4_type == 'string' else ''
        # make all into strings, put actual strings in quotes
        value = '' if value is None else str(value)

        return f'{q}{value}{q}'

    def _write_records(
            self,
            entities: Iterable[Node] | Iterable[Edge],
            batch_size: int | None = None,
        ) -> bool:
        """
        Process mixed type node or edge records and write the into CSV files.

        Writes biocypher nodes or edges to CSV conforming to the headers
        created with `_write_headers`, and is actually required to be run
        before calling `_write_headers()` to set the values in
        :py:attr:`self.property_types`.

        Args:
            entities:
                An iterable of nodes or edges, each represented by a
                :py:class:`Node`` or :py:class:``Edge``
                instance.
            batch_size:
                Override instance or config level value.

        Returns:
            True for success, False otherwise.

        Todo:
            - currently works for mixed edges but in practice often is
              called on one iterable containing one type of edge only
        """

        # TODO not memory efficient, but should be fine for most cases; is
        # there a more elegant solution?
        # --- what is not efficient?

        if not isinstance(entities, _misc.LIST_LIKE):

            # is this error worth to check? if this value is not
            # iterable at all, that means some huge and obvious error
            # that raises a type error just a few lines below anyways
            logger.error('Components must be passed as list or generator.')
            return False

        # sort entities by label
        by_label = defaultdict(list)
        # additional labels (derived from biolink) by primary label
        labels = {}
        batch_size = self._batch_size(batch_size)
        written_here = 0
        processed_here = 0

        for e in entities:

            processed_here += 1
            what = self._what(e)
            node = what == 'node'

            if not node and not (e.source and e.target):

                logger.error(
                    'Edge must have source and target node. '
                    f'Caused by: {e}',
                )
                continue

            _id = e.id if node else f'{e.source} -> {e.target}'
            self.seen[what][_id] += 1
            label = e.label

            # check for duplicates
            if self.seen[what][_id] > 1:

                self.dupl_by_type[what][label] += 1
                continue

            if label not in by_label:

                self._property_types(label = label, instance = e)

                if node:

                    # else use first encountered node to define properties for
                    # checking; could later be by checking all nodes but much
                    # more complicated, particularly involving batch writing
                    # (would require "do-overs"). for now, we output a warning
                    # if node properties diverge from reference properties (in
                    # write_single_node_list_to_file) TODO if it occurs, ask
                    # user to select desired properties and restart the process
                    all_labels = (
                        self.bl_adapter.biolink_schema.
                        get(label, {}).
                        get('ancestors')
                    ) or (label,)
                    all_labels = OrderedDict.fromkeys(all_labels)
                    all_labels = self.adelim.join(all_labels)
                    labels[label] = all_labels

            by_label[label].append(e)

            if len(by_label[label]) >= batch_size:

                # batch size controlled here
                passed = self._compile_batch(
                    entities = by_label[label],
                    label = label,
                    labels = labels[label] if node else None,
                )

                if not passed:

                    return False

                written_here += len(by_label[label])
                by_label[label] = []

        # after generator depleted, write remainder of by_label
        for label, items in by_label.items():

            node = items and self._what(items[0]) == 'node'

            passed = self._compile_batch(
                entities = items,
                label = label,
                labels = labels[label] if node else None,
            )

            if not passed:

                return False

            written_here += len(items)

        logger.info(
            f'Processed {processed_here} '
            f'and written to CSV {written_here} items.'
        )

        if processed_here and not written_here:

            logger.warning(
                'Looks like no items has been written out in this call! '
                'Maybe all were duplicates, maybe you forgot to call `reset`?'
            )

        # --- where this comment belongs to?
        # use complete bin list to write header files
        # TODO if a node/edge type has varying properties
        # (ie missingness), we'd need to collect all possible
        # properties in the generator pass
        return True

    @staticmethod
    def _what(entity: Node | Edge) -> ENTITIES:

        return re.sub('^BioCypher', '', entity.__class__.__name__).lower()

    def _write_headers(self, what: ENTITIES) -> bool:
        """
        Write the header lines for exported CSV data.

        Returns:
            True for success, False otherwise.
        """

        # load headers from data parse
        if not self.property_types[what]:

            logger.warning(
                f'Header information not found for {what}s. '
                f'Either the data contains no {what}s, '
                'or `_write_headers` was called at the wrong point?',
            )
            return True

        node = what == 'node'

        for label, props in self.property_types[what].items():

            # to programmatically define properties to be written, the
            # data would have to be parsed before writing the header.
            # alternatively, desired properties can also be provided
            # via the schema_config.yaml.

            # translate label to PascalCase
            label = self.bl_adapter.name_sentence_to_pascal(label)
            hdr_path = os.path.join(self.outdir, f'{label}-header.csv')
            prt_path = os.path.join(self.outdir, f'{label}-part.*')

            # check if file already exists
            if os.path.exists(hdr_path): continue

            # add column types
            header = [':ID' if node else ':START_ID']
            # this is not guaranteed to be order preserving
            # though it will work fine in cpython
            header.extend(
                f'{prop}{self._col_type(py_t)}'
                for prop, py_t in sorted(props.items(), key = lambda i: i[0])
            )
            header.extend([':LABEL'] if node else [':END_ID', ':TYPE'])

            with open(hdr_path, 'w', encoding="utf-8") as f:

                # concatenate with delimiter
                f.write(self.delim.join(header))

            # add file path to neo4 admin import statement
            switch = "node" if node else "relationship"
            self.call['node'].append(f'--{switch}s="{hdr_path},{prt_path}"')

        return True

    def _compile_batch(
        self,
        entities: Iterable[Node] | Iterable[Edge],
        label: str,
        labels: str,
    ) -> bool:
        """
        Compiles one batch into lines and writes the out to a CSV file.

        Args:
            entities:
                Iterable of nodes or edges, each represented as a
                ``Node`` or ``Edge`` instance.
            label:
                Label or type of the nodes or edges.
            labels:
                String of one or several concatenated labels
                for the node class. Ignored for edges.

        Returns:
            True for success, False otherwise.
        """

        entities = _misc.to_list(entities)

        if not entities: return True

        eclass = entities[0].__class__
        what = self._what(entities[0])
        node = what == 'node'

        if not all(isinstance(e, eclass) for e in entities):

            whatcap = what.capitalize()
            logger.error(
                f'{whatcap}s must be passed as '
                f'type BioCypher{whatcap}.'
            )
            return False

        ptypes = self.property_types[what][label]
        ptypes_sorted = sorted(ptypes.items(), key = lambda i: i[0])

        # from list of nodes or edges to list of strings
        lines = []

        for e in entities:
            # check for deviations in properties
            # edge properties
            props = e.props

            missing = set(ptypes.keys()) - set(props.keys())
            excess = set(props.keys()) - set(ptypes.keys())
            e_display = (
                f'ID `{e.id}`'
                    if node else
                f'endpoint IDs `{e.source}-{e.target}`'
            )

            if missing:

                logger.error(
                    f'One `{e.label}` {what} with {e_display} is '
                    f'missing the following properties: {", ".join(missing)}.'
                )
                return False

            if excess:

                logger.error(
                    f'One `{e.label}` node with {e_display} has t'
                    f'he following unexpected properties: {", ".join(excess)}.'
                )
                return False

            line = [e.id if node else e.source]
            # this is not guaranteed to be order preserving
            # though it will work fine in cpython
            line.extend(
                self._proc_prop(props.get(prop), py_type)
                for prop, py_type in ptypes_sorted
            )
            line.append(labels if node else e.target)

            if not node:

                line.append(e.label)

            lines.append(self.delim.join(line))

        self._write_batch(label, lines)

        return True

    def _write_batch(self, label: str, lines: list[str]):
        """
        This function writes a list of strings to a new part file.

        Args:
            label:
                The label (type) of the edge; internal representation
                in sentence case -> will change to PascalCase before
                writing to CSV.
            lines:
                The lines to be written.
        """

        # avoid writing empty files
        if not lines: return

        # translate label to PascalCase
        label = _misc.cc(label)

        # list files in self.outdir
        files = glob.glob(os.path.join(self.outdir, f'{label}-part*.csv'))
        repart = re.compile(r'part(\d+)')

        idx = max([int(repart.search(f).group(1)) for f in files] + [-1]) + 1

        # write to file
        path = os.path.join(self.outdir, f'{label}-part{idx:03d}.csv')
        logger.info(f'Writing {len(lines)} entries to `{path}`.')

        with open(path, 'w', encoding = 'utf-8') as fp:

            # concatenate with delimiter
            fp.write(os.linesep.join(lines))

        return True

    def write_call(self) -> bool:
        """
        Write the *neo4j-admin import* call into a file.

        This call contains information about all the exported files,
        their headers, the database name and further parameters. It
        is suitable for importing data into Neo4j by executing it
        in the shell.

        Returns:
            True for success, False otherwise.
        """

        file_path = os.path.join(self.outdir, 'neo4j-admin-import-call.sh')
        logger.info(f'Writing neo4j-admin import call to `{file_path}`.')

        with open(file_path, 'w', encoding = 'utf-8') as f:

            f.write(self.compile_call())

        return True

    def compile_call(self) -> str:
        """
        Compile the *neo4j-admin* call based on previously processed data.

        This call contains information about all the exported files,
        their headers, the database name and further parameters. It
        is suitable for importing data into Neo4j by executing it
        in the shell.

        Returns:
            A call of the *neo4j-admin import* command.
        """

        q = "'" if self.quote == '"' else '"'

        call = (
            [
                'neo4j-admin import',
                f'--database={self.db_name}',
                f'--delimiter="{self.delim}"',
                f'--array-delimiter="{self.adelim}"',
                f'--quote={q}{self.quote}{q}',
                '--skip-bad-relationships='
                f'{str(self.skip_bad_relationships).lower()}',
                '--skip-duplicate-nodes='
                f'{str(self.skip_duplicate_nodes).lower()}',
            ] +
            (['--force=true'] if self.wipe else []) +
            self.call['node'] +
            self.call['edge']
        )

        return f'    {os.linesep}'.join(call)
