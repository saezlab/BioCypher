(adapter_functions)=
# BioCypher Tutorial - Adapters

![BioCypher pipeline interface](figure_pipeline.png)

```{note}
To facilitate the creation of a BioCypher pipeline, we have created a template
repository that can be used as a starting point for your own adapter. It
contains a basic structure for an adapter, as well as a script that can be used
as a blueprint for a build pipeline. The repository can be found
[here](https://github.com/saezlab/biocypher-project-template).
```

A "BioCypher adapter" is a python program responsible for connecting to the
BioCypher core and providing it with the data from its associated resource.
In doing so, it should adhere to several design principles to ensure simple
interoperability between the core and multiple adapters. In essence, an adapter
should conform to an interface that is defined by the core to give information
about the nodes and edges the adapter provides to enable automatic harmonisation
of the contents. An adapter can be "primary", i.e., responsible for a single
"atomic" resource (e.g. UniProt, Reactome, etc.), or "secondary", i.e.,
connecting to a resource that is itself a combination of multiple primary
resources (e.g. OmniPath, Open Targets, etc.). Due to extensive prior
harmonisation, the latter is often easier to implement and thus is a good
starting point that can be subsequently extended to and replaced by primary
adapters.

```{caution}
The adapter interface is still under development and may change rapidly.
```

In general, one adapter fulfils the following tasks:

1. Load the data from the primary resource, for instance by using pypath
download / caching functions (as in the [UniProt example
adapter](https://github.com/HUBioDataLab/CROssBAR-BioCypher-Migration)), by
using columnar distributed data formats such as Parquet (as in the [Open Targets
example adapter](https://github.com/saezlab/OTAR-BioCypher)), by using a running
database instance (as in the [CKG example
adapter](https://github.com/saezlab/CKG-BioCypher/)), or by simply reading a
file from disk (as in the [Dependency Map example
adapter](https://github.com/saezlab/DepMap-BioCypher)). Generally, any method
that allows the efficient transfer of the data from adapter to BioCypher core is
acceptable.

2. Pass the data to BioCypher as a stream or list to be written to the Neo4j
database via the python driver ("online") or via admin import (batch import from
CSV). The latter has the advantage of high throughput and a low memory
footprint, while the former allows for a more interactive workflow but is much
slower, thus making it better suited for small incremental updates.

3. Provide or connect to additional functionality that is useful for the
creation of knowledge graphs, such as identifier translation (e.g. via
pypath.mapping as in the UniProt example adapter), or identifier and prefix
standardisation and validation (e.g. via Bioregistry as in the UniProt example
adapter and others).

```{note}
For developers: We follow a design philosophy of "separation of concerns" in
BioCypher. This means that the core should not be concerned with the details of
how data is loaded, but only with the data itself. This is why the core does not
contain any code for loading data from a resource, but only for writing it to
the database. The adapter is responsible for loading the data and passing it to
the core, which allows for a more modular design and makes it easier to
maintain, extend, and reuse the code.

For introduction of new features, we recommend to first implement them in the
adapter, and to move them to the core only if they have shown to be useful for
multiple adapters.
```

## 1. Loading the Data

Depending on the data source, it is up to the developer of the adapter to find
and define a suitable representation to be piped into BioCypher. The way we
handle it in ``PyPath`` is only one of many: we load the entire ``PyPath``
object into memory, to be passed to BioCypher using a generator that evaluates
each ``PyPath`` object and transforms it to the tuple representation described
below. This is made possible by the "pre-harmonised" form in which the data
is represented within ``PyPath``. For more heterogeneous data representations,
additional transformations may be necessary before piping into BioCypher.

For larger datasets, it can be beneficial to adopt a streaming approach or batch
processing, as demonstrated in the [Open Targets
adapter](https://github.com/saezlab/OTAR-BioCypher) and the [CKG
adapter](https://github.com/saezlab/CKG-BioCypher/). BioCypher can handle input
streams of arbitrary length via Python generators.

## 2. Passing the Data

We currently pass data into BioCypher as a collection of tuples. Nodes are
represented as 3-tuples, containing:
- the node ID (unique in the space of the knowledge graph, ideally a CURIE with
  a prefix registered in the Bioregistry)
- the node type, i.e., its label (this is the string that is mapped to an
  ontological class via the `label_in_input` field in the schema configuration)
- a dictionary of node attributes

While edges are represented as 5-tuples, containing:
- the (optional) relationship ID (unique in the space of the KG)
- the source node ID (referring to a unique node ID in the KG)
- the target node ID (referring to a unique node ID in the KG)
- the relationship type, i.e., its label (this is the string that is mapped to
  an ontological class via the `label_in_input` field in the schema configuration)
- a dictionary of relationship attributes

```{note}
This representation will probably be subject to change soon and yield to a more
standardised interface for nodes and edges, derived from a BioCypher core class.
```

## Note: Strict mode
We can activate BioCypher strict mode with the `strict_mode` parameter upon
instantiation of the driver. In strict mode, the driver will raise an error if
it encounters a node or edge without data source, version, and licence. These
currently need to be provided as part of the node and edge attribute
dictionaries, with the reserved keywords `source`, `version`, and `licence` or
`license`. However, this may change to a more rigorous implementation in the
future.


# Interacting with Neo4j

## Communication via the Neo4j Python Driver

The BioCypher [Driver](driver) is the main submodule of BioCypher. It
establishes a connection with a running graph database via the
{py:class}`neo4j.GraphDatabase.driver`, integrates the funtions of the
other submodules, and serves as outside interface of BioCypher. The
``Driver`` is the main class for interacting with BioCypher in the host
module's adapter class. It handles authentification and basic database
management as well as the creation and manipulation of graph entries. It
can be instantiated and accessed like this:

```
import biocypher

# offline mode
d = biocypher.Driver(
   db_name = "neo4j",
   offline = True
)

# online mode
d = biocypher.Driver(
   db_name = "neo4j",
   db_user = "neo4j",
   db_passwd = "neo4j"
)
```

In our example, it is instantiated in the initialisation of the adapter,
and then called on for [interacting with a running graph](running) and
for exporting a complete database in CSV format for the [Neo4j
admin-import feature](admin_import). Upon instantiation, it
automatically assesses the graph database it is connected to (specified
using the ``db_name`` argument) regarding whether or not it already
contains a BioCypher graph, and, if so, what the structure of this graph
is.

If there exists no BioCypher graph in the currently active database, or
if the user explicitly specifies so using the ``wipe`` attribute of the
driver, a new BioCypher database is created using the schema
configuration specified in the [schema-config.yaml](schema-config).

(running)=
## Interacting with a running Neo4j instance

Once instantiated, the BioCypher driver can be used to modify the
current graph by adding or deleting nodes, edges, properties,
constraints, et cetera. Most commonly, the methods
{py:meth}`biocypher.driver.Driver.add_nodes()` and
{py:meth}`biocypher.driver.Driver.add_edges()` are used to introduce new
entries into the graph database.

(admin_import)=
## Exporting for the `neo4j-admin import` feature

Particularly if the data are very extensive (or performance is of the
utmost priority), BioCypher can be used to facilitate a speedy and safe
import of the data using the ``neo4j-admin import`` console command.
[Admin
Import](https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/)
is a particularly fast method of writing data to a newly created graph
database (the database needs to be completely empty) that gains most of
its performance advantage from turning off safety features regarding
input data consistency. Therefore, a sound and consistent representation
of the nodes and edges in the graph is of high importance in this
process, which is why the BioCypher export functionality has been
specifically designed to perform type and content checking for all data
to be written to the graph.

Data input from the source database is exactly as in the case of
[interacting with a running database](running), with the data
representation being converted to a series of CSV files in a designated
output folder (standard being ``biocypher-out/`` and the current datetime).
BioCypher creates separate header and data files for all node and edge
types to be represented in the graph via the driver methods
{py:meth}`biocypher.driver.Driver.write_nodes()` and
{py:meth}`biocypher.driver.Driver.write_edges()`. Additionally, it creates
a file called ``neo4j-admin-import-call.txt`` containing the console
command for creating a new database, which only has to be executed from
the directory of the currently running Neo4j database.

The name of the database to be created is given by the ``db_name``
attribute of the driver methods, ie, ``write_nodes()`` and
``write_edges()``, which should receive the same name as in the
``PyPath`` adapter example, and can be arbitrary. In case the
``db_name`` is not the default Neo4j database name, ``neo4j``, the
database needs to be created in Neo4j before or after using the
``neo4j-admin import`` statement. This can be done by executing, in the
running database (``<db_name>`` being the name assigned in the method):

1. ``:use system``
2. ``create database <db_name>``
3. ``:use <db_name>``

(translation)=
## Translating between original and BioCypher vocabulary
For quickly transitioning to BioCypher, single terms or entire queries
can be translated using the information provided in the
`schema_config.yaml`. The translation functionality can be accessed
through the driver methods {py:meth}`biocypher.driver.translate_term()`,
{py:meth}`biocypher.driver.reverse_translate_term()`,
{py:meth}`biocypher.driver.translate_query()`, and
{py:meth}`biocypher.driver.reverse_translate_query()`. For instance, to
find out the designation of "gene_gene" relationships in BioCypher, one
can call:

```
driver.translate_term("gene_gene")
# 'GeneToGeneAssociation'
```

Similarly, to discover the original naming of
"PostTranslationalInteraction" relationships, one can call the reverse
function:

```
driver.reverse_translate_term("PostTranslationalInteraction")
# 'post_translational'
```

{py:meth}`biocypher.driver.translate_query()` and
{py:meth}`biocypher.driver.reverse_translate_query()` replace all label
designations (following a ":") in entire CYPHER query strings with the
BioCypher or original versions, respectively.
