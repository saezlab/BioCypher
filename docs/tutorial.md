(tutorial)=
# BioCypher Tutorial - Basics
The main purpose of BioCypher is to facilitate the pre-processing of biomedical
data to save development time in the maintenance of curated knowledge graphs
and to allow the simple and efficient creation of task-specific lightweight
knowledge graphs in a user-friendly and biology-centric fashion.

We are going to use a toy example to familiarise the user with the basic
functionality of BioCypher. One central task of BioCypher is the harmonisation
of dissimilar datasets describing the same entities. Thus, in this example, the
input data - which in the real-world use case could come from any type of
interface - are represented by simulated data containing some examples of
differently formatted biomedical entities such as proteins and their
interactions.

## Section 1: Adding data
```{admonition} Tutorial files
:class: note
The code for this tutorial can be found at `tutorial/01_basic_import.py`. The
schema is at `tutorial/01_schema_config.yaml`. Data generation happens in
`tutorial/data_generator.py`.
```

### Input data stream ("adapter")
The basic operation of adding data to the knowledge graph requires two
components: an input stream of data (which we call adapter) and a configuration
for the resulting desired output (the schema configuration). The former will be
simulated by calling the `Protein` class of our data generator 10 times.

```{code-block} python
from data_generator import Protein
proteins = [Protein() for _ in range(10)]
```

Each protein in our simulated data has a UniProt ID, a label
("uniprot_protein"), and a dictionary of properties describing it. This is -
purely by coincidence - very close to the input BioCypher expects (for nodes):
- a unique identifier
- an input label (to allow mapping to the ontology, see the second step below)
- a dictionary of further properties (which can be empty)

These should be presented to the BioCypher driver in the form of a tuple. To
achieve this representation, we can use a generator function that iterates
through our simulated input data and, for each entity, forms the corresponding
tuple. The use of a generator allows for efficient streaming of larger datasets
where required.

```{code-block} python
def node_generator():
    for protein in proteins:
        yield (
          protein.get_id(),
          protein.get_label(),
          protein.get_properties()
        )
```

The concept of an adapter can become arbitrarily complex and involve
programmatic access to databases, API requests, asynchronous queries, context
managers, and other complicating factors. However, it always boils down to
providing the BioCypher driver with a collection of tuples, one for each entity
in the input data.

As descibed above, *nodes* possess:

- a mandatory ID,
- a mandatory label, and
- a property dictionary,

while *edges* possess:

- an (optional) ID,
- two mandatory IDs for source and target,
- a mandatory label, and
- a property dictionary.

How these entities are mapped to the ontological hierarchy
underlying a BioCypher graph is determined by their mandatory labels, which
connect the input data stream to the schema configuration. This we will see in
the following section.

<!-- Figure for ID, label, prop of nodes and edges? -->

(tut_01_schema)=
### Schema configuration
How each BioCypher graph is structured is determined by the schema
configuration YAML file that is given to the driver. This also serves to ground
the entities of the graph in the biomedical realm by using an ontological
hierarchy. In this tutorial, we refer to the Biolink model as the general
backbone of our ontological hierarchy. The basic premise of the schema
configuration YAML file is that each component of the desired knowledge graph
output should be configured here; if (and only if) an entity is represented in
the schema configuration *and* is present in the input data stream, will it be
part of our knowledge graph.

In our case, since we only import proteins, we
only require few lines of configuration:

```{code-block} yaml
protein:                            # mapping
  represented_as: node              # schema configuration
  preferred_id: uniprot             # uniqueness
  label_in_input: uniprot_protein   # connection to input stream
```

The first line (`protein`) identifies our entity and connects to the
ontological backbone; here we define the first class to be represented in the
graph. In the configuration YAML, we represent entities — similar to the
internal representation of Biolink — in lower sentence case (e.g., "small
molecule"). Conversely, for class names, in file names, and property graph
labels, we use PascalCase instead (e.g., "SmallMolecule") to avoid issues with
handling spaces. The transformation is done by BioCypher internally. BioCypher
does not strictly enforce the entities allowed in this class definition; in
fact, we provide [several methods of extending the existing ontological
backbone *ad hoc* by providing custom inheritance or hybridising
ontologies](biolink). However, every entity should at some point be connected
to the underlying ontology, otherwise the multiple hierarchical labels will not
be populated. Following this first line are three indented values of the
protein class.

<!-- TODO link to ontology manipulation -->

The second line (`represented_as`) tells BioCypher in which way each entity
should be represented in the graph; the only options are `node` and `edge`.
Representation as an edge is only possible when source and target IDs are
provided in the input data stream. Conversely, relationships can be represented
as both `node` or `edge`, depending on the desired output. When a relationship
should be represented as a node, i.e., "reified", BioCypher takes care to create
a set of two edges and a node in place of the relationship. This is useful when
we want to connect the relationship to other entities in the graph, for example
literature references.

The third line (`preferred_id`) informs the uniqueness of represented entities
by selecting an ontological namespace around which the definition of uniqueness
should revolve. In our example, if a protein has its own uniprot ID, it is
understood to be a unique entity. When there are multiple protein isoforms
carrying the same uniprot ID, they are understood to be aggregated to result in
only one unique entity in the graph. Decisions around uniqueness of graph
constituents sometimes require some consideration in task-specific
applications. Selection of a namespace also has effects in identifier mapping;
in our case, for protein nodes that do not carry a uniprot ID, identifier
mapping will attempt to find a uniprot ID given the other identifiers of that
node. To account for the broadest possible range of identifier systems while
also dealing with parsing of namespace prefixes and validation, we refer to the
[Bioregistry](https://bioregistry.io) project namespaces, which should be
preferred values for this field.

Finally, the fourth line (`label_in_input`) connects the input data stream to
the configuration; here we indicate which label to expect in the input tuple
for each class in the graph. In our case, we expect "uniprot_protein" as the
label for each protein in the input data stream; all other input entities that
do not carry this label are ignored as long as they are not in the schema
configuration.

### Creating the graph (using the BioCypher driver)
All that remains to be done now is to instantiate the BioCypher driver (as the
main means of communicating with BioCypher) and call the function to create the
graph. While this can be done "online", i.e., by connecting to a running Neo4j
instance, we will in this example use the offline mode of BioCypher, which does
not require setting up a graph database instance. The following code will use
the data stream and configuration set up above to write the files for knowledge
graph creation:

```{code-block} python
import biocypher
driver = biocypher.Driver(
    offline=True,
    user_schema_config_path="tutorial/01_schema_config.yaml",
)
driver.write_nodes(node_generator())
```

We pass our configuration file at driver instantiation, and we pass the data
stream to the `write_nodes` function. The driver will then create the graph and
write it to the output directory, which is set to `biocypher-out/` by default,
creating a subfolder with the current datetime for each driver instance.

```{note}
If you run the above code more than once (back to back), you will notice that
the second time is much faster than the first. This is because the driver will
cache the ontological hierarchy that needs to be parsed by LinkML from the
schema configuration and the Biolink model. This is done to avoid unnecessary
waiting time for parsing and validating the same information, for reducing the
number of API requests to the Biolink API, and for enabling a "soft" pinning of
the Biolink model version used for a particular graph.

The ontological hierarchy is automatically re-parsed if BioCypher notices a
change in the schema configuration. For instance, if you run another tutorial
file and then return to this one, the schema will be different, and thus the
hierarchy re-parsed. If you want to force the driver to re-parse the ontological
hierarchy without changing the schema, you can pass `clear_cache=True` at driver
instantiation.
```

### Importing data into Neo4j
The graph can now be imported into Neo4j using the `neo4j-admin` command line
tool. This is not necessary if the graph is created in online mode. For
convenience, BioCypher provides the command line call required to import the
data into Neo4j:

```{code-block} python
driver.write_import_call()
```

This creates an executable shell script in the output directory that can be
executed from the location of the database folder (or copied into the Neo4j
terminal) to import the graph into Neo4j. Since BioCypher creates separate
header and data files for each entity type, the import call conveniently
aggregates this information into one command, detailing the location of all
files on disk, so no data need to be copied around.

```{caution}
The generated import call is currently only compatible with Neo4j version 4.
Version 5 support coming soon!
```

Neo4j can manage multiple projects, each with multiple DBMS (database management
system) instances, each of which can house multiple databases. The screenshot
below shows a project managed by Neo4j Desktop (project name "BioCypher")
containing a DBMS instance (called "Test DBMS") with multiple named databases in
it: the non-removable "system" DB, the default "neo4j" DB, and several
user-created databases.

![Neo4j Desktop](DBMS.png)

Crucially, the import call generated by BioCypher needs to be executed by the
`neo4j-admin` binary in the DBMS folder (in the `bin/` directory of the root of
the DBMS folder). The DBMS folder is the folder that contains the `data/`
directory, which in turn contains the `databases/` folder, which is where the
graph data is stored in individual folders corresponding to the DB names in the
DBMS. On Neo4j Desktop, you can open a terminal at the DBMS root location by
clicking on the three dots next to the DBMS name and selecting "Terminal" (see
screenshot below).

![Neo4j Desktop](DBMS-Terminal.png)

Since the import call should be executed in the root of the DMBS folder,
BioCypher generates the import call starting with `bin/neo4j-admin`, which can
be copied into the terminal opened at the DBMS root location. For other
operating systems and Neo4j installations (e.g. in Docker containers), please
refer to the Neo4j documentation to find the correct location of your DMBS. We
are working on extensions of the BioCypher process to improve interoperability
with Neo4j as well as other storage systems.

```{note}
Neo4j provide a [Neo4j Desktop
application](https://neo4j.com/download-center/#desktop) that can be used to
create a local instance of Neo4j. The desktop application provides information
about the DBMS folder and can open a terminal at the DBMS root location. The
import call generated by BioCypher can then be copied into the terminal of the
Neo4j Desktop application for each corresponding DBMS.

Neo4j is also available as a command line interface (CLI) tool. To use the CLI
with the BioCypher admin import call, directory structures and permissions need
to be set up correctly. The Neo4j CLI tool can be downloaded from the [Neo4j
download center](https://neo4j.com/download-center/#community). Please follow
the [Neo4j documentation](https://neo4j.com/docs/) for correct setup and usage
on your system.

Be mindful that different versions of Neo4j may differ in features and thus are
also documented differently.
```

### Quality control and convenience functions
BioCypher provides a number of convenience functions for quality control and
data exploration. In addition to writing the import call for Neo4j, we can print
a log of ontological classes that were present in the input data but are not
accounted for in the schema configuration, as well as a log of duplicates in the
input data (for the level of granularity that was used for the import). We can
also print the ontological hierarchy derived from the underlying model(s)
according to the classes that were given in the schema configuration:

```{code-block} python
driver.log_missing_bl_types()     # show input unaccounted for in the schema
driver.log_duplicates()           # show duplicates in the input data
driver.show_ontology_structure()  # show ontological hierarchy
```

## Section 2: Merging data

(merging)=
### Plain merge
```{admonition} Tutorial files
:class: note
The code for this tutorial can be found at `tutorial/02_merge.py`.
Schema files are at `tutorial/02_schema_config.yaml`. Data generation happens
in `tutorial/data_generator.py`.
```

Using the workflow described above with minor changes, we can merge data from
different input streams. If we do not want to introduce additional ontological
subcategories, we can simply add the new input stream to the existing one and
add the new label to the schema configuration (the new label being
`entrez_protein`). In this case, we would add the following to the schema
configuration:

```{code-block} yaml
protein:
  represented_as: node
  preferred_id: uniprot
  label_in_input: [uniprot_protein, entrez_protein]
```

This again creates a single output file, now for both protein types, including
both input streams, and the graph can be created as before using the command
line call created by BioCypher. However, we are generating our `entrez`
proteins as having entrez IDs, which could result in problems in querying.
Additionally, a strict import mode including regex pattern matching of
identifiers will fail at this point due to the difference in pattern of UniProt
vs. Entrez IDs. This issue could be resolved by mapping the Entrez IDs to
UniProt IDs, but we will instead use the opportunity to demonstrate how to
merge data from different sources into the same ontological class using *ad
hoc* subclasses.

(implicit_subclass)=
### *Ad hoc* subclassing
```{admonition} Tutorial files
:class: note
The code for this tutorial can be found at `tutorial/03_implicit_subclass.py`.
Schema files are at `tutorial/03_schema_config.yaml`. Data generation happens
in `tutorial/data_generator.py`.
```

In the previous section, we saw how to merge data from different sources into
the same ontological class. However, we did not resolve the issue of the
`entrez` proteins living in a different namespace than the `uniprot` proteins,
which could result in problems in querying. In proteins, it would probably be
more appropriate to solve this problem using identifier mapping, but in other
categories, e.g., pathways, this may not be possible because of a lack of
one-to-one mapping between different data sources. Thus, if we so desire, we
can merge datasets into the same ontological class by creating *ad hoc*
subclasses implicitly through BioCypher, by providing multiple preferred
identifiers. In our case, we update our schema configuration as follows:

```{code-block} yaml
protein:
  represented_as: node
  preferred_id: [uniprot, entrez]
  label_in_input: [uniprot_protein, entrez_protein]
```

This will "implicitly" create two subclasses of the `protein` class, which will
inherit the entire hierarchy of the `protein` class. The two subclasses will be
named using a combination of their preferred namespace and the name of the
parent class, separated by a dot, i.e., `uniprot.protein` and `entrez.protein`.
In this manner, they can be identified as proteins regardless of their sources
by any queries for the generic `protein` class, while still carrying
information about their namespace and avoiding identifier conflicts.

```{note}
The only change affected upon the code from the previous section is the
referral to the updated schema configuration file.
```

```{hint}
In the output, we now generate two separate files for the `protein` class, one
for each subclass (with names in PascalCase).
```

## Section 3: Handling properties
While ID and label are mandatory components of our knowledge graph, properties
are optional and can include different types of information on the entities. In
source data, properties are represented in arbitrary ways, and designations
rarely overlap even for the most trivial of cases (spelling differences,
formatting, etc). Additionally, some data sources contain a large wealth of
information about entities, most of which may not be needed for the given task.
Thus, it is often desirable to filter out properties that are not needed to
save time, disk space, and memory.

```{note}
Maintaining consistent properties per entity type is particularly important
when using the admin import feature of Neo4j, which requires consistency
between the header and data files. Properties that are introduced into only
some of the rows will lead to column misalignment and import failure. In
"online mode", this is not an issue.
```

We will take a look at how to handle property selection in BioCypher in a
way that is flexible and easy to maintain.

### Designated properties
```{admonition} Tutorial files
:class: note
The code for this tutorial can be found at `tutorial/04_properties.py`. Schema
files are at `tutorial/04_schema_config.yaml`. Data generation happens in
`tutorial/data_generator.py`.
```

The simplest and most straightforward way to ensure that properties are
consistent for each entity type is to designate them explicitly in the schema
configuration. This is done by adding a `properties` key to the entity type
configuration. The value of this key is another dictionary, where in the
standard case the keys are the names of the properties that the entity type
should possess, and the values give the type of the property (`int`, `str`,
`bool`). In the case of properties that are not present in (some of) the source
data, BioCypher will add them to the output with a default value of `None`.
Additional properties in the input, that are not represented in these
designated property names, will be ignored.

Let's imagine that some, but not all, of our protein nodes have a `mass` value.
If we want to include the mass value on all proteins, we can add the following
to our schema configuration:

```{code-block} yaml
protein:
  represented_as: node
  preferred_id: [uniprot, entrez]
  label_in_input: [uniprot_protein, entrez_protein]
  properties:
    sequence: str
    description: str
    taxon: str
    mass: int
```

This will add the `mass` property to all proteins (in addition to the three we
had before); if not encountered, the column will be empty. Implicit subclasses
will automatically inherit the property configuration; in this case, both
`uniprot.protein` and `entrez.protein` will have the `mass` property, even
though the `entrez` proteins do not have a `mass` value in the input data.

```{note}
If we wanted to ignore the mass value for all properties, we could simply
remove the `mass` key from the `properties` dictionary.
```

```{tip}
BioCypher provides feedback about property conflicts; try running the code
for this example (`04_properties.py`) with the schema configuration of the
previous section (`03_schema_config.yaml`) and see what happens.
```

### Inheriting properties
```{admonition} Tutorial files
:class: note
The code for this tutorial can be found at
`tutorial/05_property_inheritance.py`. Schema files are at
`tutorial/05_schema_config.yaml`. Data generation happens in
`tutorial/data_generator.py`.
```

Sometimes, explicit designation of properties requires a lot of maintenance
work, particularly for classes with many properties. In these cases, it may be
more convenient to inherit properties from a parent class. This is done by
adding a `properties` key to a suitable parent class configuration, and then
defining inheritance via the `is_a` key in the child class configuration and
setting the `inherit_properties` key to `true`.

Let's say we have an additional `protein isoform` class, which can reasonably
inherit from `protein` and should carry the same properties as the parent. We
can add the following to our schema configuration:

```{code-block} yaml
protein isoform:
  is_a: protein
  inherit_properties: true
  represented_as: node
  preferred_id: uniprot
  label_in_input: uniprot_isoform
```

This allows maintenance of property lists for many classes at once.

```{note}
Again, apart from adding the protein isoforms to the input stream, the code
for this example is identical to the previous one except for the reference to
the updated schema configuration.
```

```{hint}
We now create three separate data files, all of which are children of the
`protein` class; two implicit children (`uniprot.protein` and `entrez.protein`)
and one explicit child (`protein isoform`).
```

(tut_relationships)=
## Section 4: Handling relationships
```{admonition} Tutorial files
:class: note
The code for this tutorial can be found at `tutorial/06_relationships.py`.
Schema files are at `tutorial/06_schema_config.yaml`. Data generation happens
in `tutorial/data_generator.py`.
```

Naturally, we do not only want nodes in our knowledge graph, but also edges. In
BioCypher, the configuration of relationships is very similar to that of nodes,
with some key differences. First the similarities: the top-level class
configuration of edges is the same; class names refer to ontological classes or
are an extension thereof. Similarly, the `is_a` key is used to define
inheritance, and the `inherit_properties` key is used to inherit properties
from a parent class. Relationships also possess a `preferred_id` key, a
`label_in_input` key, and a `properties` key, which work in the same way as for
nodes.

Relationships also have a `represented_as` key, which in this case can be
either `node` or `edge`. The `node` option is used to "reify" the relationship
in order to be able to connect it to other nodes in the graph. In addition to
the configuration of nodes, relationships also have fields for the `source` and
`target` node types, which refer to the ontological classes of the respective
nodes, and are currently optional.

To add protein-protein interactions to our graph, we can add the following to
the schema configuration above:

```{code-block} yaml
protein protein interaction:
  is_a: pairwise molecular interaction
  represented_as: node
  preferred_id: intact
  label_in_input: interacts_with
  properties:
    method: str
    source: str
```

Here, we use explicit subclassing to define the protein-protein interaction,
which is not represented in the basic Biolink model, as a direct child of the
Biolink "pairwise molecular interaction" class. We also reify this relationship
by representing it as a node. This allows us to connect it to other nodes in
the graph, for example to evidences for each interaction. If we do not want to
reify the relationship, we can set `represented_as` to `edge` instead.
