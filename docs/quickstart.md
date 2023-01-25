# Quickstart

```{note}
If you already know how BioCypher works, we provide here a quickstart into the
knowledge graph build process. We provide a template repository on GitHub, which
you can use to get started with your own project.  You can get it
[here](https://github.com/saezlab/biocypher-project-template).  To set up a new
project, simply follow the instructions in the README.

If you are new to BioCypher and would like a step-by-step introduction to the
package, please follow the [tutorial](tutorial).
```

The main interface for interacting with the BioCypher module to create
your own property graph consists of two components:

1. the [host module adapter](qs_host-module-adapter), a python
   program, and
2. the [schema configuration file](qs_schema-config), a YAML file.

The adapter serves as a data interface between the source and BioCypher,
piping the "raw" data into BioCypher for the creation of the property
graph, while the schema configuration tells BioCypher how the graph
should be structured, detailing the names of constituents and how they
should be connected.

(qs_host-module-adapter)=
## The host module adapter

Currently, BioCypher expects input from the user module via an adapter module.
The adapter has the job of piping the data as it is represented in the original
database into the BioCypher input, for instance as a {py:class}`Generator`
object of single database entries, whether they be nodes or relationships in the
graph to be created. Examples for current adapters can be found in several of
our ongoing projects and the section on [adapter functions](adapter_functions).
Adapters can ingest data from many different input sources, including Python
modules as in the [CROssBAR
adapter](https://github.com/HUBioDataLab/CROssBAR-BioCypher-Migration) (which
uses the OmniPath backend software, PyPath, for downloading and caching data),
advanced file management formats such as Parquet as in the [Open Targets
adapter](https://github.com/saezlab/OTAR-BioCypher), or simple CSV files as in
the [Dependency Map adapter](https://github.com/saezlab/DepMap-BioCypher).

The recommended mode of access into BioCypher functionality is via the
{py:class}``biocypher._driver.Driver`` class. It can be called either
starting in "offline mode" using `offline = True`, i.e., without
connection to a running Neo4j instance, or by providing authentication
details via arguments or configuration file:

```{code-block} python
import biocypher
d = biocypher.Driver(
  offline = False,
  db_uri = "bolt://localhost:7687",
  db_user = "neo4j",
  db_passwd = "password",
)
```

```{note}
We use the APOC library for Neo4j, which is not included automatically, but
needs to be installed as a plugin to the DMBS. For more information, please
refer to the [APOC documentation](https://neo4j.com/labs/apoc/).
```

```{hint}
The settings for the BioCypher driver can also be specified in a configuration
file. For more details, please refer to the [Setup instructions](config).
```

The main function of the adapter is to pass data into BioCypher, usually
as some form of iterable (commonly a list or generator of items). As a
minimal example, we load a list of proteins with identifiers, trivial
names, and molecular masses from a (fictional) CSV:

```{code-block} python
# read into data frame
with open("file.csv", "r") as f:
  proteins = pd.read_csv(f)

# yield proteins from data frame
def node_generator():
  for p in proteins:
    _id = p["uniprot_id"]
    _type = "protein"
    _props = {
      "name": p["trivial_name"]
      "mm": p["molecular_mass"]
    }

    yield (_id, _type, _props)

# write biocypher nodes
d.write_nodes(node_generator())
```

For nodes, BioCypher expects a tuple containing three entries; the preferred
identifier of the node, the type of entity, and a dictionary containing all
other properties (can be empty). What BioCypher does with the received
information is determined largely by the schema configuration detailed below.

For advanced usage, the type of node or edge can be determined
programatically. Properties do not need to be explicitly called one by
one; they can be passed in as a complete dictionary of all entries and
filtered inside BioCypher by detailing the desired properties per node
type in the schema configuration file.

(qs_schema-config)=
## The schema configuration YAML file

The second important component of translation into a
BioCypher-compatible property graph is the specification of graph
constituents and their mode of representation in the graph. For
instance, we want to add a representation for proteins to the OmniPath
graph, and the proteins should be represented as nodes. To make this
known to the BioCypher module, we use the
[schema-config.yaml](https://github.com/saezlab/BioCypher/blob/main/biocypher/_config/schema_config.yaml),
which details *only* the immediate constituents of the desired graph.
Since the identifier systems in the Biolink schema are not comprehensive
and offer many alternatives, we currently use the CURIE prefixes
directly as given by [Bioregistry](https://bioregistry.io). For
instance, a protein could be represented, for instance, by a UniProt
identifier, the corresponding ENSEMBL identifier, or an HGNC gene
symbol. The CURIE prefix for "Uniprot Protein" is `uniprot`, so a
consistent protein schema definition would be:

```{code-block} yaml
protein:
  represented_as: node 
  preferred_id: uniprot  
  label_in_input: protein  
```

```{note}
For BioCypher classes, similar to the internal representation in the Biolink
model, we use lower sentence-case notation, e.g., `protein` and `small 
molecule`. For file names and Neo4j labels, these are converted to PascalCase.
```

In the protein case, we are specifying its representation as a node,
that we wish to use the UniProt identifier as the main identifier for
proteins, and that proteins in the input coming from ``PyPath`` carry
the label ``protein`` (in lowercase). Should one wish to use ENSEMBL
notation instead of UniProt, the corresponding CURIE prefix, in this
case, `ensembl`, can be substituted.

```{code-block} yaml
protein:
  represented_as: node 
  preferred_id: ensembl  
  label_in_input: protein  
```

If there exists no identifier system that is suitable for coverage of
the data, the standard field `id` can be used; this will not result in
the creation of a named property that reflects the identifier of each
node. See below for an example. The `preferred_id` field can in this case also
be omitted entirely; this will lead to the same outcome (`id`).

The other slots of a graph constituent entry contain information
BioCypher needs to receive the input data correctly and construct the
graph accordingly. For "Named Thing" entities such as the protein, this
includes the mode of representation (YAML entry ``represented_as``),
which can be ``node`` or ``edge``. Proteins can only feasibly
represented as nodes, but for other entities, such as interactions or
aggregates, representation can be both as node or as edge. In Biolink,
these belong to the super-class
[Associations](https://biolink.github.io/biolink-model/docs/associations.html).
For associations, BioCypher additionally requires the specification of
the source and target of the association; for instance, a
post-translational interaction occurs between proteins, so the source
and target attribute in the ``schema-config.yaml`` will both be
``protein``.

```{code-block} yaml
post translational interaction:
  represented_as: node
  preferred_id: id
  source: protein 
  target: protein 
  label_in_input: post_translational 
```

For the post-translational interaction, which is an association, we are
specifying representation as a node (prompting BioCypher to create not
only the node but also two edges connecting to the proteins
participating in any particular post-translational interaction). In
other words, we are reifying the post-translational interaction in order
to have a node to which other nodes can be linked; for instance, we
might want to add a publication to a particular interaction to serve as
source of evidence, which is only possible for nodes in a property
graph, not for edges.

Since there are no systematic identifiers for post-translational
interactions, we concatenate the protein ids and relevant properties of
the interaction to a new unique id. We prevent creation of a specific
named property by specifying `id` as the identifier system in this case.
If a specific property name (in addition to the generic `id` field) is
desired, one can use any arbitrary string as a designation for this
identifier, which will then be a named property on the
``PostTranslationalInteraction`` nodes.

```{note}
BioCypher accepts non-Biolink IDs since not all possible entries possess a
systematic identifier system, whereas the entity class (``protein``,
``post translational interaction``) has to be included in the Biolink schema and
spelled identically. For this reason, we [extend the Biolink schema](qs_biolink)
in cases where there exists no entry for our entity of choice. Further, we are
specifying the source and target classes of our association (both ``protein``),
the label we provide in the input from ``PyPath`` (``post_translational``). 
```

If we wanted the interaction to be represented in the graph as an edge,
we would also need to supply an additional - arbitrary - property,
`label_as_edge`, which would be used as the relationship type; this
could for instance be `INTERACTS_POST_TRANSLATIONALLY`, following the
property graph database consensus that property graph edges are
represented in all upper case form and as verbs, to distinguish from
nodes that are represented in PascalCase and as nouns. This would modify
the above example to the following:

```{code-block} yaml
post translational interaction:
  represented_as: edge
  preferred_id: id
  source: protein 
  target: protein 
  label_in_input: post_translational 
  label_as_edge: INTERACTS_POST_TRANSLATIONALLY
```

(qs_biolink)=
## The Biolink model extension

### Soft extensions

In some cases that are not too complex, the Biolink model can be
extended using only implicit subclasses given in the BioCypher
schema_config.yaml file. Soft extensions can be achieved in two ways:
- via [implicit subclasses](qs_implicit) of existing Biolink classes by supplying
  multiple preferred ids and input labels in the `schema_config.yaml`
- via [explicit subclasses](qs_explicit) of existing Biolink classes by supplying an
  `is_a` parameter to any non-biolink class, referring to an existing
  one

(qs_implicit)=
#### Implicit subclasses

For instance, Pathway annotations are supplied by multiple sources,
e.g., [KEGG](https://www.genome.jp/kegg/pathway.html) and
[Reactome](https://reactome.org/), which do not allow direct mapping due
to their distinct make-up. In this case, we can use the
schema_config.yaml to implicitly extend the Biolink model by specifying
more than one preferred_id and label_in_input (in a paired manner):

```{code-block} yaml
pathway:
  represented_as: node
  preferred_id: [reactome, kegg] 
  label_in_input: [reactome, kegg_pathway] 
```

This pattern will be parsed by BioCypher to yield two implicit children
of the Biolink entity "pathway" by prepending the preferred ID to the
label (``reactome.pathway`` and ``kegg.pathway``). The input labels again are
arbitrary and can be adjusted to fit the way these pathways are
represented in the raw data.

This will allow both datasets to be represented as pathways in the final
graph with granular access to each one and without information loss, but
will also enable aggregate query of all pathways by calling the parent 
entity, ``pathway``.

(qs_explicit)=
#### Explicit subclasses

For example, adding the child `tissue` to the existing Biolink class
`gross anatomical structure` as a specification:

```{code-block} yaml
tissue:
  is_a: gross anatomical structure
  represented_as: node
  preferred_id: uberon
  label_in_input: tissue
```

This will create a "faux" Biolink class at BioCypher runtime, extending
the hierarchical tree to include `tissue` as a
`gross anatomical structure`, preserving the entire inheritance of the
parent class. BioCypher can create arbitrarily long inheritance
structures by accepting lists as input to the `is_a` field, as long as
the final entry in the list is an existant Biolink entity. All entities
along the list will be established as virtual children of the Biolink 
parent node. For example:

```{code-block} yaml
mutation to tissue association:
  is_a: [
    genotype to tissue association, 
    entity to tissue association, 
    association
  ]
```

Where only `association` is an existent class in the Biolink model.
Generally, it is preferable to add extensions to the Biolink model to
the original repository via a pull request to ensure compatibility with
other DBs. Creating a hard-wired extension as described in the next
section can be a first step towards a pull request to the Biolink repo.

### Hard-wired extensions

The post-translational interaction that we would like to model in
OmniPath has no literal counterpart in the Biolink model, due to
Biolink's design philosophy. The most granular level of interactions as
Biolink class is the
[PairwiseMolecularInteraction](https://biolink.github.io/biolink-model/docs/PairwiseMolecularInteraction.html);
all more granular relationships should be encoded in the properties of
the class, which has severe performance implications for property graph
representation, for instance in filtering for specific relationship
types. Briefly, it is the difference between being able to selectively
return only relationships of a certain class (eg, post-translational),
and having to return all relationships to filter for the ones possessing
the correct property in a second step.

Therefore, we extend the Biolink model in places where it is necessary
for the BioCypher translation and integration to work. The extended
model is the central Biolink YAML file with additions following the same
[LinkML](https://linkml.io) syntax as is used in the original model.
Depending on the extent of the modification, not only new classes are
introduced, but also new mixin categories (eg, "microRNA or siRNA" to
account for different types of small noncoding RNA). We provide [our
extended version of the Biolink
model](https://github.com/saezlab/BioCypher/blob/main/biocypher/_config/biocypher-biolink-model.yaml)
with the BioCypher repository.

Changes or additions desired by the user can be introduced locally in this file
without having to modify remote contents. Users also have the option to create
their own modified version of the Biolink YAML file under a different file name
and specify that path in the ``user_schema_config_path`` argument of the
{class}`biocypher.Driver` class, which handles all communication between
BioCypher and Biolink.
