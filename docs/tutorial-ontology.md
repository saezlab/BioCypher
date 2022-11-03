# BioCypher Tutorial: Handling Ontologies
BioCypher relies on ontologies to ground the knowledge graph contents in
biology. This has the advantage of making working with BioCypher accessible to
biologically oriented researchers. However, it also means that BioCypher
requires a certain amount of knowledge about ontologies and how to use them. We
try to make dealing with ontologies as easy as possible, but some basic
understanding is required. In the following we will cover the basics of
ontologies and how to use them in BioCypher.

## What is an ontology?
An ontology is a formal representation of a domain of knowledge. It is a
hierarchical structure of concepts and relations. The concepts are organized
into a hierarchy, where each concept is a subclass of a more general concept.
For instance, a *wardrobe* is a subclass of a *piece of furniture*. Individual
wardrobes, such as yours or mine, are instances of the concept *wardrobe*, and
as such would be represented as *Wardrobe* nodes in a knowledge graph. In
BioCypher, these nodes would additionally inherit the *PieceOfFurniture* label
from the ontological hierarchy of things.

```{note}
Why is the class called *piece of furniture* but the label is
*PieceOfFurniture*?

The Biolink model uses two different case notations for its labels: the
"internal" designation of classes is in lower sentence case ("protein",
"pairwise molecular interaction"), while the "external" designation is in
PascalCase ("Protein", "PairwiseMolecularInteraction"). BioCypher uses the same
paradigm: in most cases (input, schema configuration, internally), the lower
sentence case is used, while in the output (Neo4j labels, file system names) the
PascalCase is more suitable; Neo4j labels and system file names don't deal well
with spaces and special characters. We also remove the "biolink:"
[CURIE](https://en.wikipedia.org/wiki/CURIE) prefix for use in file names and
Neo4j labels.
```

The relations between concepts can also be organized into a hierarchy. In the
specific case of a Neo4j graph, however, relationships cannot possess multiple
labels; therefore, if concept inheritance is desired for relationships, they
need to be "reified", i.e., turned into nodes. BioCypher provides a simple way
of converting edges to nodes and vice versa (using the `represented_as` field).

## How BioCypher uses ontologies
BioCypher is - in principle - agnostic to the choice of ontology. Practically,
however, we have built our initial projects around the [Biolink
model](https://biolink.github.io/biolink-model/), because it comprises a large
part of concepts that are relevant to the biomedical domain.  This does not mean
that it is the only ontology that can be used with BioCypher. In fact, it is
possible to use multiple ontologies in the same project. For instance, one might
want to extend the rather basic classes relating to molecular interactions in
Biolink (the most specific being `pairwise molecular interaction`) with more
specific classes from a more domain-specific ontology, such as the EBI molecular
interactions ontology ([PSI-MI](https://www.ebi.ac.uk/ols/ontologies/mi)).

<!-- TODO example -->

## Using ontologies: plain Biolink
BioCypher maps any input data to the underlying ontology; in the basic case, the
Biolink model. This mapping is defined in the schema configuration
(`schema_config.yaml`, see also [here](qs_schema-config)). In the simplest case,
the representation of a concept in the knowledge graph to be built and the
Biolink model class representing this concept are synonymous. For instance, the
concept *protein* is represented by the Biolink class [*protein*](). To
introduce proteins into the knowledge graph, one would simply define a node
constituent with the class label *protein*. This is the mechanism we implicitly
used for proteins in the basic tutorial; to reiterate:

```{code-block} yaml
:caption: schema_config.yaml
protein:
  represented_as: node
  # ...
```

## Biolink model extensions
There are multiple reasons why a user might want to modify the Biolink model.  A
class that is relevant to the user's task might be missing ([Explicit
inheritance](tut_explicit)). A class might be not granular enough, and the user
would like to split it into subclasses based on distinct inputs ([Implicit
inheritance](tut_implicit)). The name of a Biolink model class may be too
unwieldy for the use inside the desired knowledge graph, and the user would like
to introduce a synonym ([Synonyms](tut_synonyms)). For some very common use
cases, we recommend going one step further and, maybe after some testing,
proposing the introduction of a new class to the Biolink model itself. Biolink
is an open source community project, and new classes can be requested by opening
an issue or filing a pull request directly on the [Biolink model GitHub
repository](https://github.com/biolink/biolink-model). To prepare this pull
request (or to maintain a local version of the Biolink model), BioCypher allows
building the Biolink model from a modified YAML file ([Custom Biolink
model](tut_custom)). Or, the user might want to extend the Biolink model with
another ontology, such as the EBI molecular interactions ontology ([Hybridising
ontologies](tut_hybridising)).

(tut_explicit)=
### Explicit inheritance
Explicit inheritance is the most straightforward way of extending the Biolink
model. It is also the most common use case. For instance, the Biolink model
does not contain a class for `protein isoform`, and neither does it contain a
relationship class for `protein protein interaction`, both of which we have
already used in the basic tutorial. Since protein isoforms are specific types of
protein, it makes sense to extend the existing Biolink model class `protein`
with the concept of protein isoforms. To do this, we simple add a new class
`protein isoform` to the schema configuration, and specify that it is a subclass
of `protein` using the (optional) `is_a` field:

```{code-block} yaml
:caption: schema_config.yaml
protein isoform:
  is_a: protein
  represented_as: node
  # ...
```

Explicit inheritance can also be used to introduce new relationship classes.
However, if the output is a Neo4j graph, these relationships must be represented
as nodes, because edges do not allow multiple labels. For instance, we have
already used the `protein protein interaction` relationship in the basic
tutorial ([part 6](tut_relationships)), making it a child of the Biolink model
class `pairwise molecular interaction`. To reiterate:

```{code-block} yaml
:caption: schema_config.yaml
protein protein interaction:
  is_a: pairwise molecular interaction
  represented_as: node
  # ...
```

The `is_a` field can be used to specify multiple inheritance, i.e., multiple
ancestor classes and their direct parent-child relationships can be created by
specifying multiple classes (as a list) in the `is_a` field. For instance, if we
wanted to further extend the protein-protein interaction with a more specific
`enzymatic interaction` class, we could do so as follows:

```{code-block} yaml
:caption: schema_config.yaml
enzymatic interaction:
  is_a: [protein protein interaction, molecular interaction]
  represented_as: node
  # ...
```

```{note}
To create this multiple inheritance chain, we do not require the creation of
a `protein protein interaction` class as shown above; all intermediary classes
are automatically created by BioCypher and inserted into the ontological
hierarchy.
```

(tut_implicit)=
### Implicit inheritance
The base model (in the standard case, Biolink) can also be extended without
specifying an explicit `is_a` field. This "implicit" inheritance happens when
a class has multiple input labels that each refer to a distinct preferred
identifier. In other words, if both the `label_in_input` and the `preferred_id`
fields of a schema configuration class are lists, BioCypher will automatically
create a subclass for each of the preferred identifiers. This is demonstrated in
[part 3](implicit_subclass) of the basic tutorial.

```{caution}
If only the `label_in_input` field - but not the `preferred_id` field - is a
list, BioCypher will merge the inputs instead. This is useful for cases where
different input streams should be unified under the same class label. See
[part 2](merging) of the basic tutorial for more information.
```

To make this more concrete, let's consider the example of `pathway` annotations.
There are multiple projects that provide pathway annotations, such as Reactome
and Wikipathways, and, in contrast to proteins, pathways are not easily mapped
one-to-one. For classes where mapping is difficult or even impossible, we can
use implicit subclassing instead. The Biolink model contains a `pathway` class,
which we can use as a parent class of the Reactome and Wikipathways classes; we
simply need to provide the pathways as two separate inputs with their own labels
(e.g., "react" and "wiki"), and specify a corresponding list of
preferred identifiers in the `preferred_id` field:

```{code-block} yaml
:caption: schema_config.yaml
pathway:
  represented_as: node
  preferred_id: [reactome, wikipathways]
  label_in_input: [react, wiki]
  # ...
```

This will prompt BioCypher to create two subclasses of `pathway`, one for each
input, and to map the input data to these subclasses. In the resulting knowledge
graph, the Reactome and Wikipathways pathways will be represented as distinct
classes by prepending the preferred identifier to the class label:
`Reactome.Pathway` and `Wikipathways.Pathway`. By virtue of BioCypher's multiple
labelling paradigm, those nodes will also inherit the `Pathway` class label as
well as all parent labels and mixins of `Pathway` (`BiologicalProcess`, etc.).
This allows us to query the graph for all `Pathway` nodes as well as for
specific datasets depending on the desired granularity.

```{note}
This also works for relationships, but in this case, not the preferred
identifiers but the sources (defined in the `source` field) are used to
create the subclasses.
```

For instance, if we wanted to create subclasses for pathway membership
relationships of proteins from two different sources, such as the above Reactome
and Wikipathways pathways, we could do so as follows:

```{code-block} yaml
:caption: schema_config.yaml
pathway to protein association:
  is_a: association
  represented_as: node
  source: [reactome.pathway, wikipathways.pathway]
  target: protein
  label_in_input: [react_protein_membership, wiki_protein_membership]
  # ...
```

Like in the above example, this will prompt BioCypher to create two subclasses
of `pathway to protein association`, one for each input, and to map the input
data to these subclasses. In the resulting knowledge graph, pathway to protein
associations from Reactome and Wikipathways will be represented as distinct
subclasses by prepending the source to the relationship label, i.e., as
`Reactome.PathwayToProteinAssociation` and
`Wikipathways.PathwayToProteinAssociation`.

<!-- TODO tutorial code -->
<!-- TODO are these really the labels? do the sources need to be provided in
sentence case or PascalCase? -->

(tut_synonyms)=
### Synonyms
In some cases, Biolink may contain a biological concept, but the name of the
concept in Biolink does for some reason not agree with the users desired
knowledge graph structure. For instance, the user may not want to represent
clinical phenotypes of patients in the graph as `clinical finding` nodes
due to ease of use and/or readability criteria and rather call these nodes
`phenotype`. In such cases, the user can introduce a synonym for the Biolink
class. This is done by selecting another, more desirable name for the respective
class(es) and specifying the `synonym_for` field in their schema configuration.
In this case, we would like to represent patient phenotypes as `phenotype`
nodes, and we can do so as follows:

```{code-block} yaml
:caption: schema_config.yaml
phenotype:
  synonym_for: clinical finding
  represented_as: node
  # ...
```

Importantly, BioCypher preserves these mappings to enable compatibility between
different structural instantiations of the Biolink model. All entities that are
mapped to Biolink classes in any way can be harmonised even between different
types of concrete representations.

```{note}
It is essential that the desired class name is used as the main class key in
the schema configuration, and the Biolink class name is given in the
`synonym_for` field. The name given in the `synonym_for` field must be an
existing Biolink class name.
```

(tut_custom)=
### Custom Biolink model
The Biolink model is a living entity, and it is constantly being updated and
extended. However, it is not always possible to wait for the Biolink model to
be updated to include a new concept or relationship. In such cases, it is
possible to create a custom Biolink model that can be used in BioCypher. This
is done by specifying a custom Biolink model to be built using the [Biolink
Model Toolkit (BMT)](https://github.com/biolink/biolink-model-toolkit) in the
BioCypher workflow. We provide one such model with BioCypher directly
(`biocypher/_config/biocypher-biolink-model.yaml`), which is a modified version
of the original Biolink model that includes a *post translational interaction*
class as well as a custom mixin (so far only for demonstration purposes).

```{caution}
The custom Biolink model provided by BioCypher is built using an older version
of the Biolink model and as such is not 100% compatible with the current
version. BioCypher defaults to the standard Biolink model to enable highest
possible compatibility. However, if a custom Biolink model is specified, it is
useful to record the version of the Biolink model that the custom model is based
on.
```

<!-- TODO provide config access to custom biolink model path -->

(tut_hybridising)=
### Hybridising ontologies
Not implemented yet; coming soon.
