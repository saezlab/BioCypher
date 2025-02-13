# OWL

The Web Ontology Language (OWL) is a (family of) knowledge representation
language(s) for authoring ontologies. BioCypher can use taxonomies written in
OWL as an input, and it can also output a knowledge graph in an OWL file.

OWL is one of the most used knowledge representation languages and is built
on the Resource Description Framework (RDF) and thus is partly compatible with
the RDF Schema data model. It can be serialized in several formats (most known
being XML and Turtle). The [Protégé](https://protege.stanford.edu/) software
is the *de facto* standard graphical user interface to design OWL ontologies.

In BioCypher, selecting the `owl` output format will call the `_OWLWriter` class
and generate a self-contained OWL file. The file is said to be "self-contained"
because it holds both the vocabulary (i.e. a part of the hierarchy of classes
from the input ontology) and the instances (i.e. "nodes", for Biocypher).


## Edge Model

The behavior of edge creation in the RDF output relies mainly on the
`edge_model` parameter, which can take two values: "ObjectProperty" or
"Association".


### ObjectProperty

This edge model translates BioCypher's edges into
OWL's object properties (if they are available under the
selected root term). Object properties are the natural way
to model edges in OWL, but they do not support annotation,
thus being incompatible with having BioCypher's properties
on edges.

As most of OWL files do not model a common term on top of both
owl:topObjectProperty and owl:Thing, you may need to ensure
that the input OWL contains a "meta-root", that is a
common ancestor honoring both:

- owl:Thing rdfs:subClassOf <root_node>
- owl:topObjectProperty rdfs:subPropertyOf <root_node>

It is this meta-root that you should select as a `root_node` in your BioCypher
configuration.


### Association

This edge model (the default) translates BioCypher's
edges into OWL's class instances. Those edges instances are
inserted in between the instances coming from BioCypher's nodes.
This allows to retain edge properties, but adds OWL instances
to model relationships, which does not follow the classical
OWL model.

In this approach, all OWL instances are linked
with a generic "edge\_source" (linking source instance to
the association instance) and "edge\_target" (linking the association
instance to the target instance). Both of which inherit from "edge",
and are in the biocypher namespace.

If you use this edge model, you should select one of the subclasses of
owl:Thing as a `root_node`, and not select any part of the object property tree.


## Taxonomy Management

This class takes care of keeping the vocabulary underneath the
selected root node and exports it along the instances in the
resulting OWL file. It discards all terms that are not in the
tree below the selected root node.

The configuration parameter `rdf_namespaces` can be used to specify which
namespaces exist in the input ontology (or the data). If the data contain IDs
with a given prefix, they will be converted into valid Uniform Resource
Identifiers (URI) to allow referencing. If no namespace is specified, BioCypher
will search for them in the input ontology.


## Settings

Important parameters are:

- `root_node`, which must be a meta-root on top of both owl:Thing and
  owl:topObjectProperty.
- `edge_model` heavily impacts the output ontology, most notably the graph
  structure, and thus the queries that can be made on it (see above).
- `file_stem` is the name of the output file (without the extension or the path)
  which will be written in the output directory.
- `rdf_format` is the output serialization format. Note that if set to "turtle",
  the output file extension will be ".ttl", but you cannot indicate "ttl" as an
  rdf_format.

### For the OpjectProperty edge model

```{code-block} yaml
:caption: biocypher_config.yaml

biocypher:
    strict_mode: true
    schema_config_path: config/schema_config.yaml
    dbms: owl # <- Use the OWL output writer.

    head_ontology:
        url: file:///home/superb/owl_file.ttl
        root_node: BioCypherRoot # <- The "meta-root" class.

owl:
    rdf_format: turtle # <- Note that this is not "ttl".
    # Can be either: xml, n3, turtle, nt, pretty-xml, trix, trig, nquads, json-ld

    edge_model: ObjectProperty
    # Can also be: Association (the default)

    file_stem: my_ontology # "biocypher" by default, do not put an extension

    # Optional:
    rdf_namespaces:
        so: http://purl.obolibrary.org/obo/SO_
        efo: http://www.ebi.ac.uk/efo/EFO_
```

### For the Association edge model

```{code-block} yaml
:caption: biocypher_config.yaml

biocypher:
    strict_mode: true
    schema_config_path: config/schema_config.yaml
    dbms: owl # <- Use the OWL output writer.

    head_ontology:
        url: file:///home/superb/owl_file.ttl
        root_node: Entity # <- NOT the meta-root!

owl:
    rdf_format: turtle
    # Can be either: xml, n3, turtle, nt, pretty-xml, trix, trig, nquads, json-ld

    edge_model: Association

    file_stem: my_ontology # "biocypher" by default, do not put an extension

    # Optional:
    rdf_namespaces:
        so: http://purl.obolibrary.org/obo/SO_
        efo: http://www.ebi.ac.uk/efo/EFO_
```

## Possible Issues

Biocypher is not able to read all OWL ontologies, and not all of the terms
hosted in an OWL ontology. Most notably, it only reads (a part of) the taxonomy
to build up its input. Some logical predicates may also be incompatible with
the selected edge model (especially "Association").

Note that Protégé may show a couple of impediments:

- It displays owl:Entity as if it inherits from owl:Thing, but that is not
  necessarily actually implemented by a predicate. You may have to add it
  manually.
- It displays all owl:ObjectProperty as if they inherit from
  owl:topObjectProperty, but you may also have to add the predicate manually.
- It provides no easy way to add a meta-root on top of both classes, and a
  manually added one will appear as a *subclass* of both owl:Thing and
  owl:topObjectProperty.

Double-checking the ontology file source code itself should help you ensure
compatibility with BioCypher's constraints.

Also, note that BioCypher requires that classes (and object properties) have an
RDFS label, and will use it (and not the IRI) to find the necessary types.
