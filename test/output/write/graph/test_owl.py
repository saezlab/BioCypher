import glob
import os

import pytest

from rdflib import (
    OWL,
    RDFS,
    URIRef,
)

from rdflib import CSVW, RDF, Graph, Literal


def fix_ontology(bw_owl):
    entity = list(bw_owl.graph.triples((None, RDFS.subClassOf, OWL.Thing)))
    if not entity:
        # Manually add Entity as having an ancestor,
        # or else Biocypher will not consider it.
        # FIXME is this a bug of BioCypher?
        bw_owl.graph.add((URIRef(bw_owl.namespaces["biocypher"]["Entity"]), RDFS.subClassOf, OWL.Thing))


@pytest.mark.parametrize("length", [4], scope="function")
def test_owl_write_data(bw_owl, length, _get_nodes, _get_edges):
    # See test/fixtures/owl.py
    bw_owl.edge_model = "Association"

    # logging.basicConfig()
    # logger = logging.getLogger("biocypher")

    nodes = _get_nodes
    edges = _get_edges

    fix_ontology(bw_owl)

    nodes = bw_owl.write_nodes(nodes)
    edges = bw_owl.write_edges(edges)

    # check if the writing of the nodes went okay.
    assert all([nodes, edges])

    tmp_path = bw_owl.outdir

    owl_file_path = os.path.join(tmp_path, "*.ttl")
    owl_file = glob.glob(owl_file_path)
    assert len(owl_file) == 1

    graph = Graph()
    with open(owl_file[0]) as f:
        temp_graph = Graph().parse(data=f.read(), format="turtle")
        graph += temp_graph

    biocypher_namespace = bw_owl.namespaces["biocypher"]

    # All nodes should have an identifier
    assert len(set(graph.subjects(biocypher_namespace["id"]))) == 8

    # everything should have a type assigned
    assert len(set(graph.subjects(RDF.type))) == 1550

    # check if all the nodes have assigned their properties
    assert len(set(graph.subject_objects())) == 9890

    # assert the classes
    # FIXME it is strange that everything is a Class in the input ontology, why no types hierarchy?
    assert (bw_owl.namespaces["biolink"]["Protein"], RDF.type, OWL.Class) in graph
    assert (bw_owl.namespaces["biolink"]["MicroRNA"], RDF.type, OWL.Class) in graph
    assert (
        biocypher_namespace["PERTURBED_IN_DISEASE"],
        RDF.type,
        OWL.Class,
    ) in graph
    assert (biocypher_namespace["Is_Mutated_In"], RDF.type, OWL.Class) in graph

    for i in range(4):
        # assert the properties of the nodes
        assert (
            biocypher_namespace[f"p{i + 1}"],
            biocypher_namespace["score"],
            Literal(float("{:.6e}".format(4 / (i + 1)).replace(".000000", ""))),
        ) in graph
        assert (
            biocypher_namespace[f"p{i + 1}"],
            CSVW.name,
            Literal("StringProperty1"),
        ) in graph
        assert (
            biocypher_namespace[f"p{i + 1}"],
            biocypher_namespace["taxon"],
            Literal(9606),
        ) in graph
        assert (
            biocypher_namespace[f"p{i + 1}"],
            biocypher_namespace["genes"],
            Literal("gene1"),
        ) in graph
        assert (
            biocypher_namespace[f"p{i + 1}"],
            biocypher_namespace["genes"],
            Literal("gene2"),
        ) in graph
        assert (
            biocypher_namespace[f"p{i + 1}"],
            biocypher_namespace["id"],
            Literal(f"p{i + 1}"),
        ) in graph
        assert (
            biocypher_namespace[f"p{i + 1}"],
            biocypher_namespace["preferred_id"],
            Literal("uniprot"),
        ) in graph
        assert (
            biocypher_namespace[f"p{i + 1}"],
            RDF.type,
            biocypher_namespace["Protein"],
        ) in graph

        assert (
            biocypher_namespace[f"m{i + 1}"],
            CSVW.name,
            Literal("StringProperty1"),
        ) in graph
        assert (
            biocypher_namespace[f"m{i + 1}"],
            biocypher_namespace["taxon"],
            Literal(9606),
        ) in graph
        assert (
            biocypher_namespace[f"m{i + 1}"],
            biocypher_namespace["id"],
            Literal(f"m{i + 1}"),
        ) in graph
        assert (
            biocypher_namespace[f"m{i + 1}"],
            biocypher_namespace["preferred_id"],
            Literal("mirbase"),
        ) in graph
        assert (
            biocypher_namespace[f"m{i + 1}"],
            RDF.type,
            biocypher_namespace["MicroRNA"],
        ) in graph

        # assert the relationship of the nodes
        assert (
            biocypher_namespace[f"p{i}"],
            biocypher_namespace["edge_source"],
            biocypher_namespace[f"prel{i}"],
        ) in graph
        assert (
            biocypher_namespace[f"prel{i}"],
            biocypher_namespace["edge_target"],
            biocypher_namespace[f"p{i + 1}"],
        ) in graph
        assert (
            biocypher_namespace[f"prel{i}"],
            biocypher_namespace["residue"],
            Literal("T253"),
        ) in graph
        assert (
            biocypher_namespace[f"prel{i}"],
            biocypher_namespace["level"],
            Literal(4),
        ) in graph
        assert (
            biocypher_namespace[f"prel{i}"],
            RDF.type,
            biocypher_namespace["PERTURBED_IN_DISEASE"],
        ) in graph

        assert (
            biocypher_namespace[f"m{i}"],
            biocypher_namespace["edge_source"],
            biocypher_namespace[f"mrel{i}"],
        ) in graph
        assert (
            biocypher_namespace[f"mrel{i}"],
            biocypher_namespace["edge_target"],
            biocypher_namespace[f"p{i + 1}"],
        ) in graph
        assert (
            biocypher_namespace[f"mrel{i}"],
            biocypher_namespace["site"],
            Literal("3-UTR"),
        ) in graph
        assert (
            biocypher_namespace[f"mrel{i}"],
            biocypher_namespace["confidence"],
            Literal(1),
        ) in graph
        assert (
            biocypher_namespace[f"mrel{i}"],
            RDF.type,
            biocypher_namespace["Is_Mutated_In"],
        ) in graph
