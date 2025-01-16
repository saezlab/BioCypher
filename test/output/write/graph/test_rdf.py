import glob
import os

import pytest

from rdflib import CSVW, RDF, RDFS, Graph, Literal, Namespace


@pytest.mark.parametrize("length", [4], scope="function")
def test_rdf_write_data(bw_rdf, length, _get_nodes, _get_edges):
    nodes = _get_nodes
    edges = _get_edges

    nodes = bw_rdf.write_nodes(nodes)
    edges = bw_rdf.write_edges(edges)

    # check if the writing of the nodes went okay.
    assert all([nodes, edges])

    tmp_path = bw_rdf.outdir

    rdf_files_path = os.path.join(tmp_path, "*.xml")
    rdf_files = glob.glob(rdf_files_path)

    graph = Graph()
    for file in rdf_files:
        with open(file) as f:
            temp_graph = Graph().parse(data=f.read(), format="xml")
            graph += temp_graph

    biocypher_namespace = Namespace("https://biocypher.org/biocypher#")

    # All nodes should have an identifier
    assert len(set(graph.subjects(biocypher_namespace["id"]))) == 8

    # everything should have a type assigned
    assert len(set(graph.subjects(RDF.type))) == 20

    # check if all the nodes have assigned their properties
    assert len(set(graph.subject_objects())) == 96

    # assert the classes
    assert (biocypher_namespace["Protein"], RDF.type, RDFS.Class) in graph
    assert (biocypher_namespace["MicroRNA"], RDF.type, RDFS.Class) in graph
    assert (
        biocypher_namespace["PERTURBED_IN_DISEASE"],
        RDF.type,
        RDFS.Class,
    ) in graph
    assert (biocypher_namespace["Is_Mutated_In"], RDF.type, RDFS.Class) in graph

    for i in range(4):
        # assert the properties of the nodes
        assert (
            biocypher_namespace[f"p{i + 1}"],
            biocypher_namespace["score"],
            Literal(4 / (i + 1)),
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
            biocypher_namespace[f"prel{i}"],
            biocypher_namespace["subject"],
            biocypher_namespace[f"p{i}"],
        ) in graph
        assert (
            biocypher_namespace[f"prel{i}"],
            biocypher_namespace["object"],
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
            biocypher_namespace[f"mrel{i}"],
            biocypher_namespace["subject"],
            biocypher_namespace[f"m{i}"],
        ) in graph
        assert (
            biocypher_namespace[f"mrel{i}"],
            biocypher_namespace["object"],
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
