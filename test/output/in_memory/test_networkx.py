import pytest


@pytest.mark.parametrize("length", [4], scope="module")
def test_nodes(in_memory_networkx_kg, _get_nodes):
    in_memory_networkx_kg.add_nodes(_get_nodes)
    networkx_kg = in_memory_networkx_kg.get_kg()
    expected_nodes = [
        (
            "p1",
            {
                "node_label": "protein",
                "score": 4.0,
                "name": "StringProperty1",
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
                "id": "p1",
                "preferred_id": "uniprot",
            },
        ),
        (
            "p2",
            {
                "node_label": "protein",
                "score": 2.0,
                "name": "StringProperty1",
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
                "id": "p2",
                "preferred_id": "uniprot",
            },
        ),
        (
            "p3",
            {
                "node_label": "protein",
                "score": 1.3333333333333333,
                "name": "StringProperty1",
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
                "id": "p3",
                "preferred_id": "uniprot",
            },
        ),
        (
            "p4",
            {
                "node_label": "protein",
                "score": 1.0,
                "name": "StringProperty1",
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
                "id": "p4",
                "preferred_id": "uniprot",
            },
        ),
        (
            "m1",
            {
                "node_label": "microRNA",
                "name": "StringProperty1",
                "taxon": 9606,
                "id": "m1",
                "preferred_id": "mirbase",
            },
        ),
        (
            "m2",
            {
                "node_label": "microRNA",
                "name": "StringProperty1",
                "taxon": 9606,
                "id": "m2",
                "preferred_id": "mirbase",
            },
        ),
        (
            "m3",
            {
                "node_label": "microRNA",
                "name": "StringProperty1",
                "taxon": 9606,
                "id": "m3",
                "preferred_id": "mirbase",
            },
        ),
        (
            "m4",
            {
                "node_label": "microRNA",
                "name": "StringProperty1",
                "taxon": 9606,
                "id": "m4",
                "preferred_id": "mirbase",
            },
        ),
    ]
    assert list(networkx_kg.nodes(data=True)) == expected_nodes


@pytest.mark.parametrize("length", [4], scope="module")
def test_nodes_gen(in_memory_networkx_kg, _get_nodes):
    def node_gen():
        for node in _get_nodes:
            yield node

    in_memory_networkx_kg.add_nodes(node_gen())
    networkx_kg = in_memory_networkx_kg.get_kg()
    assert len(networkx_kg.nodes(data=True)) == 8


@pytest.mark.parametrize("length", [4], scope="module")
def test_duplicates(in_memory_networkx_kg, _get_nodes):
    nodes = _get_nodes + _get_nodes
    in_memory_networkx_kg.add_nodes(nodes)
    networkx_kg = in_memory_networkx_kg.get_kg()
    protein_nodes = [node for node in networkx_kg.nodes(data=True) if node[1]["node_label"] == "protein"]
    assert len(protein_nodes) == 4


@pytest.mark.parametrize("length", [8], scope="module")
def test_two_step_add(in_memory_networkx_kg, _get_nodes):
    in_memory_networkx_kg.add_nodes(_get_nodes[:4])
    in_memory_networkx_kg.add_nodes(_get_nodes[4:])
    networkx_kg = in_memory_networkx_kg.get_kg()
    protein_nodes = [node for node in networkx_kg.nodes(data=True) if node[1]["node_label"] == "protein"]
    assert len(protein_nodes) == 8


@pytest.mark.parametrize("length", [4], scope="module")
def test_edges(in_memory_networkx_kg, _get_edges):
    in_memory_networkx_kg.add_edges(_get_edges)
    networkx_kg = in_memory_networkx_kg.get_kg()
    expected_edges = [
        (
            "p0",
            "p1",
            {
                "relationship_id": "prel0",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "p1",
            "p2",
            {
                "relationship_id": "prel1",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "p2",
            "p3",
            {
                "relationship_id": "prel2",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "p3",
            "p4",
            {
                "relationship_id": "prel3",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "m0",
            "p1",
            {
                "relationship_id": "mrel0",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
        (
            "m1",
            "p2",
            {
                "relationship_id": "mrel1",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
        (
            "m2",
            "p3",
            {
                "relationship_id": "mrel2",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
        (
            "m3",
            "p4",
            {
                "relationship_id": "mrel3",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
    ]
    assert list(networkx_kg.edges(data=True)) == expected_edges


@pytest.mark.parametrize("length", [4], scope="module")
def test_edges_gen(in_memory_networkx_kg, _get_edges):
    def edge_gen():
        for edge in _get_edges:
            yield edge

    in_memory_networkx_kg.add_edges(edge_gen())
    networkx_kg = in_memory_networkx_kg.get_kg()
    expected_edges = [
        (
            "p0",
            "p1",
            {
                "relationship_id": "prel0",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "p1",
            "p2",
            {
                "relationship_id": "prel1",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "p2",
            "p3",
            {
                "relationship_id": "prel2",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "p3",
            "p4",
            {
                "relationship_id": "prel3",
                "relationship_label": "PERTURBED_IN_DISEASE",
                "residue": "T253",
                "level": 4,
            },
        ),
        (
            "m0",
            "p1",
            {
                "relationship_id": "mrel0",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
        (
            "m1",
            "p2",
            {
                "relationship_id": "mrel1",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
        (
            "m2",
            "p3",
            {
                "relationship_id": "mrel2",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
        (
            "m3",
            "p4",
            {
                "relationship_id": "mrel3",
                "relationship_label": "Is_Mutated_In",
                "site": "3-UTR",
                "confidence": 1,
            },
        ),
    ]
    assert list(networkx_kg.edges(data=True)) == expected_edges


@pytest.mark.parametrize("length", [4], scope="module")
def test_rel_as_nodes(in_memory_networkx_kg, _get_rel_as_nodes):
    in_memory_networkx_kg.add_nodes(_get_rel_as_nodes)
    networkx_kg = in_memory_networkx_kg.get_kg()
    expected_edges = [
        (
            "i1",
            "p1",
            {"relationship_id": None, "relationship_label": "IS_SOURCE_OF"},
        ),
        (
            "i1",
            "p3",
            {"relationship_id": None, "relationship_label": "IS_TARGET_OF"},
        ),
        (
            "i2",
            "p2",
            {"relationship_id": None, "relationship_label": "IS_SOURCE_OF"},
        ),
        (
            "i2",
            "p4",
            {"relationship_id": None, "relationship_label": "IS_TARGET_OF"},
        ),
        (
            "i3",
            "p3",
            {"relationship_id": None, "relationship_label": "IS_SOURCE_OF"},
        ),
        (
            "i3",
            "p5",
            {"relationship_id": None, "relationship_label": "IS_TARGET_OF"},
        ),
        (
            "i4",
            "p4",
            {"relationship_id": None, "relationship_label": "IS_SOURCE_OF"},
        ),
        (
            "i0",
            "p2",
            {"relationship_id": None, "relationship_label": "IS_TARGET_OF"},
        ),
    ]
    assert list(networkx_kg.edges(data=True)) == expected_edges
