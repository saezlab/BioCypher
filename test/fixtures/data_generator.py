import pytest

from biocypher._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode


@pytest.fixture(scope="function")
def _get_nodes(length: int) -> list:
    nodes = []
    for i in range(length):
        bnp = BioCypherNode(
            node_id=f"p{i + 1}",
            node_label="protein",
            preferred_id="uniprot",
            properties={
                "score": 4 / (i + 1),
                "name": "StringProperty1",
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
            },
        )
        nodes.append(bnp)
        bnm = BioCypherNode(
            node_id=f"m{i + 1}",
            node_label="microRNA",
            preferred_id="mirbase",
            properties={
                "name": "StringProperty1",
                "taxon": 9606,
            },
        )
        nodes.append(bnm)

    return nodes


@pytest.fixture(scope="function")
def _get_nodes_non_compliant_names(length: int) -> list:
    nodes = []
    for i in range(length):
        bnp = BioCypherNode(
            node_id=f"p{i + 1}",
            node_label="Patient (person)",
            preferred_id="snomedct",
            properties={},
        )
        nodes.append(bnp)
        bnm = BioCypherNode(
            node_id=f"m{i + 1}",
            node_label="1$He524ll<o wor.ld <(",
            preferred_id="snomedct",
            properties={},
        )
        nodes.append(bnm)

    return nodes


@pytest.fixture(scope="function")
def _get_edges(length):
    edges = []
    for i in range(length):
        e1 = BioCypherEdge(
            relationship_id=f"prel{i}",
            source_id=f"p{i}",
            target_id=f"p{i + 1}",
            relationship_label="PERTURBED_IN_DISEASE",
            properties={
                "residue": "T253",
                "level": 4,
            },
            # we suppose the verb-form relationship label is created by
            # translation functionality in translate.py
        )
        edges.append(e1)
        e2 = BioCypherEdge(
            relationship_id=f"mrel{i}",
            source_id=f"m{i}",
            target_id=f"p{i + 1}",
            relationship_label="Is_Mutated_In",
            properties={
                "site": "3-UTR",
                "confidence": 1,
            },
            # we suppose the verb-form relationship label is created by
            # translation functionality in translate.py
        )
        edges.append(e2)
    return edges


@pytest.fixture(scope="function")
def _get_edges_non_compliant_names(length):
    edges = []
    for i in range(length):
        e1 = BioCypherEdge(
            relationship_id=f"prel{i}",
            source_id=f"p{i}",
            target_id=f"p{i + 1}",
            relationship_label="(Compliant) edge",
            properties={},
        )
        edges.append(e1)
        e2 = BioCypherEdge(
            relationship_id=f"mrel{i}",
            source_id=f"m{i}",
            target_id=f"p{i + 1}",
            relationship_label="42Is_Mutated_In",
            properties={
                "site": "3-UTR",
                "confidence": 1,
            },
            # we suppose the verb-form relationship label is created by
            # translation functionality in translate.py
        )
        edges.append(e2)
    return edges


@pytest.fixture(scope="function")
def _get_rel_as_nodes(length):
    rels = []
    for i in range(length):
        n = BioCypherNode(
            node_id=f"i{i + 1}",
            node_label="post translational interaction",
            properties={
                "directed": True,
                "effect": -1,
            },
        )
        e1 = BioCypherEdge(
            source_id=f"i{i + 1}",
            target_id=f"p{i + 1}",
            relationship_label="IS_SOURCE_OF",
        )
        e2 = BioCypherEdge(
            source_id=f"i{i}",
            target_id=f"p{i + 2}",
            relationship_label="IS_TARGET_OF",
        )
        rels.append(BioCypherRelAsNode(n, e1, e2))
    return rels
