import logging
import os

import networkx as nx
import pytest

from biocypher import BioCypher
from biocypher._ontology import Ontology, OntologyAdapter


def test_biolink_adapter(biolink_adapter):
    assert biolink_adapter.get_root_node() == "entity"
    assert biolink_adapter.get_nx_graph().number_of_nodes() > 100
    assert "biological entity" in biolink_adapter.get_ancestors("gene")


def test_so_adapter(so_adapter):
    assert so_adapter.get_root_node() == "sequence variant"
    assert "sequence variant" in so_adapter.get_ancestors("lethal variant")


def test_go_adapter(go_adapter):
    assert go_adapter.get_root_node() == "molecular function"
    assert "molecular function" in go_adapter.get_ancestors("rna helicase activity")


def test_mondo_adapter(mondo_adapter):
    assert mondo_adapter.get_root_node() == "disease"
    assert "human disease" in mondo_adapter.get_ancestors("cystic fibrosis")


def test_ontology_adapter_root_node_missing():
    with pytest.raises(ValueError):
        OntologyAdapter("test/ontologies/so.owl", "not_in_tree")


def test_ontology_functions(hybrid_ontology):
    assert isinstance(hybrid_ontology, Ontology)
    first_tail_ontology = hybrid_ontology._tail_ontologies.get("so").get_nx_graph()
    assert len(first_tail_ontology) == 6
    assert nx.is_directed_acyclic_graph(first_tail_ontology)
    # subgraph combination
    combined_length = len(hybrid_ontology._head_ontology.get_nx_graph())
    for adapter in hybrid_ontology._tail_ontologies.values():
        combined_length += len(adapter.get_nx_graph())
    # need to add 1 for the 'merge_nodes' = False case for mondo ontology
    combined_length += 1
    hybrid_length = len(hybrid_ontology._nx_graph)
    # subtract number of tail ontologies
    num_tail = len(hybrid_ontology._tail_ontologies)
    # subtract user extensions
    num_ext = len(hybrid_ontology._extended_nodes)
    assert hybrid_length - num_ext == combined_length - num_tail
    dgpl_ancestors = list(hybrid_ontology.get_ancestors("decreased gene product level"))
    assert "decreased gene product level" in dgpl_ancestors
    assert "altered gene product level" in dgpl_ancestors
    assert "functional effect variant" in dgpl_ancestors
    assert "sequence variant" in dgpl_ancestors
    assert "biological entity" in dgpl_ancestors
    assert "named thing" in dgpl_ancestors
    assert "entity" in dgpl_ancestors
    lethal_var = hybrid_ontology._nx_graph.nodes["lethal variant"]
    assert lethal_var["label"] == "SO_0001773"
    # second tail ontology: here we don't merge the nodes, but attach 'human
    # disease' as a child of 'disease'
    cf_ancestors = list(hybrid_ontology.get_ancestors("cystic fibrosis"))
    assert "cystic fibrosis" in cf_ancestors
    assert "autosomal recessive disease" in cf_ancestors
    assert "autosomal genetic disease" in cf_ancestors
    assert "hereditary disease" in cf_ancestors
    assert "human disease" in cf_ancestors
    assert "disease" in cf_ancestors
    assert "disease or phenotypic feature" in cf_ancestors
    assert "biological entity" in cf_ancestors
    assert "entity" in cf_ancestors
    # mixins?
    # user extensions
    dsdna_ancestors = list(hybrid_ontology.get_ancestors("dsDNA sequence"))
    assert "chemical entity" in dsdna_ancestors
    assert "association" in hybrid_ontology.get_ancestors("mutation to tissue association")
    # properties
    protein = hybrid_ontology._nx_graph.nodes["protein"]
    assert protein["label"] == "Protein"
    assert "taxon" in protein["properties"].keys()
    # synonyms
    assert "complex" in hybrid_ontology._nx_graph.nodes
    assert "macromolecular complex" not in hybrid_ontology._nx_graph.nodes


def test_show_ontology(hybrid_ontology):
    treevis = hybrid_ontology.show_ontology_structure()
    assert treevis is not None


def test_show_full_ontology(hybrid_ontology):
    treevis = hybrid_ontology.show_ontology_structure(full=True)
    assert treevis is not None


def test_write_ontology(hybrid_ontology, tmp_path):
    passed = hybrid_ontology.show_ontology_structure(to_disk=tmp_path)
    file_path = os.path.join(tmp_path, "ontology_structure.graphml")
    assert passed
    assert os.path.isfile(file_path)


def test_disconnected_exception(disconnected_mapping):
    with pytest.raises(ValueError):
        Ontology(
            head_ontology={
                "url": "test/ontologies/so.owl",
                "root_node": "sequence_variant",
            },
            ontology_mapping=disconnected_mapping,
        )


def test_manual_format():
    """
    Allow manual specification of ontology file format. Also test for allowing
    no schema config use.
    """
    ontology = Ontology(
        head_ontology={
            "url": "test/ontologies/sem.file",
            "root_node": "Core",
            "format": "rdf",
        },
        ontology_mapping=None,
    )
    assert isinstance(ontology._nx_graph, nx.DiGraph)
    assert "event" in ontology._nx_graph.nodes


@pytest.mark.parametrize(
    "ontology_file",
    [
        "test/ontologies/multiple_parent_nodes.ttl",
        "test/ontologies/multiple_parent_nodes.owl",
    ],
)
def test_multiple_parents(ontology_file):
    ontology_adapter = OntologyAdapter(ontology_file=ontology_file, root_label="Root")
    result = ontology_adapter.get_nx_graph()
    # Expected hierarchy:
    # root
    # ├── level1A
    # │   ├── level2A
    # │   │   └── child
    # │   └── level2B
    # │       └── child
    # └── level1B
    #     └── level2C
    #         └── child
    expected_edges = [
        ("level1A", "root"),
        ("level2A", "level1A"),
        ("level1B", "root"),
        ("level2C", "level1B"),
        ("child", "level2A"),
        ("child", "level2B"),
        ("child", "level2C"),
        ("level2B", "level1A"),
    ]
    for edge in expected_edges:
        assert edge in result.edges


def test_missing_label_on_node():
    ontology_adapter = OntologyAdapter(
        ontology_file="test/ontologies/missing_label.ttl",
        root_label="Test_Missing_Label_Root",
    )
    result = ontology_adapter.get_nx_graph()
    # Expected hierarchy:
    #  test missing label root
    #  ├── test missing label level1a
    # (└── level1B) <- missing label on this node (should not be part of the graph)
    expected_edges = [("test missing label level1a", "test missing label root")]
    for edge in expected_edges:
        assert edge in result.edges
    assert len(result.edges) == len(expected_edges)


def test_switch_id_and_label():
    ontology_adapter_reversed = OntologyAdapter(
        ontology_file="test/ontologies/reverse_labels.ttl",
        root_label="Label_Root",
        switch_label_and_id=True,
    )

    expected_switched = ["label level1b", "label root", "label level1a"]
    for node in ontology_adapter_reversed.get_nx_graph().nodes:
        assert node in expected_switched


def test_do_not_switch_id_and_label():
    ontology_adapter = OntologyAdapter(
        ontology_file="test/ontologies/reverse_labels.ttl",
        root_label="Label_Root",
        switch_label_and_id=False,
    )

    expected_not_switched = ["ID_0", "ID_1", "ID_2"]
    for node in ontology_adapter.get_nx_graph().nodes:
        assert node in expected_not_switched


def test_root_node_not_found():
    with pytest.raises(ValueError) as error_message:
        OntologyAdapter(
            ontology_file="test/ontologies/reverse_labels.ttl",
            root_label="not present",
        )
    error_message = str(error_message.value)
    assert "Could not find root node with label 'not present'." in error_message
    assert (
        "The ontology contains the following labels: ['Label_Root', 'Label_Level1A', 'Label_Level1B']" in error_message
    )


def test_switch_id_and_label_from_yaml_config():
    bc = BioCypher(
        head_ontology={
            "url": "test/ontologies/reverse_labels.ttl",
            "root_node": "Label_Root",
            "switch_label_and_id": True,
        },
        tail_ontologies={
            "tail": {
                "url": "test/ontologies/missing_label.ttl",
                "head_join_node": "Label_Level1A",
                "tail_join_node": "Test_Missing_Label_Root",
                "switch_label_and_id": True,
            }
        },
    )
    expected_not_switched = [
        "label level1b",
        "label root",
        "label level1a",
        "test missing label level1a",
    ]
    for node in bc._get_ontology()._nx_graph.nodes:
        assert node in expected_not_switched


def test_mixed_switch_id_and_label_from_yaml_config():
    bc = BioCypher(
        head_ontology={
            "url": "test/ontologies/reverse_labels.ttl",
            "root_node": "Label_Root",
            "switch_label_and_id": True,
        },
        tail_ontologies={
            "tail": {
                "url": "test/ontologies/missing_label.ttl",
                "head_join_node": "Label_Level1A",
                "tail_join_node": "Test_Missing_Label_Root",
                "switch_label_and_id": False,
            }
        },
    )
    expected_not_switched = [
        "label level1b",
        "label root",
        "label level1a",
        "ID_1A",
    ]
    for node in bc._get_ontology()._nx_graph.nodes:
        assert node in expected_not_switched


def test_do_not_switch_id_and_label_from_yaml_config():
    bc = BioCypher(
        head_ontology={
            "url": "test/ontologies/reverse_labels.ttl",
            "root_node": "Label_Root",
            "switch_label_and_id": False,
        },
        tail_ontologies={
            "tail": {
                "url": "test/ontologies/missing_label.ttl",
                "head_join_node": "Label_Level1A",
                "tail_join_node": "Test_Missing_Label_Root",
                "switch_label_and_id": False,
            }
        },
    )
    expected_not_switched = ["ID_0", "ID_1", "ID_2", "ID_1A"]
    for node in bc._get_ontology()._nx_graph.nodes:
        assert node in expected_not_switched


def test_head_join_node_not_found():
    bc = BioCypher(
        head_ontology={
            "url": "test/ontologies/reverse_labels.ttl",
            "root_node": "Label_Root",
        },
        tail_ontologies={
            "tail": {
                "url": "test/ontologies/missing_label.ttl",
                "head_join_node": "not present",
                "tail_join_node": "Test_Missing_Label_Root",
            }
        },
    )
    with pytest.raises(ValueError) as error_message:
        bc._get_ontology()
    error_message = str(error_message.value)
    assert "Head join node 'not present' not found in head ontology." in error_message
    assert "The head ontology contains the following" in error_message
    assert "Label_Level1A" in error_message
    assert "Label_Root" in error_message
    assert "Label_Level1B" in error_message


def test_simple_ontology(simple_ontology):
    assert list(simple_ontology.get_ancestors("accuracy")) == [
        "accuracy",
        "entity",
        "thing",
    ]


def test_duplicated_tail_ontologies(caplog, extended_ontology_mapping):
    ontology = Ontology(
        head_ontology={
            "url": "https://github.com/biolink/biolink-model/raw/v3.2.1/biolink-model.owl.ttl",
            "root_node": "entity",
        },
        ontology_mapping=extended_ontology_mapping,
        tail_ontologies={
            "so": {
                "url": "test/ontologies/so.owl",
                "head_join_node": "sequence variant",
                "tail_join_node": "sequence_variant",
            },
            "so_2": {
                "url": "test/ontologies/so.owl",
                "head_join_node": "device",
                "tail_join_node": "sequence_variant",
            },
            "mondo": {
                "url": "test/ontologies/mondo.owl",
                "head_join_node": "disease",
                "tail_join_node": "human disease",
            },
        },
    )
    assert ontology
    with caplog.at_level(logging.INFO):
        tree = ontology.show_ontology_structure()
    assert tree
    assert any("The ontology contains multiple inheritance" in record.message for record in caplog.records)
