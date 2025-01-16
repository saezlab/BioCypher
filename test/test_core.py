import os

import pytest
import yaml

from biocypher import BioCypher
from biocypher.output.in_memory._get_in_memory_kg import IN_MEMORY_DBMS
from biocypher.output.write._get_writer import DBMS_TO_CLASS


def test_biocypher(core):
    assert core._dbms == "neo4j"
    assert core._offline
    assert not core._strict_mode


def test_log_missing_types(core, translator):
    core._translator = translator
    core._translator.notype = {}
    assert core.log_missing_input_labels() is None

    core._translator.notype = {"a": 1, "b": 2}
    real_missing_types = core.log_missing_input_labels()

    assert real_missing_types.get("a") == 1 and real_missing_types.get("b") == 2


@pytest.mark.parametrize("length", [4], scope="function")
def test_log_duplicates(core, deduplicator, _get_nodes):
    core._deduplicator = deduplicator
    nodes = _get_nodes + _get_nodes

    core.add(nodes)
    core.log_duplicates()

    assert "protein" in core._deduplicator.duplicate_entity_types
    assert "p1" in core._deduplicator.duplicate_entity_ids
    assert "microRNA" in core._deduplicator.duplicate_entity_types
    assert "m1" in core._deduplicator.duplicate_entity_ids


@pytest.mark.parametrize("length", [4], scope="function")
def test_write_schema_info(core, _get_nodes, _get_edges, _get_rel_as_nodes):
    core._offline = False
    core._dbms = "csv"
    core.add(_get_nodes)
    core.add(_get_edges)
    core.add(_get_rel_as_nodes)

    schema = core.write_schema_info()

    assert schema.get("is_schema_info")
    assert schema.get("protein").get("present_in_knowledge_graph")
    assert not schema.get("protein").get("is_relationship")
    assert schema.get("microRNA").get("present_in_knowledge_graph")
    assert not schema.get("microRNA").get("is_relationship")
    assert schema.get("gene to disease association").get("present_in_knowledge_graph")

    assert schema.get("gene to disease association").get("is_relationship")
    assert schema.get("mutation to tissue association").get("present_in_knowledge_graph")
    assert schema.get("mutation to tissue association").get("is_relationship")
    assert schema.get("post translational interaction").get("present_in_knowledge_graph")
    assert schema.get("post translational interaction").get("is_relationship")

    path = os.path.join(core._output_directory, "schema_info.yaml")
    assert os.path.exists(path)

    with open(path, "r") as f:
        schema_loaded = yaml.safe_load(f)

    assert schema_loaded == schema


def test_show_full_ontology_structure_without_schema():
    bc = BioCypher(
        head_ontology={
            "url": "test/ontologies/so.owl",
            "root_node": "sequence_variant",
        }
    )
    treevis = bc.show_ontology_structure(full=True)

    assert "sequence variant" in treevis
    assert "functional effect variant" in treevis
    assert "altered gene product level" in treevis
    assert "decreased gene product level" in treevis
    assert "functionally abnormal" in treevis
    assert "lethal variant" in treevis


def test_in_memory_kg_only_in_online_mode(core):
    for in_memory_dbms in IN_MEMORY_DBMS:
        core._dbms = in_memory_dbms
        core._offline = True
        with pytest.raises(ValueError) as e:
            core.get_kg()
        assert "Getting the in-memory KG is only available in online mode for " in str(e.value)


def test_no_in_memory_kg_for_dbms(core):
    for dbms in ["neo4j", "arangodb", "rdf"]:
        core._dbms = dbms
        core._offline = False
        with pytest.raises(ValueError) as e:
            core.get_kg()
        assert "Getting the in-memory KG is only available in online mode for " in str(e.value)


def test_no_in_memory_instance_found(core):
    core._dbms = "csv"
    core._offline = False

    with pytest.raises(ValueError) as e:
        core.get_kg()
    assert str(e.value) == "No in-memory KG instance found. Please call `add()` first."


def test_online_mode_not_supported_for_dbms(core):
    for dbms in ["arangodb", "rdf", "postgres", "sqlite3"]:
        core._dbms = dbms
        core._offline = False
        with pytest.raises(NotImplementedError) as e:
            core._get_driver()
        assert str(e.value) == f"Online mode is not supported for the DBMS {dbms}."


def test_get_driver_in_offline_mode(core):
    for dbms in ["neo4j"]:
        core._dbms = dbms
        core._offline = True
        with pytest.raises(NotImplementedError) as e:
            core._get_driver()
        assert str(e.value) == "Cannot get driver in offline mode."


def test_get_writer_in_online_mode(core):
    for dbms in DBMS_TO_CLASS.keys():
        core._dbms = dbms
        core._offline = False
        with pytest.raises(NotImplementedError) as e:
            core._get_writer()
        assert str(e.value) == "Cannot get writer in online mode."
