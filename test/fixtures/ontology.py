import pytest

from biocypher._mapping import OntologyMapping
from biocypher._ontology import Ontology, OntologyAdapter


@pytest.fixture(scope="module")
def ontology_mapping():
    return OntologyMapping(config_file="biocypher/_config/test_schema_config.yaml")


@pytest.fixture(scope="module")
def simple_ontology_mapping():
    m = OntologyMapping()
    m.schema = {
        "accuracy": {},
    }
    return m


@pytest.fixture(scope="module")
def extended_ontology_mapping():
    return OntologyMapping(config_file="biocypher/_config/test_schema_config_extended.yaml")


@pytest.fixture(scope="module")
def disconnected_mapping():
    return OntologyMapping(config_file="biocypher/_config/test_schema_config_disconnected.yaml")


@pytest.fixture(scope="module")
def hybrid_ontology(extended_ontology_mapping):
    return Ontology(
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
            "mondo": {
                "url": "test/ontologies/mondo.owl",
                "head_join_node": "disease",
                "tail_join_node": "human disease",
                "merge_nodes": False,
            },
        },
    )


@pytest.fixture(scope="module")
def simple_ontology(simple_ontology_mapping):
    return Ontology(
        head_ontology={
            "url": "test/ontologies/ontology1.ttl",
            "root_node": "Thing",
        },
        ontology_mapping=simple_ontology_mapping,
        tail_ontologies={
            "test": {
                "url": "test/ontologies/ontology2.ttl",
                "head_join_node": "entity",
                "tail_join_node": "EvaluationCriterion",
            },
        },
    )


@pytest.fixture(scope="module")
def biolink_adapter():
    return OntologyAdapter(
        "https://github.com/biolink/biolink-model/raw/v3.2.1/biolink-model.owl.ttl",
        "entity",
    )


@pytest.fixture(scope="module")
def so_adapter():
    return OntologyAdapter("test/ontologies/so.owl", "sequence_variant")


@pytest.fixture(scope="module")
def go_adapter():
    return OntologyAdapter("test/ontologies/go.owl", "molecular_function")


@pytest.fixture(scope="module")
def mondo_adapter():
    return OntologyAdapter("test/ontologies/mondo.owl", "disease")
