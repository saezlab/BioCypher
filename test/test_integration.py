import json
import os

import networkx as nx
import pytest

import pandas as pd

from biocypher._ontology import Ontology


@pytest.mark.parametrize("length", [4], scope="function")
def test_write_node_data_from_gen(core, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = core.write_nodes(node_gen(nodes))
    assert passed

    path = core._output_directory

    protein_csv = os.path.join(path, "Protein-part000.csv")
    micro_rna_csv = os.path.join(path, "MicroRNA-part000.csv")

    with open(protein_csv) as f:
        protein_data = f.read()

    with open(micro_rna_csv) as f:
        micro_rna_data = f.read()

    assert passed
    assert "p1;'StringProperty1';4.0;9606;'gene1|gene2';'p1';'uniprot'" in protein_data
    assert "BiologicalEntity" in protein_data
    assert "m1;'StringProperty1';9606;'m1';'mirbase'" in micro_rna_data
    assert "ChemicalEntity" in micro_rna_data


def test_show_ontology_structure_kwargs(core):
    treevis = core.show_ontology_structure(full=True)

    assert treevis is not None


def test_ontology_without_schema_config(core_no_schema):
    assert core_no_schema

    core_no_schema._head_ontology = {
        "url": "test/ontologies/sem.file",  # any file suffix
        "root_node": "Core",
        "format": "rdf",
    }
    core_no_schema._ontology_mapping = None

    core_no_schema._get_ontology()

    assert isinstance(core_no_schema._ontology, Ontology)
    assert isinstance(core_no_schema._ontology._nx_graph, nx.DiGraph)


@pytest.mark.parametrize("length", [4], scope="function")
def test_write_schema_info_as_node(core, _get_nodes):
    core.write_nodes(_get_nodes)

    schema = core.write_schema_info(as_node=True)

    header_path = os.path.join(core._output_directory, "Schema_info-header.csv")
    assert os.path.exists(header_path)
    schema_path = os.path.join(core._output_directory, "Schema_info-part000.csv")
    assert os.path.exists(schema_path)

    with open(header_path, "r") as f:
        schema_header = f.read()

    assert "schema_info" in schema_header

    # read schema_path with pandas
    schema_df = pd.read_csv(schema_path, sep=";", header=None)

    # get the second column of the first row and decode from json dumps format
    string = schema_df.iloc[0, 1]
    # fix initial and end quotes
    string = string[1:-1]
    schema_part = json.loads(string)

    assert schema_part == schema

    # test import call
    import_call_path = os.path.join(core._output_directory, "neo4j-admin-import-call.sh")
    assert os.path.exists(import_call_path)
    with open(import_call_path, "r") as f:
        import_call = f.read()

    assert "Schema_info" in import_call
