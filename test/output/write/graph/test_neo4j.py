import logging
import os

import pytest

from genericpath import isfile

from biocypher._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from biocypher.output.write._batch_writer import parse_label
from biocypher.output.write.graph._neo4j import _Neo4jBatchWriter


def test_neo4j_writer_and_output_dir(bw):
    tmp_path = bw.outdir

    assert os.path.isdir(tmp_path) and isinstance(bw, _Neo4jBatchWriter) and bw.delim == ";"


def test_create_import_call(bw):
    mixed = []
    number_of_items = 4
    for i in range(number_of_items):
        node = BioCypherNode(
            f"i{i + 1}",
            "post translational interaction",
        )
        edge_1 = BioCypherEdge(
            source_id=f"i{i + 1}",
            target_id=f"p{i + 1}",
            relationship_label="IS_SOURCE_OF",
        )
        edge_2 = BioCypherEdge(
            source_id=f"i{i}",
            target_id=f"p{i + 2}",
            relationship_label="IS_TARGET_OF",
        )
        mixed.append(BioCypherRelAsNode(node, edge_1, edge_2))

        edge_3 = BioCypherEdge(
            source_id=f"p{i + 1}",
            target_id=f"p{i + 1}",
            relationship_label="PERTURBED_IN_DISEASE",
        )
        mixed.append(edge_3)

    def gen(lis):
        yield from lis

    passed = bw.write_edges(gen(mixed))

    call = bw.get_import_call()

    tmp_path = bw.outdir

    assert passed
    assert "neo4j-admin" in call
    assert "import" in call
    assert '--delimiter=";"' in call
    assert '--array-delimiter="|" --quote="\'"' in call
    assert (
        f'--nodes="{tmp_path}{os.sep}PostTranslationalInteraction-header.csv,'
        f'{tmp_path}{os.sep}PostTranslationalInteraction-part.*" '
    ) in call
    assert (
        f'--relationships="{tmp_path}{os.sep}IS_SOURCE_OF-header.csv,{tmp_path}{os.sep}IS_SOURCE_OF-part.*" '
    ) in call
    assert (
        f'--relationships="{tmp_path}{os.sep}IS_TARGET_OF-header.csv,{tmp_path}{os.sep}IS_TARGET_OF-part.*" '
    ) in call
    assert (
        f'--relationships="{tmp_path}{os.sep}PERTURBED_IN_DISEASE-header.csv,'
        f'{tmp_path}{os.sep}PERTURBED_IN_DISEASE-part.*" '
    ) in call


@pytest.mark.parametrize("length", [4], scope="module")
def test_neo4j_write_node_data_headers_import_call(bw, _get_nodes):
    # four proteins, four miRNAs
    nodes = _get_nodes

    passed = bw.write_nodes(nodes[:4])
    passed = bw.write_nodes(nodes[4:])
    bw.write_import_call()

    assert passed

    tmp_path = bw.outdir

    protein_header_csv = os.path.join(tmp_path, "Protein-header.csv")
    micro_rna_header_csv = os.path.join(tmp_path, "MicroRNA-header.csv")
    import_call_path = os.path.join(tmp_path, "neo4j-admin-import-call.sh")

    with open(protein_header_csv) as f:
        protein_header = f.read()
    with open(micro_rna_header_csv) as f:
        micro_rna_header = f.read()
    with open(import_call_path) as f:
        call = f.read()

    assert protein_header == ":ID;name;score:double;taxon:long;genes:string[];id;preferred_id;:LABEL"
    assert micro_rna_header == ":ID;name;taxon:long;id;preferred_id;:LABEL"
    assert "neo4j-admin" in call
    assert "import" in call
    assert '--delimiter=";"' in call
    assert '--nodes="' in call
    assert "Protein-header.csv" in call
    assert 'Protein-part.*"' in call
    assert "MicroRNA-header.csv" in call
    assert 'MicroRNA-part.*"' in call

    # custom import call executable path
    bw.import_call_bin_prefix = "custom/path/"

    os.remove(import_call_path)
    bw.write_import_call()

    with open(import_call_path) as f:
        call = f.read()

    assert "custom/path/neo4j-admin" in call
    assert "import" in call

    # custom file prefix
    # TODO


def test_construct_import_call(bw):
    assert isinstance(bw, _Neo4jBatchWriter)

    import_script = bw._construct_import_call()

    assert "--overwrite-destination=true" in import_script
    assert "bin/neo4j-admin import" in import_script
    assert "--force=true" in import_script
    assert "--database=neo4j" in import_script
    assert "neo4j" in import_script
    assert "bin/neo4j-admin database import full neo4j" in import_script
    assert "bin/neo4j-admin import --database=neo4j" in import_script


def test_write_hybrid_ontology_nodes(bw):
    nodes = []
    for i in range(4):
        nodes.append(
            BioCypherNode(
                node_id=f"agpl:000{i}",
                node_label="altered gene product level",
                properties={},
            ),
        )

    passed = bw.write_nodes(nodes)

    assert passed

    tmp_path = bw.outdir

    header_csv = os.path.join(tmp_path, "AlteredGeneProductLevel-header.csv")
    data_csv = os.path.join(tmp_path, "AlteredGeneProductLevel-part000.csv")

    with open(header_csv) as f:
        header = f.read()

    with open(data_csv) as f:
        part = f.read()

    assert header == ":ID;id;preferred_id;:LABEL"
    assert "agpl:0000;'agpl:0000';'id'" in part
    assert "AlteredGeneProductLevel" in part
    assert "BiologicalEntity" in part


def test_property_types(bw):
    nodes = []
    for i in range(4):
        biocypher_node_protein = BioCypherNode(
            node_id=f"p{i + 1}",
            node_label="protein",
            properties={
                "score": 4 / (i + 1),
                "name": "StringProperty1",
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
            },
        )
        nodes.append(biocypher_node_protein)

    passed = bw.write_nodes(nodes, batch_size=1e6)

    tmp_path = bw.outdir

    data_csv = os.path.join(tmp_path, "Protein-part000.csv")
    header_csv = os.path.join(tmp_path, "Protein-header.csv")

    with open(data_csv) as f:
        data = f.read()

    with open(header_csv) as f:
        header = f.read()

    assert passed
    assert header == ":ID;name;score:double;taxon:long;genes:string[];id;preferred_id;:LABEL"
    assert "p1;'StringProperty1';4.0;9606;'gene1|gene2';'p1';'id'" in data
    assert "BiologicalEntity" in data


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_node_data_from_list(bw, _get_nodes):
    nodes = _get_nodes

    passed = bw._write_node_data(nodes, batch_size=1e6)

    tmp_path = bw.outdir

    protein_csv = os.path.join(tmp_path, "Protein-part000.csv")
    micro_rna_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")

    with open(protein_csv) as f:
        protein = f.read()

    with open(micro_rna_csv) as f:
        micro_rna = f.read()

    assert passed
    assert "p1;'StringProperty1';4.0;9606;'gene1|gene2';'p1';'uniprot'" in protein
    assert "BiologicalEntity" in protein
    assert "m1;'StringProperty1';9606;'m1';'mirbase'" in micro_rna
    assert "ChemicalEntity" in micro_rna


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_node_data_from_list_not_compliant_names(monkeypatch, caplog, bw, _get_nodes_non_compliant_names):
    nodes = _get_nodes_non_compliant_names

    def mock_get_ancestors(self, label):
        return ["First level", label]

    monkeypatch.setattr("biocypher._ontology.Ontology.get_ancestors", mock_get_ancestors)

    with caplog.at_level(logging.INFO):
        passed = bw._write_node_data(nodes, batch_size=1e6)
    tmp_path = bw.outdir

    expected_file_names = [
        "PatientPerson-part000.csv",
        "$He524lloWor.Ld-part000.csv",
    ]
    for file_name in os.listdir(tmp_path):
        assert file_name in expected_file_names
        assert any("Label is not compliant with Neo4j naming rules" in record.message for record in caplog.records)
    assert any(
        "Label does not start with an alphabetic character or with $" in record.message for record in caplog.records
    )
    assert passed


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_node_data_from_gen(bw, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = bw._write_node_data(node_gen(nodes), batch_size=1e6)

    tmp_path = bw.outdir

    protein_csv = os.path.join(tmp_path, "Protein-part000.csv")
    micro_rna_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")

    with open(protein_csv) as f:
        protein = f.read()

    with open(micro_rna_csv) as f:
        micro_rna = f.read()

    assert passed
    assert "p1;'StringProperty1';4.0;9606;'gene1|gene2';'p1';'uniprot'" in protein
    assert "BiologicalEntity" in protein
    assert "m1;'StringProperty1';9606;'m1';'mirbase'" in micro_rna
    assert "ChemicalEntity" in micro_rna


def test_write_node_data_from_gen_no_props(bw):
    nodes = []
    number_of_items = 4
    for i in range(number_of_items):
        biocypher_node_protein = BioCypherNode(
            node_id=f"p{i + 1}",
            node_label="protein",
            properties={
                "score": 4 / (i + 1),
                "name": "StringProperty1",
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
            },
        )
        nodes.append(biocypher_node_protein)
        biocypher_node_micro_rna = BioCypherNode(
            node_id=f"m{i + 1}",
            node_label="microRNA",
        )
        nodes.append(biocypher_node_micro_rna)

    def node_gen(nodes):
        yield from nodes

    passed = bw._write_node_data(node_gen(nodes), batch_size=1e6)

    tmp_path = bw.outdir
    assert os.path.exists(tmp_path)

    protein_csv = os.path.join(tmp_path, "Protein-part000.csv")
    micro_rna_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")

    with open(protein_csv) as f:
        protein = f.read()

    with open(micro_rna_csv) as f:
        micro_rna = f.read()

    assert passed
    assert "p1;'StringProperty1';4.0;9606;'gene1|gene2';'p1';'id'" in protein
    assert "BiologicalEntity" in protein
    assert "m1;'m1';'id'" in micro_rna
    assert "ChemicalEntity" in micro_rna


@pytest.mark.parametrize("length", [int(1e4 + 4)], scope="module")
def test_write_node_data_from_large_gen(bw, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = bw._write_node_data(
        node_gen(nodes),
        batch_size=int(1e4),
    )  # reduce test time

    tmp_path = bw.outdir

    protein_0_csv = os.path.join(tmp_path, "Protein-part000.csv")
    micro_rna_0_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")
    protein_1_csv = os.path.join(tmp_path, "Protein-part001.csv")
    micro_rna_1_csv = os.path.join(tmp_path, "MicroRNA-part001.csv")

    protein_lines = sum(1 for _ in open(protein_0_csv))
    micro_rna_lines = sum(1 for _ in open(micro_rna_0_csv))
    protein_lines1 = sum(1 for _ in open(protein_1_csv))
    micro_rna_lines1 = sum(1 for _ in open(micro_rna_1_csv))

    assert passed and protein_lines == 1e4 and micro_rna_lines == 1e4 and protein_lines1 == 4 and micro_rna_lines1 == 4


@pytest.mark.parametrize("length", [1], scope="module")
def test_too_many_properties(bw, _get_nodes):
    nodes = _get_nodes

    biocypher_node_1 = BioCypherNode(
        node_id="p0",
        node_label="protein",
        properties={
            "p1": "StringProperty1",
            "p2": "StringProperty2",
            "p3": "StringProperty3",
            "p4": "StringProperty4",
        },
    )
    nodes.append(biocypher_node_1)

    def node_gen(nodes):
        yield from nodes

    passed = bw._write_node_data(
        node_gen(nodes),
        batch_size=int(1e4),
    )  # reduce test time

    assert not passed


@pytest.mark.parametrize("length", [1], scope="module")
def test_not_enough_properties(bw, _get_nodes):
    nodes = _get_nodes

    biocypher_node_1 = BioCypherNode(
        node_id="p0",
        node_label="protein",
        properties={"p1": "StringProperty1"},
    )
    nodes.append(biocypher_node_1)

    def node_gen(nodes):
        yield from nodes

    passed = bw._write_node_data(
        node_gen(nodes),
        batch_size=int(1e4),
    )  # reduce test time

    tmp_path = bw.outdir

    protein_0_csv = os.path.join(tmp_path, "Protein-part000.csv")

    assert not passed and not isfile(protein_0_csv)


def test_write_none_type_property_and_order_invariance(bw):
    # as introduced by translation using defined properties in
    # schema_config.yaml
    nodes = []

    biocypher_node_protein_1 = BioCypherNode(
        node_id="p1",
        node_label="protein",
        properties={
            "taxon": 9606,
            "score": 1,
            "name": None,
            "genes": None,
        },
    )
    biocypher_node_protein_2 = BioCypherNode(
        node_id="p2",
        node_label="protein",
        properties={
            "name": None,
            "genes": ["gene1", "gene2"],
            "score": 2,
            "taxon": 9606,
        },
    )
    biocypher_node_micro_rna = BioCypherNode(
        node_id="m1",
        node_label="microRNA",
        properties={
            "name": None,
            "taxon": 9606,
        },
    )
    nodes.append(biocypher_node_protein_1)
    nodes.append(biocypher_node_protein_2)
    nodes.append(biocypher_node_micro_rna)

    def node_gen(nodes):
        yield from nodes

    passed = bw._write_node_data(
        node_gen(nodes),
        batch_size=int(1e4),
    )  # reduce test time

    tmp_path = bw.outdir

    protein_0_csv = os.path.join(tmp_path, "Protein-part000.csv")
    with open(protein_0_csv) as f:
        protein = f.read()

    assert passed
    assert "p1;;1;9606;;'p1';'id'" in protein
    assert "BiologicalEntity" in protein


@pytest.mark.parametrize("length", [int(1e4)], scope="module")
def test_accidental_exact_batch_size(bw, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = bw.write_nodes(
        node_gen(nodes),
        batch_size=int(1e4),
    )  # reduce test time

    tmp_path = bw.outdir

    protein_0_csv = os.path.join(tmp_path, "Protein-part000.csv")
    micro_rna_0_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")
    protein_1_csv = os.path.join(tmp_path, "Protein-part001.csv")
    micro_rna_1_csv = os.path.join(tmp_path, "MicroRNA-part001.csv")

    protein_lines = sum(1 for _ in open(protein_0_csv))
    micro_rna_lines = sum(1 for _ in open(micro_rna_0_csv))

    protein_header_csv = os.path.join(tmp_path, "Protein-header.csv")
    micro_rna_header_csv = os.path.join(tmp_path, "MicroRNA-header.csv")

    with open(protein_header_csv) as f:
        protein = f.read()
    with open(micro_rna_header_csv) as f:
        micro_rna = f.read()

    assert (
        passed
        and protein_lines == 1e4
        and micro_rna_lines == 1e4
        and not isfile(protein_1_csv)
        and not isfile(micro_rna_1_csv)
        and protein == ":ID;name;score:double;taxon:long;genes:string[];id;preferred_id;:LABEL"
        and micro_rna == ":ID;name;taxon:long;id;preferred_id;:LABEL"
    )


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_edge_data_from_gen(bw, _get_edges):
    edges = _get_edges

    def edge_gen(edges):
        yield from edges

    passed = bw._write_edge_data(edge_gen(edges), batch_size=int(1e4))

    tmp_path = bw.outdir

    pid_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part000.csv")
    imi_csv = os.path.join(tmp_path, "Is_Mutated_In-part000.csv")

    with open(pid_csv) as f:
        perturbed_in_disease = f.read()
    with open(imi_csv) as f:
        is_mutated_in = f.read()

    assert passed
    assert "p0;" in perturbed_in_disease
    assert "prel0;" in perturbed_in_disease
    assert "'T253';" in perturbed_in_disease
    assert "4;" in perturbed_in_disease
    assert "p1;" in perturbed_in_disease
    assert "PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "p1;" in perturbed_in_disease
    assert "prel1;" in perturbed_in_disease
    assert "'T253';" in perturbed_in_disease
    assert "4;" in perturbed_in_disease
    assert "p2;" in perturbed_in_disease
    assert "PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "\n" in perturbed_in_disease
    assert "m0;" in is_mutated_in
    assert "mrel0;" in is_mutated_in
    assert "'3-UTR';" in is_mutated_in
    assert "1;" in is_mutated_in
    assert "p1;" in is_mutated_in
    assert "Is_Mutated_In" in is_mutated_in
    assert "m1;" in is_mutated_in
    assert "mrel1;" in is_mutated_in
    assert "'3-UTR';" in is_mutated_in
    assert "1;" in is_mutated_in
    assert "p2;" in is_mutated_in
    assert "Is_Mutated_In" in is_mutated_in
    assert "\n" in is_mutated_in


@pytest.mark.parametrize("length", [int(1e4 + 4)], scope="module")
def test_write_edge_data_from_large_gen(bw, _get_edges):
    edges = _get_edges

    def edge_gen(edges):
        yield from edges

    passed = bw._write_edge_data(edge_gen(edges), batch_size=int(1e4))

    tmp_path = bw.outdir

    perturbed_in_disease_data_0_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part000.csv")
    is_mutated_in_0_csv = os.path.join(tmp_path, "Is_Mutated_In-part000.csv")
    perturbed_in_disease_data_1_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part001.csv")
    is_mutated_in_1_csv = os.path.join(tmp_path, "Is_Mutated_In-part001.csv")

    perturbed_in_disease_data_0 = sum(1 for _ in open(perturbed_in_disease_data_0_csv))
    is_mutated_in_0 = sum(1 for _ in open(is_mutated_in_0_csv))
    perturbed_in_disease_data_1 = sum(1 for _ in open(perturbed_in_disease_data_1_csv))
    is_mutated_in_1 = sum(1 for _ in open(is_mutated_in_1_csv))

    assert (
        passed
        and perturbed_in_disease_data_0 == 1e4
        and is_mutated_in_0 == 1e4
        and perturbed_in_disease_data_1 == 4
        and is_mutated_in_1 == 4
    )


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_edge_data_from_list(bw, _get_edges):
    edges = _get_edges

    passed = bw._write_edge_data(edges, batch_size=int(1e4))

    tmp_path = bw.outdir

    perturbed_in_disease_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part000.csv")
    is_mutated_in_csv = os.path.join(tmp_path, "Is_Mutated_In-part000.csv")

    with open(perturbed_in_disease_csv) as f:
        perturbed_in_disease = f.read()
    with open(is_mutated_in_csv) as f:
        is_mutated_in = f.read()

    assert passed
    assert "p0;" in perturbed_in_disease
    assert "prel0;" in perturbed_in_disease
    assert "'T253';" in perturbed_in_disease
    assert "4;" in perturbed_in_disease
    assert "p1;" in perturbed_in_disease
    assert "PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "\n" in perturbed_in_disease
    assert "p2;PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "m0;" in is_mutated_in
    assert "mrel0;" in is_mutated_in
    assert "'3-UTR';" in is_mutated_in
    assert "1;" in is_mutated_in
    assert "p1;" in is_mutated_in
    assert "Is_Mutated_In" in is_mutated_in
    assert "m1;" in is_mutated_in
    assert "\n" in is_mutated_in


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_edge_data_from_list_non_compliant_names(monkeypatch, caplog, bw, _get_edges_non_compliant_names):
    edges = _get_edges_non_compliant_names

    def mock_get_ancestors(self, label):
        return ["First level", label]

    monkeypatch.setattr("biocypher._ontology.Ontology.get_ancestors", mock_get_ancestors)

    with caplog.at_level(logging.INFO):
        passed = bw._write_edge_data(edges, batch_size=int(1e4))
    tmp_path = bw.outdir

    expected_file_names = [
        "Is_Mutated_In-part000.csv",
        "CompliantEdge-part000.csv",
    ]
    for file_name in os.listdir(tmp_path):
        assert file_name in expected_file_names
    assert any("Label is not compliant with Neo4j naming rules" in record.message for record in caplog.records)
    assert any(
        "Label does not start with an alphabetic character or with $" in record.message for record in caplog.records
    )
    assert passed


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_edge_id_optional(bw, _get_edges):
    edges = _get_edges

    # add phosphorylation edges
    for i in range(4):
        edge_1 = BioCypherEdge(
            relationship_id=f"phos{i}",  # should be ignored
            source_id=f"p{i}",
            target_id=f"p{i + 1}",
            relationship_label="phosphorylation",
        )
        edges.append(edge_1)

    passed = bw.write_edges(edges, batch_size=int(1e4))
    assert passed

    tmp_path = bw.outdir

    perturbed_in_disease_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part000.csv")
    phosphorylation_csv = os.path.join(tmp_path, "Phosphorylation-part000.csv")

    with open(perturbed_in_disease_csv) as f:
        perturbed_in_disease = f.read()
    with open(phosphorylation_csv) as f:
        phosphorylation = f.read()

    assert "prel0;" in perturbed_in_disease
    assert "phos1;" not in phosphorylation

    perturbed_in_disease_header = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-header.csv")
    phosphorylation_header = os.path.join(tmp_path, "Phosphorylation-header.csv")

    with open(perturbed_in_disease_header) as f:
        perturbed_in_disease_header = f.read()
    with open(phosphorylation_header) as f:
        phosphorylation_header = f.read()

    assert "id;" in perturbed_in_disease_header
    assert "id;" not in phosphorylation_header


def test_write_edge_data_from_list_no_props(bw):
    number_of_items = 4
    edges = []
    for i in range(number_of_items):
        edge_1 = BioCypherEdge(
            source_id=f"p{i}",
            target_id=f"p{i + 1}",
            relationship_label="PERTURBED_IN_DISEASE",
        )
        edges.append(edge_1)
        edge_2 = BioCypherEdge(
            source_id=f"m{i}",
            target_id=f"p{i + 1}",
            relationship_label="Is_Mutated_In",
        )
        edges.append(edge_2)

    passed = bw._write_edge_data(edges, batch_size=int(1e4))

    tmp_path = bw.outdir

    perturbed_in_disease_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part000.csv")
    is_mutated_in_csv = os.path.join(tmp_path, "Is_Mutated_In-part000.csv")

    with open(perturbed_in_disease_csv) as f:
        perturbed_in_disease = f.read()
    with open(is_mutated_in_csv) as f:
        is_mutated_in = f.read()

    assert passed
    assert "p0;" in perturbed_in_disease
    assert "p1;" in perturbed_in_disease
    assert "PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "p1;" in perturbed_in_disease
    assert "p2;" in perturbed_in_disease
    assert "PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "\n" in perturbed_in_disease
    assert "m0;" in is_mutated_in
    assert "p1;" in is_mutated_in
    assert "Is_Mutated_In" in is_mutated_in
    assert "m1;" in is_mutated_in
    assert "p2;" in is_mutated_in
    assert "Is_Mutated_In" in is_mutated_in
    assert "\n" in is_mutated_in


@pytest.mark.parametrize("length", [8], scope="module")
def test_write_edge_data_headers_import_call(bw, _get_nodes, _get_edges):
    edges = _get_edges

    nodes = _get_nodes

    def edge_gen1(edges):
        yield from edges[:4]

    def edge_gen2(edges):
        yield from edges[4:]

    passed = bw.write_edges(edge_gen1(edges))
    assert passed

    passed = bw.write_edges(edge_gen2(edges))
    assert passed

    passed = bw.write_nodes(nodes)
    assert passed

    bw.write_import_call()

    tmp_path = bw.outdir

    perturbed_in_disease_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-header.csv")
    is_mutated_in_csv = os.path.join(tmp_path, "Is_Mutated_In-header.csv")
    call_csv = os.path.join(tmp_path, "neo4j-admin-import-call.sh")

    with open(perturbed_in_disease_csv) as f:
        perturbed_in_disease = f.read()
    with open(is_mutated_in_csv) as f:
        is_mutated_in = f.read()
    with open(call_csv) as f:
        call = f.read()

    assert perturbed_in_disease == ":START_ID;id;residue;level:long;:END_ID;:TYPE"
    assert is_mutated_in == ":START_ID;id;site;confidence:long;:END_ID;:TYPE"

    assert "neo4j-admin" in call
    assert "import" in call
    assert '--delimiter=";"' in call
    assert '--nodes="' in call
    assert "PERTURBED_IN_DISEASE" in call
    assert "Is_Mutated_In" in call


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_duplicate_edges(bw, _get_edges):
    edges = _get_edges
    edges.append(edges[0])

    passed = bw.write_edges(edges)

    tmp_path = bw.outdir

    perturbed_in_disease_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part000.csv")
    is_mutated_in_csv = os.path.join(tmp_path, "Is_Mutated_In-part000.csv")

    perturbed_in_disease = sum(1 for _ in open(perturbed_in_disease_csv))
    is_mutated_in = sum(1 for _ in open(is_mutated_in_csv))

    assert passed and perturbed_in_disease == 4 and is_mutated_in == 4


@pytest.mark.parametrize("length", [4], scope="module")
def test_BioCypherRelAsNode_implementation(bw, _get_rel_as_nodes):
    trips = _get_rel_as_nodes

    def gen(lis):
        yield from lis

    passed = bw.write_edges(gen(trips))

    tmp_path = bw.outdir

    is_source_of_csv = os.path.join(tmp_path, "IS_SOURCE_OF-part000.csv")
    is_target_of_csv = os.path.join(tmp_path, "IS_TARGET_OF-part000.csv")
    post_translational_interaction_csv = os.path.join(tmp_path, "PostTranslationalInteraction-part000.csv")

    with open(is_source_of_csv) as f:
        is_source_of = f.read()
    with open(is_target_of_csv) as f:
        is_target_of = f.read()
    with open(post_translational_interaction_csv) as f:
        post_translational_interaction = f.read()

    assert passed
    assert "i1;" in is_source_of
    assert "p1;" in is_source_of
    assert "IS_SOURCE_OF" in is_source_of
    assert "\n" in is_source_of
    assert "i0;" in is_target_of
    assert "p2;" in is_target_of
    assert "IS_TARGET_OF" in is_target_of
    assert "\n" in is_target_of
    assert "i1;True;-1;'i1';'id'" in post_translational_interaction
    assert "Association" in post_translational_interaction
    assert "\n" in post_translational_interaction


@pytest.mark.parametrize("length", [8], scope="module")
def test_RelAsNode_overwrite_behaviour(bw, _get_rel_as_nodes):
    # if rel as node is called from successive write calls, SOURCE_OF,
    # TARGET_OF, and PART_OF should be continued, not overwritten
    trips = _get_rel_as_nodes

    def gen1(lis):
        yield from lis[:5]

    def gen2(lis):
        yield from lis[5:]

    passed1 = bw.write_edges(gen1(trips))
    passed2 = bw.write_edges(gen2(trips))

    tmp_path = bw.outdir

    is_source_of_csv = os.path.join(tmp_path, "IS_SOURCE_OF-part001.csv")

    assert passed1 and passed2 and isfile(is_source_of_csv)


def test_write_mixed_edges(bw):
    mixed = []
    number_of_items = 4
    for i in range(number_of_items):
        edge_3 = BioCypherEdge(
            source_id=f"p{i + 1}",
            target_id=f"p{i + 1}",
            relationship_label="PERTURBED_IN_DISEASE",
        )
        mixed.append(edge_3)

        node = BioCypherNode(
            f"i{i + 1}",
            "post translational interaction",
        )
        edge_1 = BioCypherEdge(
            source_id=f"i{i + 1}",
            target_id=f"p{i + 1}",
            relationship_label="IS_SOURCE_OF",
        )
        edge_2 = BioCypherEdge(
            source_id=f"i{i}",
            target_id=f"p{i + 2}",
            relationship_label="IS_TARGET_OF",
        )
        mixed.append(BioCypherRelAsNode(node, edge_1, edge_2))

    def gen(lis):
        yield from lis

    passed = bw.write_edges(gen(mixed))

    tmp_path = bw.outdir

    post_translational_interaction_csv = os.path.join(tmp_path, "PostTranslationalInteraction-header.csv")
    is_source_of_csv = os.path.join(tmp_path, "IS_SOURCE_OF-header.csv")
    is_target_of_csv = os.path.join(tmp_path, "IS_TARGET_OF-header.csv")
    perturbed_in_disease_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-header.csv")

    assert (
        passed
        and os.path.isfile(post_translational_interaction_csv)
        and os.path.isfile(is_source_of_csv)
        and os.path.isfile(is_target_of_csv)
        and os.path.isfile(perturbed_in_disease_csv)
    )


def test_duplicate_id(bw):
    nodes = []

    tmp_path = bw.outdir

    csv = os.path.join(tmp_path, "Protein-part000.csv")

    # remove csv file in path
    if os.path.exists(csv):
        os.remove(csv)

    # four proteins, four miRNAs
    for _ in range(2):
        biocypher_node_protein = BioCypherNode(
            node_id="p1",
            node_label="protein",
            properties={
                "name": "StringProperty1",
                "score": 4.32,
                "taxon": 9606,
                "genes": ["gene1", "gene2"],
            },
        )
        nodes.append(biocypher_node_protein)

    passed = bw.write_nodes(nodes)

    l_lines0 = sum(1 for _ in open(csv))

    assert passed and l_lines0 == 1


def test_write_synonym(bw):
    nodes = []

    tmp_path = bw.outdir

    csv = os.path.join(tmp_path, "Complex-part000.csv")

    # remove csv file in path
    if os.path.exists(csv):
        os.remove(csv)
    # four proteins, four miRNAs
    for _ in range(4):
        biocypher_node_protein = BioCypherNode(
            node_id=f"p{_ + 1}",
            node_label="complex",
            properties={
                "name": "StringProperty1",
                "score": 4.32,
                "taxon": 9606,
            },
        )
        nodes.append(biocypher_node_protein)

    passed = bw.write_nodes(nodes)

    with open(csv) as f:
        complex = f.read()

    assert passed and os.path.exists(csv)
    assert "p1;'StringProperty1';4.32;9606;'p1';'id'" in complex
    assert "Complex" in complex


def test_write_strict(bw_strict):
    node_1 = BioCypherNode(
        node_id="p1",
        node_label="protein",
        properties={
            "name": "StringProperty1",
            "score": 4.32,
            "taxon": 9606,
            "genes": ["gene1", "gene2"],
            "source": "source1",
            "version": "version1",
            "licence": "licence1",
        },
    )

    passed = bw_strict.write_nodes([node_1])

    assert passed

    tmp_path = bw_strict.outdir

    csv = os.path.join(tmp_path, "Protein-part000.csv")

    with open(csv) as f:
        protein = f.read()

    assert "p1;'StringProperty1';4.32;9606;'gene1|gene2';'p1';'id';'source1';'version1';'licence1'" in protein
    assert "BiologicalEntity" in protein


@pytest.mark.parametrize("length", [4], scope="module")
def test_tab_delimiter(bw_tab, _get_nodes):
    passed = bw_tab.write_nodes(_get_nodes)

    assert passed

    tmp_path = bw_tab.outdir

    header = os.path.join(tmp_path, "Protein-header.csv")

    with open(header) as f:
        protein = f.read()

    assert "\t" in protein

    call = bw_tab._construct_import_call()

    assert '--delimiter="\\t"' in call


def test_check_label_name():
    # Test case 1: label with compliant characters
    assert parse_label("Compliant_Label") == "Compliant_Label"

    # Test case 2: label with non-compliant characters
    assert parse_label("Non@Compl<>i(an)t_Labe#l") == "NonCompliant_Label"

    # Test case 3: label starts with a number
    assert parse_label("15Invalid_Label") == "Invalid_Label"

    # Test case 4: label starts with a non-alphanumeric character
    assert parse_label("@Invalid_Label") == "Invalid_Label"

    # Additional test case: label with dot (for class hierarchy of BioCypher)
    assert parse_label("valid.label") == "valid.label"

    # Additional test case: label with dot and non-compliant characters
    assert parse_label("In.valid.Label@1") == "In.valid.Label1"
    # Assert warning log is written
