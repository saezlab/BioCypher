import os

import pytest


@pytest.mark.parametrize("length", [4], scope="function")
def test_arango_write_data_headers_import_call(
    bw_arango,
    _get_nodes,
    _get_edges,
):
    # four proteins, four miRNAs
    nodes = _get_nodes

    edges = _get_edges

    protein_1 = bw_arango.write_nodes(nodes[:4])
    protein_2 = bw_arango.write_nodes(nodes[4:])
    protein_3 = bw_arango.write_edges(edges[:4])
    protein_4 = bw_arango.write_edges(edges[4:])

    assert all([protein_1, protein_2, protein_3, protein_4])

    bw_arango.write_import_call()

    tmp_path = bw_arango.outdir

    protein_header_csv = os.path.join(tmp_path, "Protein-header.csv")
    protein_data_1_csv = os.path.join(tmp_path, "Protein-part000.csv")
    protein_data_2_csv = os.path.join(tmp_path, "Protein-part001.csv")
    micro_rna_header_csv = os.path.join(tmp_path, "MicroRNA-header.csv")
    micro_rna_data_1_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")
    micro_rna_data_2_csv = os.path.join(tmp_path, "MicroRNA-part001.csv")
    disease_header_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-header.csv")
    disease_data_1_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part000.csv")
    disease_data_2_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE-part001.csv")
    mutation_header_csv = os.path.join(tmp_path, "Is_Mutated_In-header.csv")
    mutation_data_1_csv = os.path.join(tmp_path, "Is_Mutated_In-part000.csv")
    mutation_data_2_csv = os.path.join(tmp_path, "Is_Mutated_In-part001.csv")
    call_csv = os.path.join(tmp_path, "arangodb-import-call.sh")

    with open(protein_header_csv) as f:
        protein_header = f.read()
    with open(protein_data_1_csv) as f:
        protein_data_1 = f.readlines()
    with open(protein_data_2_csv) as f:
        protein_data_2 = f.readlines()
    with open(micro_rna_header_csv) as f:
        micro_rna_header = f.read()
    with open(micro_rna_data_1_csv) as f:
        micro_rna_data_1 = f.readlines()
    with open(micro_rna_data_2_csv) as f:
        micro_rna_data_2 = f.readlines()
    with open(disease_header_csv) as f:
        disease_header = f.read()
    with open(disease_data_1_csv) as f:
        disease_data_1 = f.readlines()
    with open(disease_data_2_csv) as f:
        disease_data_2 = f.readlines()
    with open(mutation_header_csv) as f:
        mutation_header = f.read()
    with open(mutation_data_1_csv) as f:
        mutation_data_1 = f.readlines()
    with open(mutation_data_2_csv) as f:
        mutation_data_2 = f.readlines()
    with open(call_csv) as f:
        call = f.read()

    assert protein_header == "_key,name,score,taxon,genes,id,preferred_id"
    assert micro_rna_header == "_key,name,taxon,id,preferred_id"
    assert "_from" in disease_header
    assert "_key" in disease_header
    assert "_to" in disease_header
    assert "_from" in mutation_header
    assert "_key" in mutation_header
    assert "_to" in mutation_header
    assert (
        len(protein_data_1)
        == len(protein_data_2)
        == len(micro_rna_data_1)
        == len(micro_rna_data_2)
        == len(disease_data_1)
        == len(disease_data_2)
        == len(mutation_data_1)
        == len(mutation_data_2)
        == 2
    )
    assert "arangoimp --type csv" in call
    assert "--collection proteins" in call
    assert "MicroRNA-part" in call

    # custom import call executable path
    bw_arango.import_call_bin_prefix = "custom/path/to/"

    os.remove(call_csv)
    bw_arango.write_import_call()

    with open(call_csv) as f:
        call = f.read()

    assert "custom/path/to/arangoimp --type csv" in call
