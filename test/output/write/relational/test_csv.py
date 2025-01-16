import os

import pytest


@pytest.mark.parametrize("length", [4], scope="module")
def test_pandas_csv_writer_nodes(bw_comma_csv, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = bw_comma_csv.write_nodes(node_gen(nodes), batch_size=1e6)

    tmp_path = bw_comma_csv.output_directory

    # print files in tmp_path
    for f in os.listdir(tmp_path):
        print(f)

    protein_csv = os.path.join(tmp_path, "protein.csv")
    micro_rna_csv = os.path.join(tmp_path, "microRNA.csv")

    with open(protein_csv) as f:
        protein = f.read()

    with open(micro_rna_csv) as f:
        micro_rna = f.read()

    assert passed
    assert "p1,protein,4.0,StringProperty1,9606," in protein
    assert "p4,protein,1.0,StringProperty1,9606" in protein
    assert ",uniprot" in protein
    assert "m1,microRNA,StringProperty1,9606,m1,mirbase" in micro_rna
    assert "m4,microRNA,StringProperty1,9606,m4,mirbase" in micro_rna

    import_call = bw_comma_csv._construct_import_call()
    assert "import pandas as pd" in import_call
    assert "protein = pd.read_csv('./protein.csv', header=0, index_col=0)" in import_call
    assert "microRNA = pd.read_csv('./microRNA.csv', header=0, index_col=0)" in import_call

    write_result = bw_comma_csv.write_import_call()
    assert write_result

    import_script_path = os.path.join(bw_comma_csv.output_directory, bw_comma_csv._get_import_script_name())
    assert "import_pandas_csv.py" in import_script_path


@pytest.mark.parametrize("length", [4], scope="module")
def test_pandas_csv_writer_edges(bw_comma_csv, _get_edges):
    edges = _get_edges

    def edge_gen(edges):
        yield from edges

    passed = bw_comma_csv._write_edge_data(edge_gen(edges))

    tmp_path = bw_comma_csv.output_directory

    pid_csv = os.path.join(tmp_path, "PERTURBED_IN_DISEASE.csv")
    imi_csv = os.path.join(tmp_path, "Is_Mutated_In.csv")

    with open(pid_csv) as f:
        perturbed_in_disease = f.read()
    with open(imi_csv) as f:
        is_mutated_in = f.read()

    assert passed
    assert "p0," in perturbed_in_disease
    assert "prel0," in perturbed_in_disease
    assert "T253," in perturbed_in_disease
    assert "4," in perturbed_in_disease
    assert "p1," in perturbed_in_disease
    assert "PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "p1," in perturbed_in_disease
    assert "prel1," in perturbed_in_disease
    assert "T253," in perturbed_in_disease
    assert "4," in perturbed_in_disease
    assert "p2," in perturbed_in_disease
    assert "PERTURBED_IN_DISEASE" in perturbed_in_disease
    assert "\n" in perturbed_in_disease
    assert "m0," in is_mutated_in
    assert "mrel0," in is_mutated_in
    assert "3-UTR," in is_mutated_in
    assert "1," in is_mutated_in
    assert "p1," in is_mutated_in
    assert "Is_Mutated_In" in is_mutated_in
    assert "m1," in is_mutated_in
    assert "mrel1," in is_mutated_in
    assert "3-UTR," in is_mutated_in
    assert "1," in is_mutated_in
    assert "p2," in is_mutated_in
    assert "Is_Mutated_In" in is_mutated_in
    assert "\n" in is_mutated_in
