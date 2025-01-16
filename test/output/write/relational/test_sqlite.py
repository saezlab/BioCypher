import os
import platform
import sqlite3
import subprocess

import pytest


@pytest.mark.parametrize("length", [4], scope="module")
def test_construct_import_call(bw_tab_sqlite, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = bw_tab_sqlite.write_nodes(node_gen(nodes), batch_size=1e6)

    tmp_path = bw_tab_sqlite.outdir

    protein_csv = os.path.join(tmp_path, "Protein-part000.csv")
    micro_rna_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")

    with open(protein_csv) as f:
        protein = f.read()

    with open(micro_rna_csv) as f:
        micro_rna = f.read()

    assert passed
    assert 'p1\t"StringProperty1"\t4.0\t9606\t' in protein
    assert '\t"uniprot"\t' in protein
    assert "BiologicalEntity" in protein
    assert "Polypeptide" in protein
    assert "Protein" in protein
    assert 'm1\t"StringProperty1"\t9606\t"m1"\t"mirbase"' in micro_rna
    assert "ChemicalEntity" in micro_rna

    import_call = bw_tab_sqlite._construct_import_call()
    assert "sqlite3 test_sqlite.db <" in import_call
    assert "protein-create_table.sql" in import_call
    assert "microrna-create_table.sql" in import_call
    assert "sqlite3 -separator $'\t' test_sqlite.db \".import" in import_call
    assert "Protein-part000.csv protein" in import_call
    assert "MicroRNA-part000.csv microrna" in import_call

    write_result = bw_tab_sqlite.write_import_call()
    assert write_result

    import_script_path = os.path.join(bw_tab_sqlite.outdir, bw_tab_sqlite._get_import_script_name())
    system = platform.system()
    if system == "Windows":
        output = subprocess.run(["cmd", "/c", import_script_path], check=True, shell=True)
    elif system == "Linux" or system == "Darwin":
        output = subprocess.run(["bash", import_script_path], check=True)
    else:
        raise OSError("Unsupported platform")
    assert output.returncode == 0

    # TODO: check database creation on Windows
    if system != "Windows":
        conn = sqlite3.connect("test_sqlite.db")
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [row[0] for row in tables]
        assert "protein" in table_names
        assert "microrna" in table_names

        cursor.execute("SELECT * FROM protein")
        proteins = cursor.fetchall()
        assert len(proteins) == 4

        cursor.execute("SELECT * FROM microrna")
        microrna = cursor.fetchall()
        assert len(microrna) == 4

        cursor.close()
        conn.close()
