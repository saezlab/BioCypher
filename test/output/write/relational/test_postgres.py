import os
import subprocess

import pytest


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_node_data_from_gen_comma_postgresql(bw_comma_postgresql, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = bw_comma_postgresql._write_node_data(node_gen(nodes), batch_size=1e6)
    assert passed

    tmp_path = bw_comma_postgresql.outdir

    protein_csv = os.path.join(tmp_path, "Protein-part000.csv")
    micro_rna_csv = os.path.join(tmp_path, "MicroRNA-part000.csv")

    with open(protein_csv) as f:
        protein = f.read()

    with open(micro_rna_csv) as f:
        micro_rna = f.read()

    assert 'p1,"StringProperty1",4.0,9606' in protein
    assert "uniprot" in protein
    assert "BiologicalEntity" in protein
    assert "Polypeptide" in protein
    assert "Protein" in protein
    assert 'm1,"StringProperty1",9606,"m1","mirbase"' in micro_rna
    assert "ChemicalEntity" in micro_rna


@pytest.mark.parametrize("length", [4], scope="module")
def test_write_node_data_from_gen_tab_postgresql(bw_tab_postgresql, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed = bw_tab_postgresql._write_node_data(node_gen(nodes), batch_size=1e6)

    tmp_path = bw_tab_postgresql.outdir

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


@pytest.mark.requires_postgresql()
@pytest.mark.parametrize("length", [4], scope="module")
def test_database_import_node_data_from_gen_comma_postgresql(bw_comma_postgresql, _get_nodes, create_database_postgres):
    (
        dbname,
        user,
        host,
        port,
        password,
        create_database_success,
    ) = create_database_postgres
    assert create_database_success

    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    bw_comma_postgresql.write_nodes(node_gen(nodes))

    tmp_path = bw_comma_postgresql.outdir

    # verify that all files have been created
    assert set(os.listdir(tmp_path)) == set(
        [
            "protein-create_table.sql",
            "Protein-part000.csv",
            "microrna-create_table.sql",
            "MicroRNA-part000.csv",
        ],
    )

    bw_comma_postgresql.write_import_call()
    # verify that import call has been created
    import_scripts = [name for name in os.listdir(tmp_path) if name.endswith("-import-call.sh")]
    assert len(import_scripts) == 1

    import_script = import_scripts[0]
    script = os.path.join(tmp_path, import_script)
    with open(script) as f:
        commands = f.readlines()
        assert len(commands) == 16

    for command in commands:
        result = subprocess.run(command, shell=True, check=False)
        assert result.returncode == 0

    # check data in the databases
    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM protein;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 4 entires in table
    assert "4" in result.stdout.decode()

    # check data in the databases
    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM microrna;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 4 entires in table
    assert "4" in result.stdout.decode()


@pytest.mark.requires_postgresql()
@pytest.mark.parametrize("length", [5], scope="module")
def test_database_import_node_data_from_gen_tab_postgresql(bw_tab_postgresql, _get_nodes, create_database_postgres):
    (
        dbname,
        user,
        host,
        port,
        password,
        create_database_success,
    ) = create_database_postgres
    assert create_database_success

    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    bw_tab_postgresql.write_nodes(node_gen(nodes))

    tmp_path = bw_tab_postgresql.outdir

    # verify that all files have been created
    assert set(os.listdir(tmp_path)) == set(
        [
            "protein-create_table.sql",
            "Protein-part000.csv",
            "microrna-create_table.sql",
            "MicroRNA-part000.csv",
        ],
    )

    bw_tab_postgresql.write_import_call()
    # verify that import call has been created
    import_scripts = [name for name in os.listdir(tmp_path) if name.endswith("-import-call.sh")]
    assert len(import_scripts) == 1

    import_script = import_scripts[0]
    script = os.path.join(tmp_path, import_script)
    with open(script) as f:
        commands = f.readlines()
        assert len(commands) == 16

    for command in commands:
        result = subprocess.run(command, shell=True, check=False)
        assert result.returncode == 0

    # check data in the databases
    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM protein;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 5 entires in table
    assert "5" in result.stdout.decode()

    # check data in the databases
    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM microrna;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 5 entires in table
    assert "5" in result.stdout.decode()


@pytest.mark.requires_postgresql()
@pytest.mark.parametrize("length", [8], scope="module")
def test_database_import_edge_data_from_gen_comma_postgresql(
    bw_comma_postgresql,
    _get_nodes,
    create_database_postgres,
    _get_edges,
):
    (
        dbname,
        user,
        host,
        port,
        password,
        create_database_success,
    ) = create_database_postgres
    assert create_database_success

    edges = _get_edges

    nodes = _get_nodes

    def edge_gen1(edges):
        yield from edges[:4]

    def edge_gen2(edges):
        yield from edges[4:]

    process_1 = bw_comma_postgresql.write_edges(edge_gen1(edges))
    process_2 = bw_comma_postgresql.write_edges(edge_gen2(edges))
    process_3 = bw_comma_postgresql.write_nodes(nodes)

    assert all([process_1, process_2, process_3])

    bw_comma_postgresql.write_import_call()

    tmp_path = bw_comma_postgresql.outdir

    # verify that import call has been created
    import_scripts = [name for name in os.listdir(tmp_path) if name.endswith("-import-call.sh")]
    assert len(import_scripts) == 1

    import_script = import_scripts[0]
    script = os.path.join(tmp_path, import_script)
    with open(script) as f:
        commands = f.readlines()

    assert len(commands) > 0
    assert str(tmp_path) in "\n".join(commands)
    assert "protein-create_table.sql" in "\n".join(commands)
    assert "--user" in "\n".join(commands)

    for command in commands:
        result = subprocess.run(command, shell=True, check=False)

    assert result.returncode == 0

    # check data in the databases
    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM is_mutated_in;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 2 entries in table
    assert "8" in result.stdout.decode()

    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM perturbed_in_disease;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 2 entries in table
    assert "8" in result.stdout.decode()


@pytest.mark.requires_postgresql()
@pytest.mark.parametrize("length", [8], scope="module")
def test_database_import_edge_data_from_gen_tab_postgresql(
    bw_tab_postgresql,
    _get_nodes,
    create_database_postgres,
    _get_edges,
):
    (
        dbname,
        user,
        host,
        port,
        password,
        create_database_success,
    ) = create_database_postgres
    assert create_database_success

    edges = _get_edges

    nodes = _get_nodes

    def edge_gen1(edges):
        yield from edges[:4]

    def edge_gen2(edges):
        yield from edges[4:]

    process_1 = bw_tab_postgresql.write_edges(edge_gen1(edges))
    process_2 = bw_tab_postgresql.write_edges(edge_gen2(edges))
    process_3 = bw_tab_postgresql.write_nodes(nodes)

    assert all([process_1, process_2, process_3])

    bw_tab_postgresql.write_import_call()

    tmp_path = bw_tab_postgresql.outdir

    # verify that import call has been created
    import_scripts = [name for name in os.listdir(tmp_path) if name.endswith("-import-call.sh")]
    assert len(import_scripts) == 1

    import_script = import_scripts[0]
    script = os.path.join(tmp_path, import_script)
    with open(script) as f:
        commands = f.readlines()

    assert len(commands) > 1
    assert str(tmp_path) in "\n".join(commands)
    assert "protein-create_table.sql" in "\n".join(commands)
    assert "--user" in "\n".join(commands)

    for command in commands:
        result = subprocess.run(command, shell=True, check=False)
        assert result.returncode == 0

    # check data in the databases
    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM is_mutated_in;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 2 entires in table
    assert "8" in result.stdout.decode()

    command = (
        f"PGPASSWORD={password} psql -c 'SELECT COUNT(*) FROM perturbed_in_disease;' "
        f"--dbname {dbname} --host {host} --port {port} --user {user}"
    )
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    # subprocess success
    assert result.returncode == 0
    # 2 entires in table
    assert "8" in result.stdout.decode()
