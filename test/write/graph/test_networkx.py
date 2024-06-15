import os
import pickle

import pytest


@pytest.mark.parametrize("length", [4], scope="module")
def test_networkx_writer(bw_networkx, _get_nodes):
    nodes = _get_nodes

    def node_gen(nodes):
        yield from nodes

    passed_nodes = bw_networkx.write_nodes(node_gen(nodes), batch_size=1e6)
    assert passed_nodes
    write_result = bw_networkx.write_import_call()
    assert write_result

    tmp_path = bw_networkx.output_directory

    produced_files = os.listdir(tmp_path)
    assert len(produced_files) == 2
    expected_files = ["networkx_graph.pkl", "import_networkx.py"]
    for file in produced_files:
        assert file in expected_files

    with open(f"{tmp_path}/networkx_graph.pkl", "rb") as f:
        G = pickle.load(f)

    assert len(nodes) == len(G.nodes)
    for node in nodes:
        expected = node.get_properties()
        real = G.nodes[node.get_id()]
        del real["node_label"]
        assert expected == real

    import_call = bw_networkx._construct_import_call()
    assert "import pickle" in import_call
    assert "with open('./networkx_graph.pkl', 'rb') as f:" in import_call
    assert "G_loaded = pickle.load(f)" in import_call

    import_script_path = os.path.join(
        bw_networkx.output_directory, bw_networkx._get_import_script_name()
    )
    assert "import_networkx.py" in import_script_path
