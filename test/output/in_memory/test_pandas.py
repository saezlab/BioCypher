import pytest


def test_pandas(in_memory_pandas_kg):
    assert in_memory_pandas_kg.dfs == {}


@pytest.mark.parametrize("length", [4], scope="module")
def test_nodes(in_memory_pandas_kg, _get_nodes):
    in_memory_pandas_kg.add_tables(_get_nodes)
    assert "protein" in in_memory_pandas_kg.dfs.keys()
    assert "microRNA" in in_memory_pandas_kg.dfs.keys()
    assert "score" in in_memory_pandas_kg.dfs["protein"].columns
    assert "p3" in in_memory_pandas_kg.dfs["protein"]["node_id"].values
    assert "taxon" in in_memory_pandas_kg.dfs["microRNA"].columns
    assert "m2" in in_memory_pandas_kg.dfs["microRNA"]["node_id"].values


@pytest.mark.parametrize("length", [4], scope="module")
def test_nodes_gen(in_memory_pandas_kg, _get_nodes):
    def node_gen():
        for node in _get_nodes:
            yield node

    in_memory_pandas_kg.add_tables(node_gen())
    assert "protein" in in_memory_pandas_kg.dfs.keys()


@pytest.mark.parametrize("length", [4], scope="module")
def test_duplicates(in_memory_pandas_kg, _get_nodes):
    nodes = _get_nodes + _get_nodes
    in_memory_pandas_kg.add_tables(nodes)
    assert len(in_memory_pandas_kg.dfs["protein"].node_id) == 4


@pytest.mark.parametrize("length", [8], scope="module")
def test_two_step_add(in_memory_pandas_kg, _get_nodes):
    in_memory_pandas_kg.add_tables(_get_nodes[:4])
    in_memory_pandas_kg.add_tables(_get_nodes[4:])
    assert len(in_memory_pandas_kg.dfs["protein"].node_id) == 8


@pytest.mark.parametrize("length", [4], scope="module")
def test_edges(in_memory_pandas_kg, _get_edges):
    in_memory_pandas_kg.add_tables(_get_edges)
    assert "PERTURBED_IN_DISEASE" in in_memory_pandas_kg.dfs.keys()
    assert "Is_Mutated_In" in in_memory_pandas_kg.dfs.keys()
    assert "source_id" in in_memory_pandas_kg.dfs["PERTURBED_IN_DISEASE"].columns
    assert "p3" in in_memory_pandas_kg.dfs["PERTURBED_IN_DISEASE"]["source_id"].values
    assert "target_id" in in_memory_pandas_kg.dfs["Is_Mutated_In"].columns
    assert "p1" in in_memory_pandas_kg.dfs["Is_Mutated_In"]["target_id"].values


@pytest.mark.parametrize("length", [4], scope="module")
def test_edges_gen(in_memory_pandas_kg, _get_edges):
    def edge_gen():
        for edge in _get_edges:
            yield edge

    in_memory_pandas_kg.add_tables(edge_gen())
    assert "PERTURBED_IN_DISEASE" in in_memory_pandas_kg.dfs.keys()


@pytest.mark.parametrize("length", [4], scope="module")
def test_rel_as_nodes(in_memory_pandas_kg, _get_rel_as_nodes):
    in_memory_pandas_kg.add_tables(_get_rel_as_nodes)
    assert "post translational interaction" in in_memory_pandas_kg.dfs.keys()
    assert "directed" in in_memory_pandas_kg.dfs["post translational interaction"].columns
    assert "effect" in in_memory_pandas_kg.dfs["post translational interaction"].columns
    assert "i1" in in_memory_pandas_kg.dfs["post translational interaction"]["node_id"].values
    assert "IS_SOURCE_OF" in in_memory_pandas_kg.dfs.keys()
    assert "IS_TARGET_OF" in in_memory_pandas_kg.dfs.keys()
    assert "source_id" in in_memory_pandas_kg.dfs["IS_SOURCE_OF"].columns
    assert "i3" in in_memory_pandas_kg.dfs["IS_SOURCE_OF"]["source_id"].values
    assert "target_id" in in_memory_pandas_kg.dfs["IS_TARGET_OF"].columns
    assert "p2" in in_memory_pandas_kg.dfs["IS_TARGET_OF"]["target_id"].values
