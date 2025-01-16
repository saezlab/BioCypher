import neo4j
import pytest

from biocypher._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from biocypher.output.connect._neo4j_driver import _Neo4jDriver


@pytest.mark.requires_neo4j
def test_create_driver(driver):
    assert isinstance(driver, _Neo4jDriver)


@pytest.mark.requires_neo4j
def test_connect_to_db(driver):
    assert isinstance(driver._driver.driver, neo4j.Neo4jDriver)


@pytest.mark.requires_neo4j
def test_increment_version(driver):
    driver._driver.wipe_db()
    query = "CREATE (n:BioCypher {id: 'v19700101-000000'})"
    driver._driver.query(query, db="test")
    driver._update_meta_graph()

    result, summary = driver._driver.query("MATCH (n:BioCypher) RETURN n", db="test")

    assert len(result) == 2


@pytest.mark.requires_neo4j
def test_explain(driver):
    query = "MATCH (n) WITH n LIMIT 25 MATCH (n)--(m)--(f) RETURN n, m, f"
    explanation = driver._driver.explain(query)
    text = explanation[0]

    assert "args" in text and "identifiers" in text


@pytest.mark.requires_neo4j
def test_profile(driver):
    query = "MATCH (n) RETURN n LIMIT 100"
    profile = driver._driver.profile(query)
    text = profile[0]

    assert "args" in text and "identifiers" in text


@pytest.mark.requires_neo4j
def test_add_invalid_biocypher_node(driver):
    # neo4j database needs to be running!

    with pytest.raises(ValueError):
        driver.add_biocypher_nodes(1)

    with pytest.raises(ValueError):
        driver.add_biocypher_nodes("String")


@pytest.mark.requires_neo4j
def test_add_single_biocypher_node(driver):
    # neo4j database needs to be running!
    node = BioCypherNode(node_id="test_id1", node_label="Test")
    driver.add_biocypher_nodes(node)
    result, summary = driver._driver.query(
        "MATCH (n:Test) WITH n, n.id AS id RETURN id ",
    )
    assert result[0]["id"] == "test_id1"


@pytest.mark.requires_neo4j
def test_add_biocypher_node_list(driver):
    # neo4j database needs to be running!
    node_1 = BioCypherNode(node_id="test_id1", node_label="Test")
    node_2 = BioCypherNode(node_id="test_id2", node_label="Test")
    driver.add_biocypher_nodes([node_1, node_2])
    result, summary = driver._driver.query(
        "MATCH (n:Test) WITH n, n.id AS id RETURN id ",
    )
    assert set([result[0]["id"], result[1]["id"]]) == set(["test_id1", "test_id2"])


@pytest.mark.requires_neo4j
def test_add_biocypher_node_generator(driver):
    # neo4j database needs to be running!
    # generator
    def gen(nodes):
        for g in nodes:
            yield BioCypherNode(g[0], g[1])

    node_generator = gen([("test_id1", "Test"), ("test_id2", "Test")])

    driver.add_biocypher_nodes(node_generator)
    result, summary = driver._driver.query(
        "MATCH (n:Test) WITH n, n.id AS id RETURN id ",
    )

    ids = [n["id"] for n in result]

    assert "test_id1" in ids
    assert "test_id2" in ids


@pytest.mark.requires_neo4j
def test_add_specific_id_node(driver):
    node = BioCypherNode(node_id="CHAT", node_label="Gene", preferred_id="hgnc")
    driver.add_biocypher_nodes(node)

    result, summary = driver._driver.query(
        "MATCH (n:Gene) RETURN n",
    )

    assert result[0]["n"].get("id") == "CHAT"
    assert result[0]["n"].get("preferred_id") == "hgnc"


@pytest.mark.requires_neo4j
def test_add_generic_id_node(driver):
    node = BioCypherNode(node_id="CHAT", node_label="Gene", preferred_id="HGNC")
    driver.add_biocypher_nodes(node)

    result, summary = driver._driver.query(
        "MATCH (n:Gene) RETURN n",
    )

    assert result[0]["n"].get("id") is not None


@pytest.mark.requires_neo4j
def test_add_invalid_biocypher_edge(driver):
    # neo4j database needs to be running!
    with pytest.raises(ValueError):
        driver.add_biocypher_edges([1, 2, 3])


@pytest.mark.requires_neo4j
def test_add_single_biocypher_edge_explicit_node_creation(driver):
    # neo4j database needs to be running!
    node_1 = BioCypherNode("src", "Test")
    node_2 = BioCypherNode("tar", "Test")
    driver.add_biocypher_nodes([node_1, node_2])

    edge = BioCypherEdge("src", "tar", "Test")
    driver.add_biocypher_edges(edge)
    result, summary = driver._driver.query(
        "MATCH (n1)-[r:Test]->(n2) "
        "WITH n1, n2, n1.id AS id1, n2.id AS id2, type(r) AS label "
        "RETURN id1, id2, label",
    )
    assert result[0]["id1"] == "src" and result[0]["id2"] == "tar" and result[0]["label"] == "Test"


@pytest.mark.requires_neo4j
def test_add_single_biocypher_edge_missing_nodes(driver):
    # neo4j database needs to be running!
    # merging on non-existing nodes creates them without labels; what is
    # the desired behaviour here? do we only want to MATCH?

    edge = BioCypherEdge("src", "tar", "Test")
    driver.add_biocypher_edges(edge)
    result, summary = driver._driver.query(
        "MATCH (n1)-[r:Test]->(n2) "
        "WITH n1, n2, n1.id AS id1, n2.id AS id2, type(r) AS label "
        "RETURN id1, id2, label",
    )
    assert result[0]["id1"] == "src" and result[0]["id2"] == "tar" and result[0]["label"] == "Test"


@pytest.mark.requires_neo4j
def test_add_biocypher_edge_list(driver):
    # neo4j database needs to be running!
    node_1 = BioCypherNode("src", "Test")
    node_2 = BioCypherNode("tar1", "Test")
    node_3 = BioCypherNode("tar2", "Test")
    driver.add_biocypher_nodes([node_1, node_2, node_3])

    # edge list
    edge_1 = BioCypherEdge("src", "tar1", "Test1")
    edge_2 = BioCypherEdge("src", "tar2", "Test2")
    driver.add_biocypher_edges([edge_1, edge_2])
    result, summary = driver._driver.query(
        "MATCH (n3)<-[r2:Test2]-(n1)-[r1:Test1]->(n2) "
        "WITH n1, n2, n3, n1.id AS id1, n2.id AS id2, n3.id AS id3, "
        "type(r1) AS label1, type(r2) AS label2 "
        "RETURN id1, id2, id3, label1, label2",
    )
    assert (
        result[0]["id1"] == "src"
        and result[0]["id2"] == "tar1"
        and result[0]["id3"] == "tar2"
        and result[0]["label1"] == "Test1"
        and result[0]["label2"] == "Test2"
    )


@pytest.mark.requires_neo4j
def test_add_biocypher_edge_generator(driver):
    # neo4j database needs to be running!
    node_1 = BioCypherNode("src", "Test")
    node_2 = BioCypherNode("tar1", "Test")
    node_3 = BioCypherNode("tar2", "Test")
    driver.add_biocypher_nodes([node_1, node_2, node_3])

    # generator
    def gen(edges):
        for e in edges:
            yield BioCypherEdge(
                e.get_source_id(),
                e.get_target_id(),
                e.get_label(),
            )

    # edge list
    edge_1 = BioCypherEdge("src", "tar1", "Test1")
    edge_2 = BioCypherEdge("src", "tar2", "Test2")
    edge_generator = gen([edge_1, edge_2])

    driver.add_biocypher_edges(edge_generator)
    result, summary = driver._driver.query(
        "MATCH (n3)<-[r2:Test2]-(n1)-[r1:Test1]->(n2) "
        "WITH n1, n2, n3, n1.id AS id1, n2.id AS id2, n3.id AS id3, "
        "type(r1) AS label1, type(r2) AS label2 "
        "RETURN id1, id2, id3, label1, label2",
    )
    assert (
        result[0]["id1"] == "src"
        and result[0]["id2"] == "tar1"
        and result[0]["id3"] == "tar2"
        and result[0]["label1"] == "Test1"
        and result[0]["label2"] == "Test2"
    )


@pytest.mark.requires_neo4j
def test_add_biocypher_interaction_as_BioCypherRelAsNode_list(driver):
    # neo4j database needs to be running!
    interaction_node_1 = BioCypherNode("int1", "Int1")
    interaction_node_2 = BioCypherNode("int2", "Int2")
    driver.add_biocypher_nodes([interaction_node_1, interaction_node_2])
    edge_1 = BioCypherEdge("src", "int1", "is_source_of")
    edge_2 = BioCypherEdge("tar", "int1", "is_target_of")
    edge_3 = BioCypherEdge("src", "int2", "is_source_of")
    edge_4 = BioCypherEdge("tar", "int2", "is_target_of")
    relationship_1, relationship_2 = (
        BioCypherRelAsNode(interaction_node_1, edge_1, edge_2),
        BioCypherRelAsNode(interaction_node_2, edge_3, edge_4),
    )
    driver.add_biocypher_edges([relationship_1, relationship_2])
    result, summary = driver._driver.query(
        "MATCH (n2)-[e4:is_target_of]->(i2:Int2)<-[e3:is_source_of]-"
        "(n1)-[e1:is_source_of]->(i1:Int1)<-[e2:is_target_of]-(n2)"
        "WITH n1, n2, i1, i2, n1.id AS id1, n2.id AS id2, "
        "i1.id AS id3, i2.id AS id4, "
        "type(e1) AS label1, type(e2) AS label2, "
        "type(e3) AS label3, type(e4) AS label4 "
        "RETURN id1, id2, id3, id4, label1, label2, label3, label4",
    )
    assert (
        result[0]["id1"] == "src"
        and result[0]["id2"] == "tar"
        and result[0]["id3"] == "int1"
        and result[0]["id4"] == "int2"
        and result[0]["label1"] == "is_source_of"
        and result[0]["label2"] == "is_target_of"
        and result[0]["label3"] == "is_source_of"
        and result[0]["label4"] == "is_target_of"
    )


@pytest.mark.requires_neo4j
def test_add_biocypher_interaction_as_BioCypherRelAsNode_generator(driver):
    # neo4j database needs to be running!
    interaction_node_1 = BioCypherNode("int1", "Int1")
    interaction_node_2 = BioCypherNode("int2", "Int2")
    driver.add_biocypher_nodes([interaction_node_1, interaction_node_2])
    edge_1 = BioCypherEdge("src", "int1", "is_source_of")
    edge_2 = BioCypherEdge("tar", "int1", "is_target_of")
    edge_3 = BioCypherEdge("src", "int2", "is_source_of")
    edge_4 = BioCypherEdge("tar", "int2", "is_target_of")
    relationship_1, relationship_2 = (
        BioCypherRelAsNode(interaction_node_1, edge_1, edge_2),
        BioCypherRelAsNode(interaction_node_2, edge_3, edge_4),
    )
    relasnode_list = [relationship_1, relationship_2]

    def gen(lis):
        for tup in lis:
            yield tup

    driver.add_biocypher_edges(gen(relasnode_list))
    result, summary = driver._driver.query(
        "MATCH (n2)-[e4:is_target_of]->(i2:Int2)<-[e3:is_source_of]-"
        "(n1)-[e1:is_source_of]->(i1:Int1)<-[e2:is_target_of]-(n2)"
        "WITH n1, n2, i1, i2, n1.id AS id1, n2.id AS id2, "
        "i1.id AS id3, i2.id AS id4, "
        "type(e1) AS label1, type(e2) AS label2, "
        "type(e3) AS label3, type(e4) AS label4 "
        "RETURN id1, id2, id3, id4, label1, label2, label3, label4",
    )
    assert (
        result[0]["id1"] == "src"
        and result[0]["id2"] == "tar"
        and result[0]["id3"] == "int1"
        and result[0]["id4"] == "int2"
        and result[0]["label1"] == "is_source_of"
        and result[0]["label2"] == "is_target_of"
        and result[0]["label3"] == "is_source_of"
        and result[0]["label4"] == "is_target_of"
    )


@pytest.mark.requires_neo4j
def test_pretty_profile(driver):
    prof, printout = driver._driver.profile(
        "UNWIND [1,2,3,4,5] as id MERGE (n:Test {id: id}) MERGE (x:Test {id: id + 1})",
    )

    assert "args" in prof and "ProduceResults" in printout[1]


@pytest.mark.requires_neo4j
def test_pretty_explain(driver):
    plan, printout = driver._driver.explain(
        "UNWIND [1,2,3,4,5] as id MERGE (n:Test {id: id}) MERGE (x:Test {id: id + 1})",
    )

    assert "args" in plan and "ProduceResults" in printout[0]
