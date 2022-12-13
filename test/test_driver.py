import neo4j
import pytest

from biocypher._entity import Edge, Node, RelAsNode
from biocypher._driver import Driver


def init_driver():

    # neo4j database needs to be running!
    # there needs to be a database called "test" in the neo4j instance

    return Driver(
        db_name="test",
        db_passwd="your_password_here",
        wipe=True,
        increment_version=False,
        user_schema_config_path="biocypher/_config/test_schema_config.yaml",
    )


@pytest.fixture
def driver():

    d = init_driver()

    yield d

    # teardown
    d.query("MATCH (n:Test)" "DETACH DELETE n")
    d.query("MATCH (n:Int1)" "DETACH DELETE n")
    d.query("MATCH (n:Int2)" "DETACH DELETE n")

    # to deal with merging on non-existing nodes
    # see test_add_single_biocypher_edge_missing_nodes()
    d.query("MATCH (n2) WHERE n2.id = 'src'" "DETACH DELETE n2")
    d.query("MATCH (n3) WHERE n3.id = 'tar'" "DETACH DELETE n3")
    d.close()


requires_neo4j = pytest.mark.skipif(
    init_driver().db_status() != 'online',
    reason='This test requires connection to a Neo4j server.'
)


@requires_neo4j
def test_wipe():
    # just convenience function to wipe the database in testing env
    d = Driver(wipe=True)
    d.close()

    assert True


def test_create_driver(driver):

    assert isinstance(driver, Driver)


def test_create_offline():
    d = Driver(offline=True)
    assert isinstance(d, Driver)
    d.close()


@requires_neo4j
def test_connect_to_db(driver):

    assert isinstance(driver.driver, neo4j.Neo4jDriver)


@requires_neo4j
def test_explain(driver):
    query = "MATCH (n) WITH n LIMIT 25 MATCH (n)--(m)--(f) RETURN n, m, f"
    e = driver.explain(query)
    t = e[0]

    assert "args" in t and "identifiers" in t


@requires_neo4j
def test_profile(driver):
    query = "MATCH (n) RETURN n LIMIT 100"
    p = driver.profile(query)
    t = p[0]

    assert "args" in t and "identifiers" in t


def test_add_invalid_biocypher_node(driver):
    # neo4j database needs to be running!

    with pytest.raises(TypeError):
        driver.add_biocypher_nodes(1)

    with pytest.raises(TypeError):
        driver.add_biocypher_nodes("String")


@requires_neo4j
def test_add_single_biocypher_node(driver):
    # neo4j database needs to be running!
    n = Node(id="test_id1", label="Test")
    driver.add_biocypher_nodes(n)
    r, summary = driver.query(
        "MATCH (n:Test) " "WITH n, n.id AS id " "RETURN id "
    )
    assert r[0]["id"] == "test_id1"


@requires_neo4j
def test_add_biocypher_node_list(driver):
    # neo4j database needs to be running!
    n1 = Node(id="test_id1", label="Test")
    n2 = Node(id="test_id2", label="Test")
    driver.add_biocypher_nodes([n1, n2])
    r, summary = driver.query(
        "MATCH (n:Test) " "WITH n, n.id AS id " "RETURN id "
    )
    assert set([r[0]["id"], r[1]["id"]]) == set(["test_id1", "test_id2"])


@requires_neo4j
def test_add_biocypher_node_generator(driver):
    # neo4j database needs to be running!
    # generator
    def gen(nodes):
        for g in nodes:
            yield Node(g[0], g[1])

    g = gen([("test_id1", "Test"), ("test_id2", "Test")])

    driver.add_biocypher_nodes(g)
    r, summary = driver.query(
        "MATCH (n:Test) " "WITH n, n.id AS id " "RETURN id "
    )
    assert r[0]["id"] == "test_id1" and r[1]["id"] == "test_id2"


@requires_neo4j
def test_add_specific_id_node(driver):
    n = Node(id="CHAT", label="Gene", id_type="hgnc")
    driver.add_biocypher_nodes(n)

    r, summary = driver.query("MATCH (n:Gene) " "RETURN n")

    assert r[0]["n"].get("id") == "CHAT"
    assert r[0]["n"].get("id_type") == "hgnc"


@requires_neo4j
def test_add_generic_id_node(driver):
    n = Node(id="CHAT", label="Gene", id_type="HGNC")
    driver.add_biocypher_nodes(n)

    r, summary = driver.query("MATCH (n:Gene) " "RETURN n")

    assert r[0]["n"].get("id") is not None


def test_add_invalid_biocypher_edge(driver):
    # neo4j database needs to be running!
    with pytest.raises(TypeError):
        driver.add_biocypher_edges([1, 2, 3])


@requires_neo4j
def test_add_single_biocypher_edge_explicit_node_creation(driver):
    # neo4j database needs to be running!
    n1 = Node("src", "Test")
    n2 = Node("tar", "Test")
    driver.add_biocypher_nodes([n1, n2])

    e = Edge("src", "tar", "Test")
    driver.add_biocypher_edges(e)
    r, summary = driver.query(
        "MATCH (n1)-[r:Test]->(n2) "
        "WITH n1, n2, n1.id AS id1, n2.id AS id2, type(r) AS label "
        "RETURN id1, id2, label"
    )
    assert (
        r[0]["id1"] == "src"
        and r[0]["id2"] == "tar"
        and r[0]["label"] == "Test"
    )


@requires_neo4j
def test_add_single_biocypher_edge_missing_nodes(driver):
    # neo4j database needs to be running!
    # merging on non-existing nodes creates them without labels; what is
    # the desired behaviour here? do we only want to MATCH?

    e = Edge("src", "tar", "Test")
    driver.add_biocypher_edges(e)
    r, summary = driver.query(
        "MATCH (n1)-[r:Test]->(n2) "
        "WITH n1, n2, n1.id AS id1, n2.id AS id2, type(r) AS label "
        "RETURN id1, id2, label"
    )
    assert (
        r[0]["id1"] == "src"
        and r[0]["id2"] == "tar"
        and r[0]["label"] == "Test"
    )


@requires_neo4j
def test_add_biocypher_edge_list(driver):
    # neo4j database needs to be running!
    n1 = Node("src", "Test")
    n2 = Node("tar1", "Test")
    n3 = Node("tar2", "Test")
    driver.add_biocypher_nodes([n1, n2, n3])

    # edge list
    e1 = Edge("src", "tar1", "Test1")
    e2 = Edge("src", "tar2", "Test2")
    driver.add_biocypher_edges([e1, e2])
    r, summary = driver.query(
        "MATCH (n3)<-[r2:Test2]-(n1)-[r1:Test1]->(n2) "
        "WITH n1, n2, n3, n1.id AS id1, n2.id AS id2, n3.id AS id3, "
        "type(r1) AS label1, type(r2) AS label2 "
        "RETURN id1, id2, id3, label1, label2"
    )
    assert (
        r[0]["id1"] == "src"
        and r[0]["id2"] == "tar1"
        and r[0]["id3"] == "tar2"
        and r[0]["label1"] == "Test1"
        and r[0]["label2"] == "Test2"
    )


@requires_neo4j
def test_add_biocypher_edge_generator(driver):
    # neo4j database needs to be running!
    n1 = Node("src", "Test")
    n2 = Node("tar1", "Test")
    n3 = Node("tar2", "Test")
    driver.add_biocypher_nodes([n1, n2, n3])

    # generator
    def gen(edges):
        for e in edges:
            yield Edge(e.source, e.target, e.label)

    # edge list
    e1 = Edge("src", "tar1", "Test1")
    e2 = Edge("src", "tar2", "Test2")
    g = gen([e1, e2])

    driver.add_biocypher_edges(g)
    r, summary = driver.query(
        "MATCH (n3)<-[r2:Test2]-(n1)-[r1:Test1]->(n2) "
        "WITH n1, n2, n3, n1.id AS id1, n2.id AS id2, n3.id AS id3, "
        "type(r1) AS label1, type(r2) AS label2 "
        "RETURN id1, id2, id3, label1, label2"
    )
    assert (
        r[0]["id1"] == "src"
        and r[0]["id2"] == "tar1"
        and r[0]["id3"] == "tar2"
        and r[0]["label1"] == "Test1"
        and r[0]["label2"] == "Test2"
    )


@requires_neo4j
def test_add_biocypher_interaction_as_relasnode_list(driver):
    # neo4j database needs to be running!
    i1 = Node("int1", "Int1")
    i2 = Node("int2", "Int2")
    driver.add_biocypher_nodes([i1, i2])
    e1 = Edge("src", "int1", "is_source_of")
    e2 = Edge("tar", "int1", "is_target_of")
    e3 = Edge("src", "int2", "is_source_of")
    e4 = Edge("tar", "int2", "is_target_of")
    r1, r2 = RelAsNode(i1, e1, e2), RelAsNode(i2, e3, e4)
    driver.add_biocypher_edges([r1, r2])
    r, summary = driver.query(
        "MATCH (n2)-[e4:is_target_of]->(i2:Int2)<-[e3:is_source_of]-"
        "(n1)-[e1:is_source_of]->(i1:Int1)<-[e2:is_target_of]-(n2)"
        "WITH n1, n2, i1, i2, n1.id AS id1, n2.id AS id2, "
        "i1.id AS id3, i2.id AS id4, "
        "type(e1) AS label1, type(e2) AS label2, "
        "type(e3) AS label3, type(e4) AS label4 "
        "RETURN id1, id2, id3, id4, label1, label2, label3, label4"
    )
    assert (
        r[0]["id1"] == "src"
        and r[0]["id2"] == "tar"
        and r[0]["id3"] == "int1"
        and r[0]["id4"] == "int2"
        and r[0]["label1"] == "is_source_of"
        and r[0]["label2"] == "is_target_of"
        and r[0]["label3"] == "is_source_of"
        and r[0]["label4"] == "is_target_of"
    )


@requires_neo4j
def test_add_biocypher_interaction_as_RelAsNode_generator(driver):
    # neo4j database needs to be running!
    i1 = Node("int1", "Int1")
    i2 = Node("int2", "Int2")
    driver.add_biocypher_nodes([i1, i2])
    e1 = Edge("src", "int1", "is_source_of")
    e2 = Edge("tar", "int1", "is_target_of")
    e3 = Edge("src", "int2", "is_source_of")
    e4 = Edge("tar", "int2", "is_target_of")
    r1, r2 = RelAsNode(i1, e1, e2), RelAsNode(i2, e3, e4)
    relasnode_list = [r1, r2]

    def gen(lis):
        for tup in lis:
            yield tup

    driver.add_biocypher_edges(gen(relasnode_list))
    r, summary = driver.query(
        "MATCH (n2)-[e4:is_target_of]->(i2:Int2)<-[e3:is_source_of]-"
        "(n1)-[e1:is_source_of]->(i1:Int1)<-[e2:is_target_of]-(n2)"
        "WITH n1, n2, i1, i2, n1.id AS id1, n2.id AS id2, "
        "i1.id AS id3, i2.id AS id4, "
        "type(e1) AS label1, type(e2) AS label2, "
        "type(e3) AS label3, type(e4) AS label4 "
        "RETURN id1, id2, id3, id4, label1, label2, label3, label4"
    )
    assert (
        r[0]["id1"] == "src"
        and r[0]["id2"] == "tar"
        and r[0]["id3"] == "int1"
        and r[0]["id4"] == "int2"
        and r[0]["label1"] == "is_source_of"
        and r[0]["label2"] == "is_target_of"
        and r[0]["label3"] == "is_source_of"
        and r[0]["label4"] == "is_target_of"
    )


@requires_neo4j
def test_pretty_profile(driver):
    prof, printout = driver.profile(
        "UNWIND [1,2,3,4,5] as id "
        "MERGE (n:Test {id: id}) "
        "MERGE (x:Test {id: id + 1})"
    )

    assert "args" in prof and "ProduceResults" in printout[1]


@requires_neo4j
def test_pretty_explain(driver):
    plan, printout = driver.explain(
        "UNWIND [1,2,3,4,5] as id "
        "MERGE (n:Test {id: id}) "
        "MERGE (x:Test {id: id + 1})"
    )

    assert "args" in plan and "ProduceResults" in printout[0]


@requires_neo4j
def test_access_translate(driver):
    assert driver.translate_term("mirna") == "MicroRNA"
    assert (
        driver.reverse_translate_term("MacromolecularComplexMixin")
        == "complex"
    )
    assert (
        driver.translate_query("MATCH (n:reactome) RETURN n")
        == "MATCH (n:Reactome.Pathway) RETURN n"
    )
    assert (
        driver.reverse_translate_query(
            "MATCH (n:Wikipathways.Pathway) RETURN n"
        )
        == "MATCH (n:wikipathways) RETURN n"
    )


def test_log_missing_bl_types(driver):
    driver.translator.notype = {}
    assert driver.log_missing_bl_types() == None

    driver.translator.notype = {"a": 1, "b": 2}
    mt = driver.log_missing_bl_types()

    assert mt.get("a") == 1 and mt.get("b") == 2
