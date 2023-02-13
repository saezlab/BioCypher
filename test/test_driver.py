import neo4j
import pytest

from biocypher import _misc
from biocypher._driver import Driver
from biocypher._entity import Edge, Node, RelAsNode

__all__ = [
    'test_access_translate',
    'test_add_biocypher_edge_generator',
    'test_add_biocypher_edge_list',
    'test_add_biocypher_interaction_as_RelAsNode_generator',
    'test_add_biocypher_interaction_as_relasnode_list',
    'test_add_biocypher_node_generator',
    'test_add_biocypher_node_list',
    'test_add_generic_id_node',
    'test_add_invalid_biocypher_edge',
    'test_add_invalid_biocypher_node',
    'test_add_single_biocypher_edge_explicit_node_creation',
    'test_add_single_biocypher_edge_missing_nodes',
    'test_add_single_biocypher_node',
    'test_add_specific_id_node',
    'test_connect_to_db',
    'test_create_driver',
    'test_create_offline',
    'test_explain',
    'test_log_missing_ontology_classes',
    'test_pretty_explain',
    'test_pretty_profile',
    'test_profile',
    'test_schema_config_from_web',
    'test_treelib_vis',
    'test_wipe',
]


@pytest.mark.requires_neo4j
@pytest.mark.inject_driver_args(driver_args = {'wipe': True})
def test_wipe(driver):

    query = 'MATCH (n:BioCypher) RETURN n'
    r, summary = driver.query(query)
    assert len(r) == 1


def test_create_driver(driver):

    assert isinstance(driver, Driver)


@pytest.mark.inject_driver_args(driver_args = {'offline': True})
def test_create_offline(driver):

    assert isinstance(driver, Driver)


@pytest.mark.requires_neo4j
def test_connect_to_db(driver):

    assert isinstance(driver.driver, neo4j.Neo4jDriver)


@pytest.mark.requires_neo4j
def test_explain(driver):
    query = 'MATCH (n) WITH n LIMIT 25 MATCH (n)--(m)--(f) RETURN n, m, f'
    e = driver.explain(query)
    t = e[0]

    assert 'args' in t and 'identifiers' in t


@pytest.mark.requires_neo4j
def test_profile(driver):
    query = 'MATCH (n) RETURN n LIMIT 100'
    p = driver.profile(query)
    t = p[0]

    assert 'args' in t and 'identifiers' in t


def test_add_invalid_biocypher_node(driver):
    # neo4j database needs to be running!

    with pytest.raises(TypeError):
        driver.add_biocypher_nodes(1)

    with pytest.raises(TypeError):
        driver.add_biocypher_nodes('String')


@pytest.mark.requires_neo4j
def test_add_single_biocypher_node(driver):
    # neo4j database needs to be running!
    n = Node(id='test_id1', label='Test')
    driver.add_biocypher_nodes(n)
    r, summary = driver.query(
        'MATCH (n:Test) ' 'WITH n, n.id AS id ' 'RETURN id ',
    )
    assert r[0]['id'] == 'test_id1'


@pytest.mark.requires_neo4j
def test_add_biocypher_node_list(driver):
    # neo4j database needs to be running!
    n1 = Node(id='test_id1', label='Test')
    n2 = Node(id='test_id2', label='Test')
    driver.add_biocypher_nodes([n1, n2])
    r, summary = driver.query(
        'MATCH (n:Test) ' 'WITH n, n.id AS id ' 'RETURN id ',
    )
    assert {r[0]['id'], r[1]['id']} == {'test_id1', 'test_id2'}


@pytest.mark.requires_neo4j
def test_add_biocypher_node_generator(driver):
    # neo4j database needs to be running!
    # generator
    def gen(nodes):
        for g in nodes:
            yield Node(g[0], g[1])

    g = gen([('test_id1', 'Test'), ('test_id2', 'Test')])

    driver.add_biocypher_nodes(g)
    r, summary = driver.query(
        'MATCH (n:Test) ' 'WITH n, n.id AS id ' 'RETURN id ',
    )
    assert r[0]['id'] == 'test_id1' and r[1]['id'] == 'test_id2'


@pytest.mark.requires_neo4j
def test_add_specific_id_node(driver):
    n = Node(id='CHAT', label='Gene', id_type='hgnc')
    driver.add_biocypher_nodes(n)

    r, summary = driver.query('MATCH (n:Gene) ' 'RETURN n')

    assert r[0]['n'].get('id') == 'CHAT'
    assert r[0]['n'].get('id_type') == 'hgnc'


@pytest.mark.requires_neo4j
def test_add_generic_id_node(driver):
    n = Node(id='CHAT', label='Gene', id_type='HGNC')
    driver.add_biocypher_nodes(n)

    r, summary = driver.query('MATCH (n:Gene) ' 'RETURN n')

    assert r[0]['n'].get('id') is not None


def test_add_invalid_biocypher_edge(driver):
    # neo4j database needs to be running!
    with pytest.raises(TypeError):
        driver.add_biocypher_edges([1, 2, 3])


@pytest.mark.requires_neo4j
def test_add_single_biocypher_edge_explicit_node_creation(driver):
    # neo4j database needs to be running!
    n1 = Node('src', 'Test')
    n2 = Node('tar', 'Test')
    driver.add_biocypher_nodes([n1, n2])

    e = Edge('src', 'tar', 'Test')
    driver.add_biocypher_edges(e)
    r, summary = driver.query(
        'MATCH (n1)-[r:TEST]->(n2) '
        'WITH n1, n2, n1.id AS id1, n2.id AS id2, type(r) AS label '
        'RETURN id1, id2, label',
    )
    assert (
        r[0]['id1'] == 'src'
        and r[0]['id2'] == 'tar'
        and r[0]['label'] == 'TEST'
    )


@pytest.mark.requires_neo4j
def test_add_single_biocypher_edge_missing_nodes(driver):
    # neo4j database needs to be running!
    # merging on non-existing nodes creates them without labels; what is
    # the desired behaviour here? do we only want to MATCH?

    e = Edge('src', 'tar', 'Test')
    driver.add_biocypher_edges(e)
    r, summary = driver.query(
        'MATCH (n1)-[r:TEST]->(n2) '
        'WITH n1, n2, n1.id AS id1, n2.id AS id2, type(r) AS label '
        'RETURN id1, id2, label',
    )
    assert (
        r[0]['id1'] == 'src'
        and r[0]['id2'] == 'tar'
        and r[0]['label'] == 'TEST'
    )


@pytest.mark.requires_neo4j
def test_add_biocypher_edge_list(driver):
    # neo4j database needs to be running!
    n1 = Node('src', 'Test')
    n2 = Node('tar1', 'Test')
    n3 = Node('tar2', 'Test')
    driver.add_biocypher_nodes([n1, n2, n3])

    # edge list
    e1 = Edge('src', 'tar1', 'Test1')
    e2 = Edge('src', 'tar2', 'Test2')
    driver.add_biocypher_edges([e1, e2])
    r, summary = driver.query(
        'MATCH (n3)<-[r2:TEST2]-(n1)-[r1:TEST1]->(n2) '
        'WITH n1, n2, n3, n1.id AS id1, n2.id AS id2, n3.id AS id3, '
        'type(r1) AS label1, type(r2) AS label2 '
        'RETURN id1, id2, id3, label1, label2',
    )
    assert (
        r[0]['id1'] == 'src'
        and r[0]['id2'] == 'tar1'
        and r[0]['id3'] == 'tar2'
        and r[0]['label1'] == 'TEST1'
        and r[0]['label2'] == 'TEST2'
    )


@pytest.mark.requires_neo4j
def test_add_biocypher_edge_generator(driver):
    # neo4j database needs to be running!
    n1 = Node('src', 'Test')
    n2 = Node('tar1', 'Test')
    n3 = Node('tar2', 'Test')
    driver.add_biocypher_nodes([n1, n2, n3])

    # generator
    def gen(edges):
        for e in edges:
            yield Edge(e.source, e.target, e.label)

    # edge list
    e1 = Edge('src', 'tar1', 'Test1')
    e2 = Edge('src', 'tar2', 'Test2')
    g = gen([e1, e2])

    driver.add_biocypher_edges(g)
    r, summary = driver.query(
        'MATCH (n3)<-[r2:TEST2]-(n1)-[r1:TEST1]->(n2) '
        'WITH n1, n2, n3, n1.id AS id1, n2.id AS id2, n3.id AS id3, '
        'type(r1) AS label1, type(r2) AS label2 '
        'RETURN id1, id2, id3, label1, label2',
    )
    assert (
        r[0]['id1'] == 'src'
        and r[0]['id2'] == 'tar1'
        and r[0]['id3'] == 'tar2'
        and r[0]['label1'] == 'TEST1'
        and r[0]['label2'] == 'TEST2'
    )


@pytest.mark.requires_neo4j
def test_add_biocypher_interaction_as_relasnode_list(driver):
    # neo4j database needs to be running!
    i1 = Node('int1', 'Int1')
    i2 = Node('int2', 'Int2')
    driver.add_biocypher_nodes([i1, i2])
    e1 = Edge('src', 'int1', 'IS_SOURCE_OF')
    e2 = Edge('tar', 'int1', 'IS_TARGET_OF')
    e3 = Edge('src', 'int2', 'IS_SOURCE_OF')
    e4 = Edge('tar', 'int2', 'IS_TARGET_OF')
    r1, r2 = RelAsNode(i1, e1, e2), RelAsNode(i2, e3, e4)
    driver.add_biocypher_edges([r1, r2])
    r, summary = driver.query(
        'MATCH (n2)-[e4:IS_TARGET_OF]->(i2:Int2)<-[e3:IS_SOURCE_OF]-'
        '(n1)-[e1:IS_SOURCE_OF]->(i1:Int1)<-[e2:IS_TARGET_OF]-(n2)'
        'WITH n1, n2, i1, i2, n1.id AS id1, n2.id AS id2, '
        'i1.id AS id3, i2.id AS id4, '
        'type(e1) AS label1, type(e2) AS label2, '
        'type(e3) AS label3, type(e4) AS label4 '
        'RETURN id1, id2, id3, id4, label1, label2, label3, label4',
    )
    assert (
        r[0]['id1'] == 'src'
        and r[0]['id2'] == 'tar'
        and r[0]['id3'] == 'int1'
        and r[0]['id4'] == 'int2'
        and r[0]['label1'] == 'IS_SOURCE_OF'
        and r[0]['label2'] == 'IS_TARGET_OF'
        and r[0]['label3'] == 'IS_SOURCE_OF'
        and r[0]['label4'] == 'IS_TARGET_OF'
    )


@pytest.mark.requires_neo4j
def test_add_biocypher_interaction_as_RelAsNode_generator(driver):
    # neo4j database needs to be running!
    i1 = Node('int1', 'Int1')
    i2 = Node('int2', 'Int2')
    driver.add_biocypher_nodes([i1, i2])
    e1 = Edge('src', 'int1', 'IS_SOURCE_OF')
    e2 = Edge('tar', 'int1', 'IS_TARGET_OF')
    e3 = Edge('src', 'int2', 'IS_SOURCE_OF')
    e4 = Edge('tar', 'int2', 'IS_TARGET_OF')
    r1, r2 = RelAsNode(i1, e1, e2), RelAsNode(i2, e3, e4)
    relasnode_list = [r1, r2]

    def gen(lis):
        yield from lis

    driver.add_biocypher_edges(gen(relasnode_list))
    r, summary = driver.query(
        'MATCH (n2)-[e4:IS_TARGET_OF]->(i2:Int2)<-[e3:IS_SOURCE_OF]-'
        '(n1)-[e1:IS_SOURCE_OF]->(i1:Int1)<-[e2:IS_TARGET_OF]-(n2)'
        'WITH n1, n2, i1, i2, n1.id AS id1, n2.id AS id2, '
        'i1.id AS id3, i2.id AS id4, '
        'type(e1) AS label1, type(e2) AS label2, '
        'type(e3) AS label3, type(e4) AS label4 '
        'RETURN id1, id2, id3, id4, label1, label2, label3, label4',
    )
    assert (
        r[0]['id1'] == 'src'
        and r[0]['id2'] == 'tar'
        and r[0]['id3'] == 'int1'
        and r[0]['id4'] == 'int2'
        and r[0]['label1'] == 'IS_SOURCE_OF'
        and r[0]['label2'] == 'IS_TARGET_OF'
        and r[0]['label3'] == 'IS_SOURCE_OF'
        and r[0]['label4'] == 'IS_TARGET_OF'
    )


@pytest.mark.requires_neo4j
def test_pretty_profile(driver):
    prof, printout = driver.profile(
        'UNWIND [1,2,3,4,5] as id '
        'MERGE (n:Test {id: id}) '
        'MERGE (x:Test {id: id + 1})',
    )

    assert 'args' in prof and 'ProduceResults' in printout[1]


@pytest.mark.requires_neo4j
def test_pretty_explain(driver):
    plan, printout = driver.explain(
        'UNWIND [1,2,3,4,5] as id '
        'MERGE (n:Test {id: id}) '
        'MERGE (x:Test {id: id + 1})',
    )

    assert 'args' in plan and 'ProduceResults' in printout[0]


@pytest.mark.requires_neo4j
def test_access_translate(driver):

    driver.start_ontology_adapter()

    assert driver.translate_term('mirna') == 'MicroRNA'

    assert (driver.reverse_translate_term('SideEffect') == 'sider')
    assert (
        _misc.first(driver.translate_query('MATCH (n:reactome) RETURN n'))
        == 'MATCH (n:Reactome.pathway) RETURN n'
    )
    assert (
        _misc.first(
            driver.reverse_translate_query(
                'MATCH (n:Wikipathways.pathway) RETURN n',
            ),
        )
        == 'MATCH (n:wikipathways) RETURN n'
    )


def test_log_missing_ontology_classes(driver):

    driver.translator.notype = {}
    assert not driver.log_missing_ontology_classes()

    driver.translator.notype = {'a': 1, 'b': 2}
    mt = driver.log_missing_ontology_classes()

    assert mt['a'] == 1 and mt['b'] == 2


def test_treelib_vis(driver):

    pass


@pytest.mark.inject_driver_args(
    driver_args = {
        'offline': True,
        'user_schema_config_path': (
            'https://raw.githubusercontent.com/saezlab/BioCypher/'
            'main/biocypher/_config/test_schema_config.yaml'
        ),
    },
)
def test_schema_config_from_web(driver):

    assert driver.translator._ontology_mapping
