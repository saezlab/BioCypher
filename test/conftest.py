import pytest

from biocypher import Driver
from biocypher import config as bcy_config
from biocypher._config import neo4j_config

__all__ = ['create_driver', 'neo4j_param', 'pytest_addoption', 'skip_if_offline']


def pytest_addoption(parser):

    options = (
        ('neo4j_db', 'The Neo4j database to be used for tests.'),
        ('neo4j_user', 'Tests access Neo4j as this user.'),
        ('neo4j_pw', 'Password to access Neo4j.'),
        ('neo4j_uri', 'URI of the Neo4j server.'),
    )

    for name, help_ in options:

        parser.addoption(
            f'--{name}',
            action = 'store',
            default = None,
            help = help_,
        )


@pytest.fixture(scope = 'session')
def neo4j_param(request):

    keys = (
        'neo4j_db',
        'neo4j_user',
        'neo4j_pw',
        'neo4j_uri',
    )

    param = {
        key: request.config.getoption(f'--{key}') or bcy_config(key)
        for key in keys
    }

    return neo4j_config(config = param)


@pytest.fixture(name = 'driver', scope = 'session')
def create_driver(request, neo4j_param):

    marker = request.node.get_closest_marker('inject_driver_args')

    marker_args = {}
    # check if marker has attribute param
    if marker and hasattr(marker, 'param'):
        marker_args = marker.param

    driver_args = {
        'wipe': True,
        'increment_version': False,
        'user_schema_config_path': 'biocypher/_config/test_schema_config.yaml',
        'clear_cache': True,
    }
    driver_args.update(marker_args)
    driver_args.update(neo4j_param)

    d = Driver(**driver_args)

    yield d

    # teardown
    d.query('MATCH (n:Test)' 'DETACH DELETE n')
    d.query('MATCH (n:Int1)' 'DETACH DELETE n')
    d.query('MATCH (n:Int2)' 'DETACH DELETE n')

    # to deal with merging on non-existing nodes
    # see test_add_single_biocypher_edge_missing_nodes()
    d.query("MATCH (n2) WHERE n2.id = 'src'" 'DETACH DELETE n2')
    d.query("MATCH (n3) WHERE n3.id = 'tar'" 'DETACH DELETE n3')
    d.close()


@pytest.fixture(autouse = True)
def skip_if_offline(request, driver):

    marker = request.node.get_closest_marker('requires_neo4j')

    if marker and driver.status != 'db online':

        pytest.skip('Requires connection to Neo4j server.')
