import os

import pytest

from neo4j.exceptions import ServiceUnavailable

from biocypher import config as bcy_config
from biocypher.output.connect._neo4j_driver import _Neo4jDriver
from biocypher.output.write.graph._neo4j import _Neo4jBatchWriter


@pytest.fixture(scope="function")
def bw(translator, deduplicator, tmp_path_session):
    bw = _Neo4jBatchWriter(
        translator=translator,
        deduplicator=deduplicator,
        output_directory=tmp_path_session,
        delimiter=";",
        array_delimiter="|",
        quote="'",
    )

    yield bw

    # teardown
    for f in os.listdir(tmp_path_session):
        os.remove(os.path.join(tmp_path_session, f))


@pytest.fixture(scope="function")
def bw_tab(translator, deduplicator, tmp_path_session):
    bw_tab = _Neo4jBatchWriter(
        translator=translator,
        deduplicator=deduplicator,
        output_directory=tmp_path_session,
        delimiter="\\t",
        array_delimiter="|",
        quote="'",
    )

    yield bw_tab

    # teardown
    for f in os.listdir(tmp_path_session):
        os.remove(os.path.join(tmp_path_session, f))


@pytest.fixture(scope="function")
def bw_strict(translator, deduplicator, tmp_path_session):
    bw = _Neo4jBatchWriter(
        translator=translator,
        deduplicator=deduplicator,
        output_directory=tmp_path_session,
        delimiter=";",
        array_delimiter="|",
        quote="'",
        strict_mode=True,
    )

    yield bw

    # teardown
    for f in os.listdir(tmp_path_session):
        os.remove(os.path.join(tmp_path_session, f))


@pytest.fixture(scope="session")
def neo4j_param(request):
    keys = (
        "database_name",
        "user",
        "password",
        "uri",
    )

    param = bcy_config("neo4j")

    cli = {key: request.config.getoption(f"--{key}") or param[key] for key in keys}

    return cli


@pytest.fixture(autouse=True)
def skip_if_offline_neo4j(request, neo4j_param, translator):
    marker = request.node.get_closest_marker("requires_neo4j")

    if marker:
        try:
            marker_args = {}
            # check if marker has attribute param
            if marker and hasattr(marker, "param"):
                marker_args = marker.param

            driver_args = {
                "wipe": True,
                "multi_db": True,
                "translator": translator,
            }
            driver_args.update(marker_args)
            driver_args.update(neo4j_param)

            driver_args["database_name"] = "test"

            _Neo4jDriver(**driver_args)

        except ServiceUnavailable as e:
            pytest.skip(f"Neo4j is offline: {e}")


@pytest.fixture(name="driver", scope="function")
def create_driver(request, neo4j_param, translator):
    marker = None  # request.node.get_closest_marker('inject_driver_args')

    marker_args = {}
    # check if marker has attribute param
    if marker and hasattr(marker, "param"):
        marker_args = marker.param

    if not marker_args and "DRIVER" in globals():
        d = globals()["DRIVER"]

    else:
        driver_args = {
            "wipe": True,
            "multi_db": True,
            "translator": translator,
        }
        driver_args.update(marker_args)
        driver_args.update(neo4j_param)

        driver_args["database_name"] = "test"

        d = _Neo4jDriver(**driver_args)

        if not marker_args:
            globals()["DRIVER"] = d

    yield d

    # teardown
    d._driver.query("MATCH (n:Test)DETACH DELETE n")
    d._driver.query("MATCH (n:Int1)DETACH DELETE n")
    d._driver.query("MATCH (n:Int2)DETACH DELETE n")

    # to deal with merging on non-existing nodes
    # see test_add_single_biocypher_edge_missing_nodes()
    d._driver.query("MATCH (n2) WHERE n2.id = 'src'DETACH DELETE n2")
    d._driver.query("MATCH (n3) WHERE n3.id = 'tar'DETACH DELETE n3")
    d._driver.close()
