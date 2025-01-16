import os
import subprocess

import pytest

from biocypher import config as bcy_config
from biocypher.output.write.relational._postgresql import _PostgreSQLBatchWriter


@pytest.fixture(scope="session")
def postgresql_param(request):
    keys = (
        "user_postgresql",
        "password_postgresql",
        "host_postgresql",
        "port_postgresql",
    )

    # get fallback parameters from biocypher config
    param = bcy_config("postgresql")
    cli = {}
    for key in keys:
        # remove '_postgresql' suffix
        key_short = key[:-11]
        # change into format of input parameters
        cli[f"db_{key_short}"] = request.config.getoption(f"--{key}") or param[key_short]

    # hardcoded string for test-db name. test-db will be created for testing and
    # droped after testing.  Do not take db_name from config to avoid accidental
    # testing on the production database

    cli["db_name"] = request.config.getoption("--database_name_postgresql") or "postgresql-biocypher-test-TG2C7GsdNw"

    return cli


@pytest.fixture(autouse=True)
def skip_if_offline_postgresql(request, postgresql_param):
    marker = request.node.get_closest_marker("requires_postgresql")

    if marker:
        params = postgresql_param
        user, host, port, password = (
            params["db_user"],
            params["db_host"],
            params["db_port"],
            params["db_password"],
        )

        # an empty command, just to test if connection is possible
        command = f"PGPASSWORD={password} psql -c '' --host {host} " f"--port {port} --user {user}"
        process = subprocess.run(command, shell=True)

        # returncode is 0 when success
        if process.returncode != 0:
            pytest.skip("Requires psql and connection to Postgresql server.")


@pytest.fixture(scope="function")
def bw_comma_postgresql(postgresql_param, translator, deduplicator, tmp_path_session):
    bw_comma = _PostgreSQLBatchWriter(
        translator=translator,
        deduplicator=deduplicator,
        output_directory=tmp_path_session,
        delimiter=",",
        **postgresql_param,
    )

    yield bw_comma

    # teardown
    for f in os.listdir(tmp_path_session):
        os.remove(os.path.join(tmp_path_session, f))


@pytest.fixture(scope="function")
def bw_tab_postgresql(postgresql_param, translator, deduplicator, tmp_path_session):
    bw_tab = _PostgreSQLBatchWriter(
        translator=translator,
        deduplicator=deduplicator,
        output_directory=tmp_path_session,
        delimiter="\\t",
        **postgresql_param,
    )

    yield bw_tab

    # teardown
    for f in os.listdir(tmp_path_session):
        os.remove(os.path.join(tmp_path_session, f))


@pytest.fixture(scope="session")
def create_database_postgres(postgresql_param):
    params = postgresql_param
    dbname, user, host, port, password = (
        params["db_name"],
        params["db_user"],
        params["db_host"],
        params["db_port"],
        params["db_password"],
    )

    # create the database
    command = (
        f"PGPASSWORD={password} psql -c 'CREATE DATABASE \"{dbname}\";' " f"--host {host} --port {port} --user {user}"
    )
    process = subprocess.run(command, shell=True)

    # 0 if success
    yield dbname, user, host, port, password, process.returncode == 0

    # teardown
    command = (
        f"PGPASSWORD={password} psql -c 'DROP DATABASE \"{dbname}\";' " f"--host {host} --port {port} --user {user}"
    )
    process = subprocess.run(command, shell=True)
