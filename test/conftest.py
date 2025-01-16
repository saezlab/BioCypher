import os
import shutil

from glob import glob

import pytest

from biocypher._deduplicate import Deduplicator
from biocypher._translate import Translator

# load all fixtures from the fixtures directory
pytest_plugins = [
    fixture_file.replace(os.sep, ".").replace(".py", "")
    for fixture_file in glob(f"test{os.sep}fixtures{os.sep}[!__]*.py", recursive=True)
]


# CLI option parser
def pytest_addoption(parser):
    options = (
        # neo4j
        ("database_name", "The Neo4j database to be used for tests."),
        ("user", "Tests access Neo4j as this user."),
        ("password", "Password to access Neo4j."),
        ("uri", "URI of the Neo4j server."),
        # postgresl
        (
            "database_name_postgresql",
            "The PostgreSQL database to be used for tests. Defaults to " '"postgresql-biocypher-test-TG2C7GsdNw".',
        ),
        ("user_postgresql", "Tests access PostgreSQL as this user."),
        ("password_postgresql", "Password to access PostgreSQL."),
        ("host_postgresql", "Host of the PostgreSQL server."),
        ("port_postgresql", "Port of the PostgreSQL server."),
    )

    for name, help_ in options:
        parser.addoption(
            f"--{name}",
            action="store",
            default=None,
            help=help_,
        )


@pytest.fixture(scope="session")
def tmp_path_session(tmp_path_factory):
    """
    Create a session-scoped temporary directory.

    Args:
        tmp_path_factory: The built-in pytest fixture.

    Returns:
        pathlib.Path: The path to the temporary directory.
    """
    return tmp_path_factory.mktemp("data")


@pytest.fixture(scope="session", autouse=True)
def cleanup(request, tmp_path_session):
    """
    Teardown function to delete the session-scoped temporary directory.

    Args:
        request: The pytest request object.
        tmp_path_session: The session-scoped temporary directory.
    """

    def remove_tmp_dir():
        shutil.rmtree(tmp_path_session)

    request.addfinalizer(remove_tmp_dir)


@pytest.fixture(scope="function")
def deduplicator():
    return Deduplicator()


@pytest.fixture(scope="module")
def translator(hybrid_ontology):
    return Translator(hybrid_ontology)
