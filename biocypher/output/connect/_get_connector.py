"""BioCypher 'connect' module.

Handles the connecting and writing a Knowledge Graph to a database.
"""

from biocypher._config import config as _config
from biocypher._logger import logger
from biocypher._translate import Translator
from biocypher.output.connect._neo4j_driver import _Neo4jDriver

logger.debug(f"Loading module {__name__}.")

__all__ = ["get_connector"]


def get_connector(
    dbms: str,
    translator: Translator,
):
    """
    Function to return the connector class.

    Returns:
        class: the connector class
    """

    dbms_config = _config(dbms)

    if dbms == "neo4j":
        return _Neo4jDriver(
            database_name=dbms_config["database_name"],
            wipe=dbms_config["wipe"],
            uri=dbms_config["uri"],
            user=dbms_config["user"],
            password=dbms_config["password"],
            multi_db=dbms_config["multi_db"],
            translator=translator,
        )
    else:
        raise NotImplementedError(f"Online mode is not supported for the DBMS {dbms}.")
