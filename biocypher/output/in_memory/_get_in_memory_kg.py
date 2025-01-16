"""
BioCypher 'in_memory' module. Handles the in-memory Knowledge Graph instance.
"""

from biocypher._deduplicate import Deduplicator
from biocypher._logger import logger
from biocypher.output.in_memory._networkx import NetworkxKG
from biocypher.output.in_memory._pandas import PandasKG

logger.debug(f"Loading module {__name__}.")

__all__ = ["get_in_memory_kg"]

IN_MEMORY_DBMS = ["csv", "pandas", "tabular", "networkx"]


def get_in_memory_kg(
    dbms: str,
    deduplicator: Deduplicator,
):
    """
    Function to return the in-memory KG class.

    Returns:
        class: the in-memory KG class
    """
    if dbms in ["csv", "pandas", "tabular"]:
        return PandasKG(deduplicator)
    elif dbms == "networkx":
        return NetworkxKG(deduplicator)
    else:
        raise NotImplementedError(
            f"Getting the in memory BioCypher KG is not supported for the DBMS {dbms}. Supported: {IN_MEMORY_DBMS}."
        )
