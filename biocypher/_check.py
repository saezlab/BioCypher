#!/usr/bin/env python

#
# Copyright 2021, Heidelberg University Clinic
#
# File author(s): Sebastian Lobentanzer
#                 ...
#
# Distributed under GPLv3 license, see the file `LICENSE`.
#

"""
Read and write BioCypher config of a Neo4j database instance.
Each BioCypher database contains a configuration encoded in the graph itself.
This configuration includes the version of the BioCypher standard, the
preferred identifier types, etc.

Todo:
    - connect graph structure setup (from config) with data parsing
    - when to do versioning?
        - setting in config file regarding the granularity?
"""

from ._logger import logger

logger.debug(f"Loading module {__name__}.")

from datetime import datetime
import yaml
import os

from . import _config as config
from ._create import BioCypherEdge, BioCypherNode

__all__ = ["MetaEdge", "MetaNode", "VersionNode"]


class MetaNode(BioCypherNode):
    """
    Graph structure information node representing node type entities in
    the BioCypher graph. Inherits from BioCypherNode but fixes label to
    ":MetaNode". Is connected to VersionNode via ":CONTAINS"
    relationship.
    """

    def __init__(
        self,
        node_id,
        node_label="MetaNode",
        optional_labels=None,
        **properties,
    ):
        super().__init__(node_id, node_label, optional_labels, **properties)


class MetaEdge(BioCypherEdge):
    """
    Graph structure information edge in the meta-graph. Inherits from
    BioCypherNode but fixes label to ":CONTAINS".
    """

    def __init__(
        self, source_id, target_id, relationship_label="CONTAINS", **properties
    ):
        super().__init__(
            source_id, target_id, relationship_label, **properties
        )


class VersionNode(BioCypherNode):
    """
    Versioning and graph structure information meta node. Inherits from
    BioCypherNode but fixes label to ":BioCypher" and sets version
    by using the current date and time (meaning it overrides both
    mandatory args from BioCypherNode).

    Is created upon establishment of connection with the database and
    remains fixed for each BioCypher "session" (ie, the entire duration
    from starting the connection to the termination of the BioCypher
    adapter instance). Is connected to MetaNodes and MetaEdges via
    ":CONTAINS" relationships.

    Todo:

        - granularity of versioning?
        - way to instantiate the MetaNode without having to give id and
          label?

            - can only think of creating a parent to both BioCypherNode
              and MetaNode that does not have mandatory id and label.

        - add graph structure information
        - on creation will be generated from yml or json?

            - yml is more readable
            - as dict? from yml/json?
    """

    def __init__(
        self,
        bcy_driver=None,
        node_id=None,
        node_label="BioCypher",
        from_config=False,
        config_file=None,
        offline=False,
        **properties,
    ):

        super().__init__(node_id, node_label, **properties)
        self.bcy_driver = bcy_driver
        self.node_id = self._get_current_id()
        self.node_label = node_label
        self.graph_state = self._get_graph_state() if not offline else None
        self.schema = self._get_graph_schema(
            from_config=from_config, config_file=config_file
        )
        self.leaves = self._get_leaves(self.schema)

    def _get_current_id(self):
        """
        Instantiate a version ID for the current session. For now does
        versioning using datetime.

        Can later implement incremental versioning, versioning from
        config file, or manual specification via argument.
        """

        now = datetime.now()
        return now.strftime("v%Y%m%d-%H%M%S")

    def _get_graph_state(self):
        """
        Check in active DBMS connection for existence of VersionNodes,
        return the most recent VersionNode as representation of the
        graph state. If no VersionNode found, assume blank graph state
        and initialise.
        """

        logger.info("Getting graph state.")

        result, summary = self.bcy_driver.query(
            "MATCH (meta:BioCypher)"
            "WHERE NOT (meta)-[:PRECEDES]->(:BioCypher)"
            "RETURN meta",
        )

        # if result is empty, initialise
        if not result:
            logger.info("No existing graph found, initialising.")
            return None
        # else, pass on graph state
        else:
            version = result[0]["meta"]["id"]
            logger.info(f"Found graph state at {version}.")
            return result[0]["meta"]

    def _get_graph_schema(self, from_config, config_file):
        """
        Return graph schema information from meta graph if it exists, or
        create new schema information properties from configuration
        file.

        Todo:
            - get schema from meta graph
        """
        if self.graph_state and not from_config:
            # TODO do we want information about actual structure here?
            res = self.bcy_driver.query(
                "MATCH (src:MetaNode) "
                # "OPTIONAL MATCH (src)-[r]->(tar)"
                "RETURN src",  # , type(r) AS type, tar"
            )
            gs_dict = {}
            for r in res[0]:
                src = r["src"]
                key = src.pop("id")
                gs_dict[key] = src

            return gs_dict

        else:
            # load default yaml from module
            # get graph state from config
            if config_file is not None:
                with open(config_file, "r") as f:
                    dataMap = yaml.safe_load(f)
            else:
                dataMap = config.module_data("schema_config")

            return dataMap

    def _get_leaves(self, d):
        """
        Get leaves of the tree hierarchy from the data structure dict
        contained in the `schema_config.yaml`. Creates virtual leaves
        (as children) from entries that provide more than one preferred
        id type (and corresponding inputs).

        Args:
            d (dict): data structure dict from yaml file

        TODO: allow multiple leaves with same Biolink name but different
        specs? (eg ProteinToDiseaseAssociation from two different
        entries in CKG, DETECTED_IN_PATHOLOGY_SAMPLE and ASSOCIATED_WITH)
        """

        leaves = dict()
        stack = list(d.items())
        visited = set()
        while stack:
            key, value = stack.pop()
            if isinstance(value, dict):
                if "represented_as" not in value.keys():
                    if key not in visited:
                        stack.extend(value.items())

                else:
                    if "preferred_id" in value.keys():
                        if isinstance(value["preferred_id"], list):
                            # create "virtual" leaves for each preferred
                            # id

                            # adjust lengths (if representation and/or id are
                            # not given as lists but inputs are multiple)
                            l = len(value["label_in_input"])
                            # adjust pid length if necessary
                            if isinstance(value["preferred_id"], str):
                                pids = [value["preferred_id"]] * l
                            else:
                                pids = value["preferred_id"]
                            # adjust rep length if necessary
                            if isinstance(value["represented_as"], str):
                                reps = [value["represented_as"]] * l
                            else:
                                reps = value["represented_as"]

                            for pid, label, rep in zip(
                                pids,
                                value["label_in_input"],
                                reps,
                            ):
                                skey = pid + "." + key
                                svalue = {
                                    "preferred_id": pid,
                                    "label_in_input": label,
                                    "represented_as": rep,
                                    # mark as virtual
                                    "virtual": True,
                                }
                                leaves[skey] = svalue
                    # add parent
                    leaves[key] = value
            visited.add(key)

        return leaves