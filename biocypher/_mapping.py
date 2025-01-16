"""
BioCypher 'mapping' module. Handles the mapping of user-defined schema to the
underlying ontology.
"""

from typing import Optional
from urllib.request import urlopen

import yaml

from . import _misc
from ._logger import logger

logger.debug(f"Loading module {__name__}.")


class OntologyMapping:
    """
    Class to store the ontology mapping and extensions.
    """

    def __init__(self, config_file: str = None):
        self.schema = self._read_config(config_file)

        self.extended_schema = self._extend_schema()

    def _read_config(self, config_file: str = None):
        """
        Read the configuration file and store the ontology mapping and extensions.
        """
        if config_file is None:
            schema_config = {}

        # load yaml file from web
        elif config_file.startswith("http"):
            with urlopen(config_file) as f:
                schema_config = yaml.safe_load(f)

        # get graph state from config (assume file is local)
        else:
            with open(config_file, "r") as f:
                schema_config = yaml.safe_load(f)

        return schema_config

    def _extend_schema(self, d: Optional[dict] = None) -> dict:
        """
        Get leaves of the tree hierarchy from the data structure dict
        contained in the `schema_config.yaml`. Creates virtual leaves
        (as children) from entries that provide more than one preferred
        id type (and corresponding inputs).

        Args:
            d:
                Data structure dict from yaml file.

        """

        d = d or self.schema

        extended_schema = dict()

        # first pass: get parent leaves with direct representation in ontology
        for k, v in d.items():
            # k is not an entity
            if "represented_as" not in v:
                continue

            # preferred_id optional: if not provided, use `id`
            if not v.get("preferred_id"):
                v["preferred_id"] = "id"

            # k is an entity that is present in the ontology
            if "is_a" not in v:
                extended_schema[k] = v

        # second pass: "vertical" inheritance
        d = self._vertical_property_inheritance(d)
        for k, v in d.items():
            if "is_a" in v:
                # prevent loops
                if k == v["is_a"]:
                    logger.warning(
                        f"Loop detected in ontology mapping: {k} -> {v}. "
                        "Removing item. Please fix the inheritance if you want "
                        "to use this item."
                    )
                    continue

                extended_schema[k] = v

        # "horizontal" inheritance: create siblings for multiple identifiers or
        # sources -> virtual leaves or implicit children
        mi_leaves = {}
        ms_leaves = {}
        for k, v in d.items():
            # k is not an entity
            if "represented_as" not in v:
                continue

            if isinstance(v.get("preferred_id"), list):
                mi_leaves = self._horizontal_inheritance_pid(k, v)
                extended_schema.update(mi_leaves)

            elif isinstance(v.get("source"), list):
                ms_leaves = self._horizontal_inheritance_source(k, v)
                extended_schema.update(ms_leaves)

        return extended_schema

    def _vertical_property_inheritance(self, d):
        """
        Inherit properties from parents to children and update `d` accordingly.
        """
        for k, v in d.items():
            # k is not an entity
            if "represented_as" not in v:
                continue

            # k is an entity that is present in the ontology
            if "is_a" not in v:
                continue

            # "vertical" inheritance: inherit properties from parent
            if v.get("inherit_properties", False):
                # get direct ancestor
                if isinstance(v["is_a"], list):
                    parent = v["is_a"][0]
                else:
                    parent = v["is_a"]

                # ensure child has properties and exclude_properties
                if "properties" not in v:
                    v["properties"] = {}
                if "exclude_properties" not in v:
                    v["exclude_properties"] = {}

                # update properties of child
                parent_props = self.schema[parent].get("properties", {})
                if parent_props:
                    v["properties"].update(parent_props)

                parent_excl_props = self.schema[parent].get("exclude_properties", {})
                if parent_excl_props:
                    v["exclude_properties"].update(parent_excl_props)

                # update schema (d)
                d[k] = v

        return d

    def _horizontal_inheritance_pid(self, key, value):
        """
        Create virtual leaves for multiple preferred id types or sources.

        If we create virtual leaves, input_label/label_in_input always has to be
        a list.
        """

        leaves = {}

        preferred_id = value["preferred_id"]
        input_label = value.get("input_label") or value["label_in_input"]
        represented_as = value["represented_as"]

        # adjust lengths
        max_l = max(
            [
                len(_misc.to_list(preferred_id)),
                len(_misc.to_list(input_label)),
                len(_misc.to_list(represented_as)),
            ],
        )

        # adjust pid length if necessary
        if isinstance(preferred_id, str):
            pids = [preferred_id] * max_l
        else:
            pids = preferred_id

        # adjust rep length if necessary
        if isinstance(represented_as, str):
            reps = [represented_as] * max_l
        else:
            reps = represented_as

        for pid, lab, rep in zip(pids, input_label, reps):
            skey = pid + "." + key
            svalue = {
                "preferred_id": pid,
                "input_label": lab,
                "represented_as": rep,
                # mark as virtual
                "virtual": True,
            }

            # inherit is_a if exists
            if "is_a" in value.keys():
                # treat as multiple inheritance
                if isinstance(value["is_a"], list):
                    v = list(value["is_a"])
                    v.insert(0, key)
                    svalue["is_a"] = v

                else:
                    svalue["is_a"] = [key, value["is_a"]]

            else:
                # set parent as is_a
                svalue["is_a"] = key

            # inherit everything except core attributes
            for k, v in value.items():
                if k not in [
                    "is_a",
                    "preferred_id",
                    "input_label",
                    "label_in_input",
                    "represented_as",
                ]:
                    svalue[k] = v

            leaves[skey] = svalue

        return leaves

    def _horizontal_inheritance_source(self, key, value):
        """
        Create virtual leaves for multiple sources.

        If we create virtual leaves, input_label/label_in_input always has to be
        a list.
        """

        leaves = {}

        source = value["source"]
        input_label = value.get("input_label") or value["label_in_input"]
        represented_as = value["represented_as"]

        # adjust lengths
        src_l = len(source)

        # adjust label length if necessary
        if isinstance(input_label, str):
            labels = [input_label] * src_l
        else:
            labels = input_label

        # adjust rep length if necessary
        if isinstance(represented_as, str):
            reps = [represented_as] * src_l
        else:
            reps = represented_as

        for src, lab, rep in zip(source, labels, reps):
            skey = src + "." + key
            svalue = {
                "source": src,
                "input_label": lab,
                "represented_as": rep,
                # mark as virtual
                "virtual": True,
            }

            # inherit is_a if exists
            if "is_a" in value.keys():
                # treat as multiple inheritance
                if isinstance(value["is_a"], list):
                    v = list(value["is_a"])
                    v.insert(0, key)
                    svalue["is_a"] = v

                else:
                    svalue["is_a"] = [key, value["is_a"]]

            else:
                # set parent as is_a
                svalue["is_a"] = key

            # inherit everything except core attributes
            for k, v in value.items():
                if k not in [
                    "is_a",
                    "source",
                    "input_label",
                    "label_in_input",
                    "represented_as",
                ]:
                    svalue[k] = v

            leaves[skey] = svalue

        return leaves
