"""
BioCypher 'translation' module. Responsible for translating between the raw
input data and the BioCypherNode and BioCypherEdge objects.
"""

from collections.abc import Generator, Iterable
from typing import Any, Optional, Union

from more_itertools import peekable

from . import _misc
from ._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from ._logger import logger
from ._ontology import Ontology

logger.debug(f"Loading module {__name__}.")

__all__ = ["Translator"]


class Translator:
    """
    Class responsible for exacting the translation process that is configured in
    the schema_config.yaml file. Creates a mapping dictionary from that file,
    and, given nodes and edges, translates them into BioCypherNodes and
    BioCypherEdges. During this process, can also filter the properties of the
    entities if the schema_config.yaml file specifies a property whitelist or
    blacklist.

    Provides utility functions for translating between input and output labels
    and cypher queries.
    """

    def __init__(self, ontology: "Ontology", strict_mode: bool = False):
        """
        Args:
            leaves:
                Dictionary detailing the leaves of the hierarchy
                tree representing the structure of the graph; the leaves are
                the entities that will be direct components of the graph,
                while the intermediary nodes are additional labels for
                filtering purposes.
            strict_mode:
                If True, the translator will raise an error if input data do not
                carry source, licence, and version information.
        """

        self.ontology = ontology
        self.strict_mode = strict_mode

        # record nodes without biolink type configured in schema_config.yaml
        self.notype = {}

        # mapping functionality for translating terms and queries
        self.mappings = {}
        self.reverse_mappings = {}

        self._update_ontology_types()

    def translate_entities(self, entities):
        entities = peekable(entities)
        if (
            isinstance(entities.peek(), BioCypherNode)
            or isinstance(entities.peek(), BioCypherEdge)
            or isinstance(entities.peek(), BioCypherRelAsNode)
        ):
            translated_entities = entities
        elif len(entities.peek()) < 4:
            translated_entities = self.translate_nodes(entities)
        else:
            translated_entities = self.translate_edges(entities)
        return translated_entities

    def translate_nodes(
        self,
        node_tuples: Iterable,
    ) -> Generator[BioCypherNode, None, None]:
        """
        Translates input node representation to a representation that
        conforms to the schema of the given BioCypher graph. For now
        requires explicit statement of node type on pass.

        Args:
            node_tuples (list of tuples): collection of tuples
                representing individual nodes by their unique id and a type
                that is translated from the original database notation to
                the corresponding BioCypher notation.

        """

        self._log_begin_translate(node_tuples, "nodes")

        for _id, _type, _props in node_tuples:
            # check for strict mode requirements
            required_props = ["source", "licence", "version"]

            if self.strict_mode:
                # rename 'license' to 'licence' in _props
                if _props.get("license"):
                    _props["licence"] = _props.pop("license")

                for prop in required_props:
                    if prop not in _props:
                        raise ValueError(
                            f"Property `{prop}` missing from node {_id}. "
                            "Strict mode is enabled, so this is not allowed."
                        )

            # find the node in leaves that represents ontology node type
            _ontology_class = self._get_ontology_mapping(_type)

            if _ontology_class:
                # filter properties for those specified in schema_config if any
                _filtered_props = self._filter_props(_ontology_class, _props)

                # preferred id
                _preferred_id = self._get_preferred_id(_ontology_class)

                yield BioCypherNode(
                    node_id=_id,
                    node_label=_ontology_class,
                    preferred_id=_preferred_id,
                    properties=_filtered_props,
                )

            else:
                self._record_no_type(_type, _id)

        self._log_finish_translate("nodes")

    def _get_preferred_id(self, _bl_type: str) -> str:
        """
        Returns the preferred id for the given Biolink type.
        """

        return (
            self.ontology.mapping.extended_schema[_bl_type]["preferred_id"]
            if "preferred_id" in self.ontology.mapping.extended_schema.get(_bl_type, {})
            else "id"
        )

    def _filter_props(self, bl_type: str, props: dict) -> dict:
        """
        Filters properties for those specified in schema_config if any.
        """

        filter_props = self.ontology.mapping.extended_schema[bl_type].get("properties", {})

        # strict mode: add required properties (only if there is a whitelist)
        if self.strict_mode and filter_props:
            filter_props.update(
                {"source": "str", "licence": "str", "version": "str"},
            )

        exclude_props = self.ontology.mapping.extended_schema[bl_type].get("exclude_properties", [])

        if isinstance(exclude_props, str):
            exclude_props = [exclude_props]

        if filter_props and exclude_props:
            filtered_props = {k: v for k, v in props.items() if (k in filter_props.keys() and k not in exclude_props)}

        elif filter_props:
            filtered_props = {k: v for k, v in props.items() if k in filter_props.keys()}

        elif exclude_props:
            filtered_props = {k: v for k, v in props.items() if k not in exclude_props}

        else:
            return props

        missing_props = [k for k in filter_props.keys() if k not in filtered_props.keys()]
        # add missing properties with default values
        for k in missing_props:
            filtered_props[k] = None

        return filtered_props

    def translate_edges(
        self,
        edge_tuples: Iterable,
    ) -> Generator[Union[BioCypherEdge, BioCypherRelAsNode], None, None]:
        """
        Translates input edge representation to a representation that
        conforms to the schema of the given BioCypher graph. For now
        requires explicit statement of edge type on pass.

        Args:

            edge_tuples (list of tuples):

                collection of tuples representing source and target of
                an interaction via their unique ids as well as the type
                of interaction in the original database notation, which
                is translated to BioCypher notation using the `leaves`.
                Can optionally possess its own ID.
        """

        self._log_begin_translate(edge_tuples, "edges")

        # legacy: deal with 4-tuples (no edge id)
        # TODO remove for performance reasons once safe
        edge_tuples = peekable(edge_tuples)
        if len(edge_tuples.peek()) == 4:
            edge_tuples = [(None, src, tar, typ, props) for src, tar, typ, props in edge_tuples]

        for _id, _src, _tar, _type, _props in edge_tuples:
            # check for strict mode requirements
            if self.strict_mode:
                if "source" not in _props:
                    raise ValueError(
                        f"Edge {_id if _id else (_src, _tar)} does not have a `source` property.",
                        " This is required in strict mode.",
                    )
                if "licence" not in _props:
                    raise ValueError(
                        f"Edge {_id if _id else (_src, _tar)} does not have a `licence` property.",
                        " This is required in strict mode.",
                    )

            # match the input label (_type) to
            # a Biolink label from schema_config
            bl_type = self._get_ontology_mapping(_type)

            if bl_type:
                # filter properties for those specified in schema_config if any
                _filtered_props = self._filter_props(bl_type, _props)

                rep = self.ontology.mapping.extended_schema[bl_type]["represented_as"]

                if rep == "node":
                    if _id:
                        # if it brings its own ID, use it
                        node_id = _id

                    else:
                        # source target concat
                        node_id = str(_src) + "_" + str(_tar) + "_" + "_".join(str(v) for v in _filtered_props.values())

                    n = BioCypherNode(
                        node_id=node_id,
                        node_label=bl_type,
                        properties=_filtered_props,
                    )

                    # directionality check TODO generalise to account for
                    # different descriptions of directionality or find a
                    # more consistent solution for indicating directionality
                    if _filtered_props.get("directed") == True:  # noqa: E712 (seems to not work without '== True')
                        l1 = "IS_SOURCE_OF"
                        l2 = "IS_TARGET_OF"

                    elif _filtered_props.get(
                        "src_role",
                    ) and _filtered_props.get("tar_role"):
                        l1 = _filtered_props.get("src_role")
                        l2 = _filtered_props.get("tar_role")

                    else:
                        l1 = l2 = "IS_PART_OF"

                    e_s = BioCypherEdge(
                        source_id=_src,
                        target_id=node_id,
                        relationship_label=l1,
                        # additional here
                    )

                    e_t = BioCypherEdge(
                        source_id=_tar,
                        target_id=node_id,
                        relationship_label=l2,
                        # additional here
                    )

                    yield BioCypherRelAsNode(n, e_s, e_t)

                else:
                    edge_label = self.ontology.mapping.extended_schema[bl_type].get("label_as_edge")

                    if edge_label is None:
                        edge_label = bl_type

                    yield BioCypherEdge(
                        relationship_id=_id,
                        source_id=_src,
                        target_id=_tar,
                        relationship_label=edge_label,
                        properties=_filtered_props,
                    )

            else:
                self._record_no_type(_type, (_src, _tar))

        self._log_finish_translate("edges")

    def _record_no_type(self, _type: Any, what: Any) -> None:
        """
        Records the type of a node or edge that is not represented in the
        schema_config.
        """

        logger.debug(f"No ontology type defined for `{_type}`: {what}")

        if self.notype.get(_type, None):
            self.notype[_type] += 1

        else:
            self.notype[_type] = 1

    def get_missing_biolink_types(self) -> dict:
        """
        Returns a dictionary of types that were not represented in the
        schema_config.
        """

        return self.notype

    @staticmethod
    def _log_begin_translate(_input: Iterable, what: str):
        n = f"{len(_input)} " if hasattr(_input, "__len__") else ""

        logger.debug(f"Translating {n}{what} to BioCypher")

    @staticmethod
    def _log_finish_translate(what: str):
        logger.debug(f"Finished translating {what} to BioCypher.")

    def _update_ontology_types(self):
        """
        Creates a dictionary to translate from input labels to ontology labels.

        If multiple input labels, creates mapping for each.
        """

        self._ontology_mapping = {}

        for key, value in self.ontology.mapping.extended_schema.items():
            labels = value.get("input_label") or value.get("label_in_input")

            if isinstance(labels, str):
                self._ontology_mapping[labels] = key

            elif isinstance(labels, list):
                for label in labels:
                    self._ontology_mapping[label] = key

            if value.get("label_as_edge"):
                self._add_translation_mappings(labels, value["label_as_edge"])

            else:
                self._add_translation_mappings(labels, key)

    def _get_ontology_mapping(self, label: str) -> Optional[str]:
        """
        For each given input type ("input_label" or "label_in_input"), find the
        corresponding ontology class in the leaves dictionary (from the
        `schema_config.yam`).

        Args:
            label:
                The input type to find (`input_label` or `label_in_input` in
                `schema_config.yaml`).
        """

        # commented out until behaviour of _update_bl_types is fixed
        return self._ontology_mapping.get(label, None)

    def translate_term(self, term):
        """
        Translate a single term.
        """

        return self.mappings.get(term, None)

    def reverse_translate_term(self, term):
        """
        Reverse translate a single term.
        """

        return self.reverse_mappings.get(term, None)

    def translate(self, query):
        """
        Translate a cypher query. Only translates labels as of now.
        """
        for key in self.mappings:
            query = query.replace(":" + key, ":" + self.mappings[key])
        return query

    def reverse_translate(self, query):
        """
        Reverse translate a cypher query. Only translates labels as of
        now.
        """
        for key in self.reverse_mappings:
            a = ":" + key + ")"
            b = ":" + key + "]"
            # TODO this conditional probably does not cover all cases
            if a in query or b in query:
                if isinstance(self.reverse_mappings[key], list):
                    raise NotImplementedError(
                        "Reverse translation of multiple inputs not "
                        "implemented yet. Many-to-one mappings are "
                        "not reversible. "
                        f"({key} -> {self.reverse_mappings[key]})",
                    )
                else:
                    query = query.replace(
                        a,
                        ":" + self.reverse_mappings[key] + ")",
                    ).replace(b, ":" + self.reverse_mappings[key] + "]")
        return query

    def _add_translation_mappings(self, original_name, biocypher_name):
        """
        Add translation mappings for a label and name. We use here the
        PascalCase version of the BioCypher name, since sentence case is
        not useful for Cypher queries.
        """
        if isinstance(original_name, list):
            for on in original_name:
                self.mappings[on] = self.name_sentence_to_pascal(
                    biocypher_name,
                )
        else:
            self.mappings[original_name] = self.name_sentence_to_pascal(
                biocypher_name,
            )

        if isinstance(biocypher_name, list):
            for bn in biocypher_name:
                self.reverse_mappings[
                    self.name_sentence_to_pascal(
                        bn,
                    )
                ] = original_name
        else:
            self.reverse_mappings[
                self.name_sentence_to_pascal(
                    biocypher_name,
                )
            ] = original_name

    @staticmethod
    def name_sentence_to_pascal(name: str) -> str:
        """
        Converts a name in sentence case to pascal case.
        """
        # split on dots if dot is present
        if "." in name:
            return ".".join(
                [_misc.sentencecase_to_pascalcase(n) for n in name.split(".")],
            )
        else:
            return _misc.sentencecase_to_pascalcase(name)
