import pandas as pd

from biocypher._create import BioCypherEdge, BioCypherNode, BioCypherRelAsNode
from biocypher.output.in_memory._in_memory_kg import _InMemoryKG


class PandasKG(_InMemoryKG):
    def __init__(self, deduplicator):
        super().__init__()  # keeping in spite of ABC not having __init__
        self.deduplicator = deduplicator

        self.dfs = {}

    def get_kg(self):
        return self.dfs

    def add_nodes(self, nodes):
        self.add_tables(nodes)

    def add_edges(self, edges):
        self.add_tables(edges)

    def _separate_entity_types(self, entities):
        """
        Given mixed iterable of BioCypher objects, separate them into lists by
        type. Also deduplicates using the `Deduplicator` instance.
        """
        lists = {}
        for entity in entities:
            if (
                not isinstance(entity, BioCypherNode)
                and not isinstance(entity, BioCypherEdge)
                and not isinstance(entity, BioCypherRelAsNode)
            ):
                raise TypeError(
                    "Expected a BioCypherNode / BioCypherEdge / " f"BioCypherRelAsNode, got {type(entity)}."
                )

            if isinstance(entity, BioCypherNode):
                seen = self.deduplicator.node_seen(entity)
            elif isinstance(entity, BioCypherEdge):
                seen = self.deduplicator.edge_seen(entity)
            elif isinstance(entity, BioCypherRelAsNode):
                seen = self.deduplicator.rel_as_node_seen(entity)

            if seen:
                continue

            if isinstance(entity, BioCypherRelAsNode):
                node = entity.get_node()
                source_edge = entity.get_source_edge()
                target_edge = entity.get_target_edge()

                _type = node.get_type()
                if _type not in lists:
                    lists[_type] = []
                lists[_type].append(node)

                _source_type = source_edge.get_type()
                if _source_type not in lists:
                    lists[_source_type] = []
                lists[_source_type].append(source_edge)

                _target_type = target_edge.get_type()
                if _target_type not in lists:
                    lists[_target_type] = []
                lists[_target_type].append(target_edge)
                continue

            _type = entity.get_type()
            if _type not in lists:
                lists[_type] = []
            lists[_type].append(entity)

        return lists

    def add_tables(self, entities):
        """
        Add Pandas dataframes for each node and edge type in the input.
        """

        lists = self._separate_entity_types(entities)

        for _type, _entities in lists.items():
            self._add_entity_df(_type, _entities)

    def _add_entity_df(self, _type, _entities):
        df = pd.DataFrame(pd.json_normalize([node.get_dict() for node in _entities]))
        # replace "properties." with "" in column names
        df.columns = [col.replace("properties.", "") for col in df.columns]
        if _type not in self.dfs:
            self.dfs[_type] = df
        else:
            self.dfs[_type] = pd.concat([self.dfs[_type], df], ignore_index=True)
        return self.dfs[_type]
