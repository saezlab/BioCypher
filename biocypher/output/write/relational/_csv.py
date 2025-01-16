from more_itertools import peekable

from biocypher._logger import logger
from biocypher.output.in_memory._pandas import PandasKG
from biocypher.output.write._writer import _Writer


class _PandasCSVWriter(_Writer):
    """
    Class for writing node and edge representations to CSV files.
    """

    def __init__(self, *args, write_to_file: bool = True, **kwargs):
        kwargs["write_to_file"] = write_to_file
        super().__init__(*args, **kwargs)
        self.in_memory_dfs = {}
        self.stored_dfs = {}
        self.pandas_in_memory = PandasKG(
            deduplicator=self.deduplicator,
        )
        self.delimiter = kwargs.get("delimiter")
        if not self.delimiter:
            self.delimiter = ","
        self.write_to_file = write_to_file

    def _construct_import_call(self) -> str:
        """Function to construct the Python code to load all node and edge csv files again into Pandas dfs.

        Returns:
            str: Python code to load the csv files into Pandas dfs.
        """
        import_call = "import pandas as pd\n\n"
        for df_name in self.stored_dfs.keys():
            import_call += f"{df_name} = pd.read_csv('./{df_name}.csv', header=0, index_col=0)\n"
        return import_call

    def _get_import_script_name(self) -> str:
        """Function to return the name of the import script."""
        return "import_pandas_csv.py"

    def _write_node_data(self, nodes) -> bool:
        passed = self._write_entities_to_file(nodes)
        return passed

    def _write_edge_data(self, edges) -> bool:
        passed = self._write_entities_to_file(edges)
        return passed

    def _write_entities_to_file(self, entities: iter) -> bool:
        """Function to write the entities to a CSV file.

        Args:
            entities (iterable): An iterable of BioCypherNode / BioCypherEdge / BioCypherRelAsNode objects.
        """
        entities = peekable(entities)
        entity_list = self.pandas_in_memory._separate_entity_types(entities)
        for entity_type, entities in entity_list.items():
            self.in_memory_dfs[entity_type] = self.pandas_in_memory._add_entity_df(entity_type, entities)
        for entity_type in self.in_memory_dfs.keys():
            entity_df = self.in_memory_dfs[entity_type]
            if " " in entity_type or "." in entity_type:
                entity_type = entity_type.replace(" ", "_").replace(".", "_")
            if self.write_to_file:
                logger.info(f"Writing {entity_df.shape[0]} entries to {entity_type}.csv.")
                entity_df.to_csv(
                    f"{self.output_directory}/{entity_type}.csv",
                    sep=self.delimiter,
                )
            self.stored_dfs[entity_type] = entity_df
        self.in_memory_dfs = {}
        return True
