import glob
import os

from biocypher._logger import logger
from biocypher.output.write._batch_writer import _BatchWriter


class _PostgreSQLBatchWriter(_BatchWriter):
    """
    Class for writing node and edge representations to disk using the
    format specified by PostgreSQL for the use of "COPY FROM...". Each batch
    writer instance has a fixed representation that needs to be passed
    at instantiation via the :py:attr:`schema` argument. The instance
    also expects an ontology adapter via :py:attr:`ontology_adapter` to be able
    to convert and extend the hierarchy.

    This class inherits from the abstract class "_BatchWriter" and implements the
    PostgreSQL-specific methods:

        - _write_node_headers
        - _write_edge_headers
        - _construct_import_call
        - _write_array_string
    """

    DATA_TYPE_LOOKUP = {
        "str": "VARCHAR",  # VARCHAR needs limit
        "int": "INTEGER",
        "long": "BIGINT",
        "float": "NUMERIC",
        "double": "NUMERIC",
        "dbl": "NUMERIC",
        "boolean": "BOOLEAN",
        "str[]": "VARCHAR[]",
        "string[]": "VARCHAR[]",
    }

    def __init__(self, *args, **kwargs):
        self._copy_from_csv_commands = set()
        super().__init__(*args, **kwargs)

    def _get_default_import_call_bin_prefix(self):
        """
        Method to provide the default string for the import call bin prefix.

        Returns:
            str: The default location for the psql command
        """
        return ""

    def _get_data_type(self, string) -> str:
        try:
            return self.DATA_TYPE_LOOKUP[string]
        except KeyError:
            logger.info('Could not determine data type {string}. Using default "VARCHAR"')
            return "VARCHAR"

    def _write_array_string(self, string_list) -> str:
        """
        Abstract method to output.write the string representation of an array into a .csv file
        as required by the postgresql COPY command, with '{','}' brackets and ',' separation.

        Args:
            string_list (list): list of ontology strings

        Returns:
            str: The string representation of an array for postgres COPY
        """
        string = ",".join(string_list)
        string = f'"{{{string}}}"'
        return string

    def _get_import_script_name(self) -> str:
        """
        Returns the name of the psql import script

        Returns:
            str: The name of the import script (ending in .sh)
        """
        return f"{self.db_name}-import-call.sh"

    def _adjust_pascal_to_psql(self, string):
        string = string.replace(".", "_")
        string = string.lower()
        return string

    def _write_node_headers(self):
        """
        Writes single CSV file for a graph entity that is represented
        as a node as per the definition in the `schema_config.yaml`,
        containing only the header for this type of node.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        # load headers from data parse
        if not self.node_property_dict:
            logger.error(
                "Header information not found. Was the data parsed first?",
            )
            return False

        for label, props in self.node_property_dict.items():
            # create header CSV with ID, properties, labels

            # translate label to PascalCase
            pascal_label = self.translator.name_sentence_to_pascal(label)

            parts = f"{pascal_label}-part*.csv"
            parts_paths = os.path.join(self.outdir, parts)
            parts_paths = glob.glob(parts_paths)
            parts_paths.sort()

            # adjust label for import to psql
            pascal_label = self._adjust_pascal_to_psql(pascal_label)
            table_create_command_path = os.path.join(
                self.outdir,
                f"{pascal_label}-create_table.sql",
            )

            # check if file already exists
            if os.path.exists(table_create_command_path):
                logger.warning(
                    f"File {table_create_command_path} already exists. Overwriting.",
                )

            # concatenate key:value in props
            columns = ["_ID VARCHAR"]
            for col_name, col_type in props.items():
                col_type = self._get_data_type(col_type)
                col_name = self._adjust_pascal_to_psql(col_name)
                columns.append(f"{col_name} {col_type}")
            columns.append("_LABEL VARCHAR[]")

            with open(table_create_command_path, "w", encoding="utf-8") as f:
                command = ""
                if self.wipe:
                    command += f"DROP TABLE IF EXISTS {pascal_label};\n"

                # table creation requires comma separation
                command += f"CREATE TABLE {pascal_label}({','.join(columns)});\n"
                f.write(command)

                for parts_path in parts_paths:
                    # if import_call_file_prefix is set, replace actual path
                    # with prefix
                    if self.import_call_file_prefix != self.outdir:
                        parts_path = parts_path.replace(
                            self.outdir,
                            self.import_call_file_prefix,
                        )

                    self._copy_from_csv_commands.add(
                        f"\\copy {pascal_label} FROM '{parts_path}' DELIMITER E'{self.delim}' CSV;"
                    )

            # add file path to import statement
            # if import_call_file_prefix is set, replace actual path
            # with prefix
            if self.import_call_file_prefix != self.outdir:
                table_create_command_path = table_create_command_path.replace(
                    self.outdir,
                    self.import_call_file_prefix,
                )

            self.import_call_nodes.add(table_create_command_path)

        return True

    def _write_edge_headers(self):
        """
        Writes single CSV file for a graph entity that is represented
        as an edge as per the definition in the `schema_config.yaml`,
        containing only the header for this type of edge.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        # load headers from data parse
        if not self.edge_property_dict:
            logger.error(
                "Header information not found. Was the data parsed first?",
            )
            return False

        for label, props in self.edge_property_dict.items():
            # translate label to PascalCase
            pascal_label = self.translator.name_sentence_to_pascal(label)

            parts_paths = os.path.join(self.outdir, f"{pascal_label}-part*.csv")
            parts_paths = glob.glob(parts_paths)
            parts_paths.sort()

            # adjust label for import to psql
            pascal_label = self._adjust_pascal_to_psql(pascal_label)
            table_create_command_path = os.path.join(
                self.outdir,
                f"{pascal_label}-create_table.sql",
            )

            # check for file exists
            if os.path.exists(table_create_command_path):
                logger.warning(
                    f"File {table_create_command_path} already exists. Overwriting.",
                )

            # concatenate key:value in props
            columns = []
            for col_name, col_type in props.items():
                col_type = self._get_data_type(col_type)
                col_name = self._adjust_pascal_to_psql(col_name)
                if col_name == "_ID":
                    # should ideally never happen
                    raise ValueError(
                        "Column name '_ID' is reserved for internal use, "
                        "denoting the relationship ID. Please choose a "
                        "different name for your column."
                    )

                columns.append(f"{col_name} {col_type}")

            # create list of lists and flatten
            # removes need for empty check of property list
            out_list = [
                "_START_ID VARCHAR",
                "_ID VARCHAR",
                *columns,
                "_END_ID VARCHAR",
                "_TYPE VARCHAR",
            ]

            with open(table_create_command_path, "w", encoding="utf-8") as f:
                command = ""
                if self.wipe:
                    command += f"DROP TABLE IF EXISTS {pascal_label};\n"

                # table creation requires comma separation
                command += f"CREATE TABLE {pascal_label}({','.join(out_list)});\n"
                f.write(command)

                for parts_path in parts_paths:
                    # if import_call_file_prefix is set, replace actual path
                    # with prefix
                    if self.import_call_file_prefix != self.outdir:
                        parts_path = parts_path.replace(
                            self.outdir,
                            self.import_call_file_prefix,
                        )

                    self._copy_from_csv_commands.add(
                        f"\\copy {pascal_label} FROM '{parts_path}' DELIMITER E'{self.delim}' CSV;"
                    )

            # add file path to import statement
            # if import_call_file_prefix is set, replace actual path
            # with prefix
            if self.import_call_file_prefix != self.outdir:
                table_create_command_path = table_create_command_path.replace(
                    self.outdir,
                    self.import_call_file_prefix,
                )

            self.import_call_edges.add(table_create_command_path)

        return True

    def _construct_import_call(self) -> str:
        """
        Function to construct the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name. Built after all data has been
        processed to ensure that nodes are called before any edges.

        Returns:
            str: a bash command for postgresql import
        """
        import_call = ""

        # create tables
        # At this point, csv files of nodes and edges do not require differentiation
        for import_file_path in [
            *self.import_call_nodes,
            *self.import_call_edges,
        ]:
            import_call += f'echo "Setup {import_file_path}..."\n'
            if {self.db_password}:
                # set password variable inline
                import_call += f"PGPASSWORD={self.db_password} "
            import_call += f"{self.import_call_bin_prefix}psql -f {import_file_path}"
            import_call += f" --dbname {self.db_name}"
            import_call += f" --host {self.db_host}"
            import_call += f" --port {self.db_port}"
            import_call += f" --user {self.db_user}"
            import_call += '\necho "Done!"\n'
            import_call += "\n"

        # copy data to tables
        for command in self._copy_from_csv_commands:
            table_part = command.split(" ")[3]
            import_call += f'echo "Importing {table_part}..."\n'
            if {self.db_password}:
                # set password variable inline
                import_call += f"PGPASSWORD={self.db_password} "
            import_call += f'{self.import_call_bin_prefix}psql -c "{command}"'
            import_call += f" --dbname {self.db_name}"
            import_call += f" --host {self.db_host}"
            import_call += f" --port {self.db_port}"
            import_call += f" --user {self.db_user}"
            import_call += '\necho "Done!"\n'
            import_call += "\n"

        return import_call
