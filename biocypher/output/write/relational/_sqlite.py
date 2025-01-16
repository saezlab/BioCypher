from biocypher.output.write.relational._postgresql import _PostgreSQLBatchWriter


class _SQLiteBatchWriter(_PostgreSQLBatchWriter):
    """
    Class for writing node and edge representations to a SQLite database.
    It uses the _PostgreSQLBatchWriter class under the hood, which already
    implements the logic to write the nodes/edges to a relational DBMS.
    Only the import bash script differs between PostgreSQL and SQLite
    and is therefore implemented in this class.

    - _construct_import_call
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _construct_import_call(self) -> str:
        """
        Function to construct the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name. Built after all data has been
        processed to ensure that nodes are called before any edges.

        Returns:
            str: a bash command for sqlite import
        """
        import_call = ""

        # create tables
        # At this point, csv files of nodes and edges do not require differentiation
        for import_file_path in [
            *self.import_call_nodes,
            *self.import_call_edges,
        ]:
            import_call += f'echo "Setup {import_file_path}..."\n'
            import_call += f"{self.import_call_bin_prefix}sqlite3 {self.db_name} < {import_file_path}"
            import_call += '\necho "Done!"\n'
            import_call += "\n"

        for command in self._copy_from_csv_commands:
            table_name = command.split(" ")[1]
            table_part = command.split(" ")[3].replace("'", "")
            import_call += f'echo "Importing {table_part}..."\n'
            separator = self.delim
            import_part = f".import {table_part} {table_name}"
            import_call += (
                f"{self.import_call_bin_prefix}sqlite3 -separator $'{separator}' {self.db_name} \"{import_part}\""
            )
            import_call += '\necho "Done!"\n'
            import_call += "\n"

        return import_call
