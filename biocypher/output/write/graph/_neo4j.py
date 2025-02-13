"""Module to provide the Neo4j writer class."""

import os

from biocypher._logger import logger
from biocypher.output.write._batch_writer import _BatchWriter, parse_label


class _Neo4jBatchWriter(_BatchWriter):
    """Class for writing node and edge representations to disk using the
    format specified by Neo4j for the use of admin import. Each batch
    writer instance has a fixed representation that needs to be passed
    at instantiation via the :py:attr:`schema` argument. The instance
    also expects an ontology adapter via :py:attr:`ontology_adapter` to be able
    to convert and extend the hierarchy.

    This class inherits from the abstract class "_BatchWriter" and implements the
    Neo4j-specific methods:

        - _write_node_headers
        - _write_edge_headers
        - _construct_import_call
        - _write_array_string
    """

    def __init__(self, *args, **kwargs):
        """Constructor.

        Check the version of Neo4j and adds a command scope if version >= 5.

        Returns
        -------
            _Neo4jBatchWriter: An instance of the writer.

        """
        # Should read the configuration and setup import_call_bin_prefix.
        super().__init__(*args, **kwargs)

    def _get_default_import_call_bin_prefix(self):
        """Method to provide the default string for the import call bin prefix.

        Returns
        -------
            str: The default location for the neo4j admin import location

        """
        return "bin/"

    def _quote_string(self, value: str) -> str:
        """Quote a string. Quote character is escaped by doubling it."""
        return f"{self.quote}{value.replace(self.quote, self.quote * 2)}{self.quote}"

    def _write_array_string(self, string_list):
        """Abstract method to output.write the string representation of an array into a .csv file
        as required by the neo4j admin-import.

        Args:
        ----
            string_list (list): list of ontology strings

        Returns:
        -------
            str: The string representation of an array for the neo4j admin import

        """
        string = self.adelim.join(string_list)
        return self._quote_string(string)

    def _write_node_headers(self):
        """Writes single CSV file for a graph entity that is represented
        as a node as per the definition in the `schema_config.yaml`,
        containing only the header for this type of node.

        Returns
        -------
            bool: The return value. True for success, False otherwise.

        """
        # load headers from data parse
        if not self.node_property_dict:
            logger.error(
                "Header information not found. Was the data parsed first?",
            )
            return False

        for label, props in self.node_property_dict.items():
            _id = ":ID"

            # translate label to PascalCase
            pascal_label = self.translator.name_sentence_to_pascal(parse_label(label))

            header = f"{pascal_label}-header.csv"
            header_path = os.path.join(
                self.outdir,
                header,
            )
            parts = f"{pascal_label}-part.*"

            # check if file already exists
            if os.path.exists(header_path):
                logger.warning(
                    f"Header file `{header_path}` already exists. Overwriting.",
                )

            # concatenate key:value in props
            props_list = []
            for k, v in props.items():
                if v in ["int", "long", "integer"]:
                    props_list.append(f"{k}:long")
                elif v in ["int[]", "long[]", "integer[]"]:
                    props_list.append(f"{k}:long[]")
                elif v in ["float", "double", "dbl"]:
                    props_list.append(f"{k}:double")
                elif v in ["float[]", "double[]"]:
                    props_list.append(f"{k}:double[]")
                elif v in ["bool", "boolean"]:
                    # TODO Neo4j boolean support / spelling?
                    props_list.append(f"{k}:boolean")
                elif v in ["bool[]", "boolean[]"]:
                    props_list.append(f"{k}:boolean[]")
                elif v in ["str[]", "string[]"]:
                    props_list.append(f"{k}:string[]")
                else:
                    props_list.append(f"{k}")

            # create list of lists and flatten
            out_list = [[_id], props_list, [":LABEL"]]
            out_list = [val for sublist in out_list for val in sublist]

            with open(header_path, "w", encoding="utf-8") as f:
                # concatenate with delimiter
                row = self.delim.join(out_list)
                f.write(row)

            # add file path to neo4 admin import statement (import call file
            # path may be different from actual file path)
            import_call_header_path = os.path.join(
                self.import_call_file_prefix,
                header,
            )
            import_call_parts_path = os.path.join(
                self.import_call_file_prefix,
                parts,
            )
            self.import_call_nodes.add((import_call_header_path, import_call_parts_path))

        return True

    def _write_edge_headers(self):
        """Writes single CSV file for a graph entity that is represented
        as an edge as per the definition in the `schema_config.yaml`,
        containing only the header for this type of edge.

        Returns
        -------
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
            pascal_label = self.translator.name_sentence_to_pascal(parse_label(label))

            # paths
            header = f"{pascal_label}-header.csv"
            header_path = os.path.join(
                self.outdir,
                header,
            )
            parts = f"{pascal_label}-part.*"

            # check for file exists
            if os.path.exists(header_path):
                logger.warning(f"File {header_path} already exists. Overwriting.")

            # concatenate key:value in props
            props_list = []
            for k, v in props.items():
                if v in ["int", "long", "integer"]:
                    props_list.append(f"{k}:long")
                elif v in ["int[]", "long[]", "integer[]"]:
                    props_list.append(f"{k}:long[]")
                elif v in ["float", "double"]:
                    props_list.append(f"{k}:double")
                elif v in ["float[]", "double[]"]:
                    props_list.append(f"{k}:double[]")
                elif v in [
                    "bool",
                    "boolean",
                ]:  # TODO does Neo4j support bool?
                    props_list.append(f"{k}:boolean")
                elif v in ["bool[]", "boolean[]"]:
                    props_list.append(f"{k}:boolean[]")
                elif v in ["str[]", "string[]"]:
                    props_list.append(f"{k}:string[]")
                else:
                    props_list.append(f"{k}")

            skip_id = False
            schema_label = None

            if label in ["IS_SOURCE_OF", "IS_TARGET_OF", "IS_PART_OF"]:
                skip_id = True
            elif not self.translator.ontology.mapping.extended_schema.get(label):
                # find label in schema by label_as_edge
                for (
                    k,
                    v,
                ) in self.translator.ontology.mapping.extended_schema.items():
                    if v.get("label_as_edge") == label:
                        schema_label = k
                        break
            else:
                schema_label = label

            out_list = [":START_ID"]

            if schema_label:
                if (
                    self.translator.ontology.mapping.extended_schema.get(  # (seems to not work with 'not')
                        schema_label,
                    ).get("use_id")
                    == False  # noqa: E712 (seems to not work with 'not')
                ):
                    skip_id = True

            if not skip_id:
                out_list.append("id")

            out_list.extend(props_list)
            out_list.extend([":END_ID", ":TYPE"])

            with open(header_path, "w", encoding="utf-8") as f:
                # concatenate with delimiter
                row = self.delim.join(out_list)
                f.write(row)

            # add file path to neo4 admin import statement (import call file
            # path may be different from actual file path)
            import_call_header_path = os.path.join(
                self.import_call_file_prefix,
                header,
            )
            import_call_parts_path = os.path.join(
                self.import_call_file_prefix,
                parts,
            )
            self.import_call_edges.add((import_call_header_path, import_call_parts_path))

        return True

    def _get_import_script_name(self) -> str:
        """Returns the name of the neo4j admin import script

        Returns
        -------
            str: The name of the import script (ending in .sh)

        """
        return "neo4j-admin-import-call.sh"

    def _construct_import_call(self) -> str:
        """Function to construct the import call detailing folder and
        individual node and edge headers and data files, as well as
        delimiters and database name. Built after all data has been
        processed to ensure that nodes are called before any edges.

        Returns
        -------
            str: a bash command for neo4j-admin import

        """
        import_call_neo4j_v4 = self._get_import_call("import", "--database=", "--force=")
        import_call_neo4j_v5 = self._get_import_call("database import full", "", "--overwrite-destination=")
        neo4j_version_check = (
            f"version=$({self._get_default_import_call_bin_prefix()}neo4j-admin --version | cut -d '.' -f 1)"
        )

        import_script = (
            f"#!/bin/bash\n{neo4j_version_check}\nif [[ $version -ge 5 ]]; "
            f"then\n\t{import_call_neo4j_v5}\nelse\n\t{import_call_neo4j_v4}\nfi"
        )
        return import_script

    def _get_import_call(self, import_cmd: str, database_cmd: str, wipe_cmd: str) -> str:
        """Get parametrized import call for Neo4j 4 or 5+.

        Args:
        ----
            import_cmd (str): The import command to use.
            database_cmd (str): The database command to use.
            wipe_cmd (str): The wipe command to use.

        Returns:
        -------
            str: The import call.

        """
        import_call = f"{self.import_call_bin_prefix}neo4j-admin {import_cmd} "

        import_call += f"{database_cmd}{self.db_name} "

        import_call += f'--delimiter="{self.escaped_delim}" '

        import_call += f'--array-delimiter="{self.escaped_adelim}" '

        if self.quote == "'":
            import_call += f'--quote="{self.quote}" '
        else:
            import_call += f"--quote='{self.quote}' "

        if self.wipe:
            import_call += f"{wipe_cmd}true "
        if self.skip_bad_relationships:
            import_call += "--skip-bad-relationships=true "
        if self.skip_duplicate_nodes:
            import_call += "--skip-duplicate-nodes=true "

        # append node import calls
        for header_path, parts_path in self.import_call_nodes:
            import_call += f'--nodes="{header_path},{parts_path}" '

        # append edge import calls
        for header_path, parts_path in self.import_call_edges:
            import_call += f'--relationships="{header_path},{parts_path}" '

        return import_call
