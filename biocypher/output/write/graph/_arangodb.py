"""Module to provide the ArangoDB writer class."""

import os

from biocypher._logger import logger
from biocypher.output.write.graph._neo4j import _Neo4jBatchWriter


class _ArangoDBBatchWriter(_Neo4jBatchWriter):
    """Class for writing node and edge representations to disk.

    Uses the format specified by ArangoDB for the use of "arangoimport".
    Output files are similar to Neo4j, but with a different header format.
    """

    def _get_default_import_call_bin_prefix(self):
        """Provide the default string for the import call bin prefix.

        Returns
        -------
            str: The default location for the neo4j admin import location

        """
        return ""

    def _get_import_script_name(self) -> str:
        """Return the name of the neo4j admin import script.

        Returns
        -------
            str: The name of the import script (ending in .sh)

        """
        return "arangodb-import-call.sh"

    def _write_node_headers(self):
        """Write single CSV file for a graph entity.

        The graph entity is represented as a node as per the definition
        in the `schema_config.yaml`, containing only the header for this type
        of node.

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
            # create header CSV with ID, properties, labels

            _id = "_key"

            # translate label to PascalCase
            pascal_label = self.translator.name_sentence_to_pascal(label)

            header = f"{pascal_label}-header.csv"
            header_path = os.path.join(
                self.outdir,
                header,
            )

            # check if file already exists
            if os.path.exists(header_path):
                logger.warning(f"File {header_path} already exists. Overwriting.")

            # concatenate key:value in props
            props_list = []
            for k in props.keys():
                props_list.append(f"{k}")

            # create list of lists and flatten
            # removes need for empty check of property list
            out_list = [[_id], props_list]
            out_list = [val for sublist in out_list for val in sublist]

            with open(header_path, "w", encoding="utf-8") as f:
                # concatenate with delimiter
                row = self.delim.join(out_list)
                f.write(row)

            # add collection from schema config
            collection = self.translator.ontology.mapping.extended_schema[label].get("db_collection_name", None)

            # add file path to neo4 admin import statement
            # do once for each part file
            parts = self.parts.get(label, [])

            if not parts:
                msg = f"No parts found for node label {label}. Check that the data was parsed first."
                logger.error(msg)
                raise ValueError(msg)

            for part in parts:
                import_call_header_path = os.path.join(
                    self.import_call_file_prefix,
                    header,
                )
                import_call_parts_path = os.path.join(
                    self.import_call_file_prefix,
                    part,
                )

                self.import_call_nodes.add(
                    (
                        import_call_header_path,
                        import_call_parts_path,
                        collection,
                    ),
                )

        return True

    def _write_edge_headers(self):
        """Write single CSV file for a graph entity.

        The graph entity is represented as an edge as per the definition
        in the `schema_config.yaml`, containing only the header for this type
        of edge.

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
            pascal_label = self.translator.name_sentence_to_pascal(label)

            # paths
            header = f"{pascal_label}-header.csv"
            header_path = os.path.join(
                self.outdir,
                header,
            )
            parts = f"{pascal_label}-part.*"

            # check for file exists
            if os.path.exists(header_path):
                logger.warning(f"Header file {header_path} already exists. Overwriting.")

            # concatenate key:value in props
            props_list = []
            for k in props.keys():
                props_list.append(f"{k}")

            out_list = ["_from", "_key", *props_list, "_to"]

            with open(header_path, "w", encoding="utf-8") as f:
                # concatenate with delimiter
                row = self.delim.join(out_list)
                f.write(row)

            # add collection from schema config
            if not self.translator.ontology.mapping.extended_schema.get(label):
                for (
                    _,
                    v,
                ) in self.translator.ontology.mapping.extended_schema.items():
                    if v.get("label_as_edge") == label:
                        collection = v.get("db_collection_name", None)
                        break

            else:
                collection = self.translator.ontology.mapping.extended_schema[label].get("db_collection_name", None)

            # add file path to neo4 admin import statement (import call path
            # may be different from actual output path)
            header_import_call_path = os.path.join(
                self.import_call_file_prefix,
                header,
            )
            parts_import_call_path = os.path.join(
                self.import_call_file_prefix,
                parts,
            )
            self.import_call_edges.add(
                (
                    header_import_call_path,
                    parts_import_call_path,
                    collection,
                ),
            )

        return True

    def _construct_import_call(self) -> str:
        """Construct the import call.

        Details folder and individual node and edge headers and data files,
        as well as delimiters and database name. Built after all data has been
        processed to ensure that nodes are called before any edges.

        Returns
        -------
            str: a bash command for arangoimport

        """
        import_call = f"{self.import_call_bin_prefix}arangoimp --type csv " f'--separator="{self.escaped_delim}" '

        if self.quote == "'":
            import_call += f'--quote="{self.quote}" '
        else:
            import_call += f"--quote='{self.quote}' "

        node_lines = ""

        # node import calls: one line per node type
        for header_path, parts_path, collection in self.import_call_nodes:
            line = f"{import_call} --headers-file {header_path} --file= {parts_path} "

            if collection:
                line += f"--create-collection --collection {collection} "

            node_lines += f"{line}\n"

        edge_lines = ""

        # edge import calls: one line per edge type
        for header_path, parts_path, collection in self.import_call_edges:
            import_call += f'--relationships="{header_path},{parts_path}" '

        return node_lines + edge_lines
