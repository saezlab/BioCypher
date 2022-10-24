from tutorial.data_generator import Protein

import biocypher


def main():
    # Setup: create a list of proteins to be imported
    proteins = [Protein() for _ in range(10)]

    # Extract id, label, and property dictionary
    def node_generator():
        for protein in proteins:
            yield (
                protein.get_id(),
                protein.get_label(),
                protein.get_properties(),
            )

    # Create BioCypher driver
    driver = biocypher.Driver(
        offline=True,  # start without connecting to Neo4j instance
        db_name="basicimport",  # name of database for import call
        user_schema_config_path="tutorial/01_schema_config.yaml",
    )
    # Run the import
    driver.write_nodes(node_generator())

    # Write command line call
    driver.write_import_call()


if __name__ == "__main__":
    main()
