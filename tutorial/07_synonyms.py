from tutorial.data_generator import (
    Complex,
    EntrezProtein,
    InteractionGenerator,
    RandomPropertyProtein,
    RandomPropertyProteinIsoform,
)
import biocypher

__all__ = ['main']


def main():
    # Setup: create a list of proteins to be imported
    proteins_complexes = [
        p for sublist in zip(
            [RandomPropertyProtein() for _ in range(10)],
            [RandomPropertyProteinIsoform() for _ in range(10)],
            [EntrezProtein() for _ in range(10)],
            [Complex() for _ in range(10)],
        ) for p in sublist
    ]

    # Extract id, label, and property dictionary
    def node_generator():
        for p_or_c in proteins_complexes:
            yield (
                p_or_c.get_id(),
                p_or_c.get_label(),
                p_or_c.get_properties(),
            )

    # Simulate edges
    ppi = InteractionGenerator(
        interactors=[p.get_id() for p in proteins_complexes],
        interaction_probability=0.05,
    ).generate_interactions()

    # Extract id, source, target, label, and property dictionary
    def edge_generator():
        for interaction in ppi:
            yield (
                interaction.get_id(),
                interaction.get_source_id(),
                interaction.get_target_id(),
                interaction.get_label(),
                interaction.get_properties(),
            )

    # Create BioCypher driver
    driver = biocypher.Driver(
        offline=True,  # start without connecting to Neo4j instance
        db_name='synonyms',  # name of database for import call
        user_schema_config_path='tutorial/07_schema_config.yaml',
    )
    # Run the import
    driver.write_nodes(node_generator())
    driver.write_edges(edge_generator())

    # Write command line call
    driver.write_import_call()

    # Visualise ontology schema
    driver.show_ontology_structure()


if __name__ == '__main__':
    main()
