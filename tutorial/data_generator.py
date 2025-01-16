"""
Classes used to generate mock data for the BioCypher tutorial.
"""

import random
import string

__all__ = [
    "EntrezProtein",
    "Interaction",
    "InteractionGenerator",
    "Node",
    "Protein",
    "ProteinProteinInteraction",
    "RandomPropertyProtein",
    "RandomPropertyProteinIsoform",
]


class Node:
    """
    Base class for nodes.
    """

    def __init__(self):
        self.id = None
        self.label = None
        self.properties = {}

    def get_id(self):
        """
        Returns the node id.
        """
        return self.id

    def get_label(self):
        """
        Returns the node label.
        """
        return self.label

    def get_properties(self):
        """
        Returns the node properties.
        """
        return self.properties


class Protein(Node):
    """
    Generates instances of proteins.
    """

    def __init__(self):
        self.id = self._generate_id()
        self.label = "uniprot_protein"
        self.properties = self._generate_properties()

    def _generate_id(self):
        """
        Generate a random UniProt-style id.
        """
        lets = [random.choice(string.ascii_uppercase) for _ in range(3)]
        nums = [random.choice(string.digits) for _ in range(3)]

        # join alternating between lets and nums
        return "".join([x for y in zip(lets, nums) for x in y])

    def _generate_properties(self):
        properties = {}

        ## random amino acid sequence

        # random int between 20 and 50
        random_length = random.randint(20, 50)

        properties["sequence"] = "".join(
            [random.choice("ACDEFGHIKLMNPQRSTVWY") for _ in range(random_length)],
        )

        ## random description
        properties["description"] = "Lorem ipsum " + "".join(
            [random.choice(string.ascii_lowercase) for _ in range(5)],
        )

        ## taxon
        properties["taxon"] = "9606"

        return properties


class Complex(Node):
    """
    Generates instances of complexes.
    """

    def __init__(self):
        self.id = self._generate_id()
        self.label = "complex"
        self.properties = self._generate_properties()

    def _generate_id(self):
        """
        Generate a random Complex Portal id.
        """
        nums = [random.choice(string.digits) for _ in range(4)]

        # join alternating between lets and nums
        return f"CPX-{''.join(nums)}"

    def _generate_properties(self):
        properties = {}

        ## random description
        properties["description"] = "Lorem ipsum " + "".join(
            [random.choice(string.ascii_lowercase) for _ in range(5)],
        )

        ## taxon
        properties["taxon"] = "9606"

        return properties


class RandomPropertyProtein(Protein):
    """
    Generates instances of proteins with random properties.
    """

    def _generate_properties(self):
        properties = {}

        ## random amino acid sequence

        # random int between 20 and 50
        random_length = random.randint(20, 50)

        properties["sequence"] = "".join(
            [random.choice("ACDEFGHIKLMNPQRSTVWY") for _ in range(random_length)],
        )

        ## random description
        properties["description"] = "Lorem ipsum " + "".join(
            [random.choice(string.ascii_lowercase) for _ in range(5)],
        )

        ## random taxon
        properties["taxon"] = str(random.randint(0, 10000))

        ## randomly add 'mass'
        if random.random() > 0.5:
            properties["mass"] = random.randint(0, 10000)

        return properties


class RandomPropertyProteinIsoform(RandomPropertyProtein):
    """
    Generates instances of protein isoforms with random properties.
    """

    def __init__(self):
        super().__init__()
        self.label = "uniprot_isoform"


class EntrezProtein(Protein):
    """
    Generates instances of proteins with Entrez IDs.
    """

    def __init__(self):
        super().__init__()
        self.id = self._generate_id()
        self.label = "entrez_protein"

    def _generate_id(self):
        """
        Generate a random Entrez-style ID.
        """
        return str(random.randint(1, 1000000))


class Interaction:
    """
    Base class for interactions.
    """

    def __init__(self):
        self.id = None
        self.source_id = None
        self.target_id = None
        self.label = None
        self.properties = {}

    def get_id(self):
        """
        Returns the relationship id.
        """
        return self.id

    def get_source_id(self):
        """
        Returns the source id.
        """
        return self.source_id

    def get_target_id(self):
        """
        Returns the target id.
        """
        return self.target_id

    def get_label(self):
        """
        Returns the node label.
        """
        return self.label

    def get_properties(self):
        """
        Returns the node properties.
        """
        return self.properties


class ProteinProteinInteraction(Interaction):
    """
    Simulates interactions between proteins given a source and target protein
    IDs. Occasionally generates an ID for the interaction itself.
    """

    def __init__(self, source, target):
        super().__init__()
        self.id = self._generate_id()
        self.source_id = source
        self.target_id = target
        self.label = "interacts_with"
        self.properties = self._generate_properties()

    def _generate_id(self):
        """
        Generate a random ID for the interaction.
        """
        if random.random() > 0.5:
            return None
        else:
            return "intact" + str(random.randint(1, 1000000))

    def _generate_properties(self):
        properties = {}

        ## randomly add 'source'
        if random.random() > 0.5:
            properties["source"] = random.choice(["intact", "signor"])

        ## randomly add 'method'
        if random.random() > 0.5:
            properties["method"] = "Lorem ipsum " + "".join(
                [random.choice(string.ascii_lowercase) for _ in range(5)],
            )

        return properties


class InteractionGenerator:
    """
    Simulates interactions given a list of potential interactors based on an
    interaction probability or probability distribution.
    """

    def __init__(self, interactors: list, interaction_probability: float):
        self.interactors = interactors
        self.interaction_probability = interaction_probability

    def generate_interactions(self) -> list:
        """
        Generates interactions between interactors. Flat probability of
        interaction.
        """
        interactions = []

        for source in self.interactors:
            for target in self.interactors:
                if source == target:
                    continue

                if random.random() < self.interaction_probability:
                    interactions.append(
                        ProteinProteinInteraction(source, target),
                    )

        return interactions
