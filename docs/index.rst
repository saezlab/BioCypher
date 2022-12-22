.. image:: biocypher-open-graph.png

############
Introduction
############

BioCypher (`GitHub repository <https://github.com/saezlab/BioCypher>`_) is a
framework designed to serve as guideline and translation mechanism for both the
creation of biomedical knowledge graph databases from primary data as well as
for the seamless integration, optimisation, and querying of these databases. Its
purpose is to combine the computational power of graph databases with the search
for answers of our most pressing biological questions and facilitate interfacing
with cutting edge developments in the areas of causal reasoning, representation
learning, and natural language processing, all of which depend on having a
consistent descriptive vocabulary. To achieve meaningful automatic
representations in the biomedical language space, we include the `Biolink model
<https://biolink.github.io/biolink-model/>`_ as a basic underlying hierarchical
ontology, supplying identification, filtering, and mapping capabilities. In
addition, we provide options to implement alternative ontological systems by way
of exchanging or extending the base model, for example using `OBO Foundry
<https://obofoundry.org>`_ ontologies. Side objectives are the creation of
human-readable graph syntax and facilitating ultra-rapid graph creation through
the Neo4j admin-import feature.

.. Note::

   BioCypher is a community-driven project. If you have any questions,
   specific needs, or want to contribute to the project, please contact
   us over by `GitHub <https://github.com/saezlab/BioCypher>`_ or via email
   at sebastian.lobentanzer (at) uni-heidelberg.de.

.. figure:: ../graphical_abstract.png
   :width: 70%
   :align: center
   :alt: BioCypher graphical abstract

   BioCypher uses a collection of reusable “adapters” for the different sources
   of biomedical knowledge, which can be flexibly recombined to fit various
   demands, thus reducing redundant maintenance work through
   quasi-standardisation.  Integrating the controlled vocabularies of ontologies
   into the process helps to harmonise the data from individual resources and
   yields a consistent semantic basis for downstream analyses. Through
   unambiguous and simple “low-code” configuration, a reproducible knowledge
   graph can be created and shared for every specific task.
