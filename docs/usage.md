# Usage Notes

-  A graph database can be built from any arbitrary collection of
   biomedical data. We here examplarise the building of a biological
   prior knowledge graph from the OmniPath database ([Türei et al.
   2021](https://www.embopress.org/doi/full/10.15252/msb.20209923)), but
   BioCypher includes the extensive translation functionality from
   ``PyPath`` to accommodate custom requirements for graph database
   contents.

-  A BioCypher graph has to be instantiated as such from the beginning,
   an existing property graph cannot currently be "updated" to conform
   to the BioCypher format.

-  As a first step, an empty Neo4j database has to be created and
   started; the Python database driver can then be established either
   through BioCypher directly or on the user's end and passed to
   BioCypher (if there is greater need for data security in
   authentication).

-  After the database driver has been passed to BioCypher, a new
   database can be established given the selected data structure, to be
   determined in the primary configuration file. In this step, all
   constraints and indices are set to conform to the selected database
   structure. These can also be modified afterwards.

   -  Note: if the database to be created is very extensive, BioCypher
      offers a "secure export" method to create CSV files that can be
      used to instantiate a new graph database very quickly using the
      [Admin Import](https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/)
      feature of Neo4j.

-  The database structure and version are recorded in a meta-graph that
   serves as a versioning system and simultaneously as a means of
   transmitting information about the graph structure for the case of
   re-loading an existing database for updating it with new information.
