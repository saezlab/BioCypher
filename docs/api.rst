.. _api-reference:

#############
API Reference
#############

.. _api_core:

The main BioCypher interface
============================

Create a BioCypher instance by running:

.. testcode:: python

   from biocypher import BioCypher
   bc = BioCypher()

Most of the settings should be configured by :ref:`YAML files <config>`. See
below for more information on the BioCypher class.

.. module:: biocypher
.. autosummary::
   :toctree: modules

   BioCypher

.. _api_write:

Database creation by file
=========================

Using the BioCypher instance, you can create a database by writing files by
using the :meth:`BioCypher.write_nodes` and :meth:`BioCypher.write_edges`
methods, which accept collections of nodes and edges either as :ref:`tuples
<tut_01>` or as :class:`BioCypherNode` and :class:`BioCypherEdge` :ref:`objects
<api_create>`. For example:

.. testsetup:: python

   from biocypher import BioCypher
   bc = BioCypher()

   def check_if_function_exists(module_name, function_name):
      if hasattr(module_name, function_name):
         print("Functions exists")
      else:
         print("Function does not exist")

.. testcode:: python
   :hide:

   check_if_function_exists(bc, 'write_nodes')
   check_if_function_exists(bc, 'write_edges')

.. testoutput:: python
   :hide:

   Functions exists
   Functions exists

.. code-block:: python

   # given lists of nodes and edges
   bc.write_nodes(node_list)
   bc.write_edges(edge_list)

.. note::

      To facilitate the interaction with the various database management systems
      (DBMSs), BioCypher provides utility functions, such as writing a Neo4j
      admin import statement to be used for creating a Neo4j database
      (:meth:`BioCypher.write_import_call`). The most commonly used utility
      functions are also available in the wrapper function
      :meth:`BioCypher.summary`. See the :class:`BioCypher` :ref:`class
      <api_core>` for more information.

Details about the :mod:`biocypher.output.write` module responsible for these
methods can be found below.

.. module:: biocypher.output.write

.. autosummary::
   :toctree: modules

   _get_writer.get_writer
   _writer._Writer
   _batch_writer._BatchWriter
   graph._neo4j._Neo4jBatchWriter
   graph._arangodb._ArangoDBBatchWriter
   graph._rdf._RDFWriter
   graph._networkx._NetworkXWriter
   relational._postgresql._PostgreSQLBatchWriter
   relational._sqlite._SQLiteBatchWriter
   relational._csv._PandasCSVWriter

.. api_pandas:

In-memory Pandas knowledge graph
================================

BioCypher provides a wrapper around the :class:`pandas.DataFrame` class to
facilitate the creation of a knowledge graph in memory. This is useful for
testing, small datasets, and for workflows that should remain purely in Python.
Example usage:

.. testcode:: python
   :hide:

   check_if_function_exists(bc, 'add')
   check_if_function_exists(bc, 'to_df')

.. testoutput:: python
   :hide:

   Functions exists
   Functions exists

.. code-block:: python

   from biocypher import BioCypher
   bc = BioCypher()
   # given lists of nodes and edges
   bc.add(node_list)
   bc.add(edge_list)
   # show list of dataframes (one per node/edge type)
   dfs = bc.to_df()

Details about the :mod:`biocypher.output.in_memory` module responsible for these
methods can be found below.

.. module:: biocypher.output.in_memory
.. autosummary::
   :toctree: modules

   _pandas.PandasKG

.. _api_connect:

Database creation and manipulation by Driver
============================================

BioCypher also provides a driver for each of the supported DBMSs. The driver can
be used to create a database and to write nodes and edges to it, as well as
allowing more subtle manipulation usually not encountered in creating a database
from scratch as in the :ref:`file-based workflow <api_write>`. This includes
merging (creation of entities only if they don't exist) and deletion. For
example:

.. testcode:: python
   :hide:

   check_if_function_exists(bc, 'merge_nodes')

.. testoutput:: python
   :hide:

   Functions exists

.. code-block:: python

   from biocypher import BioCypher
   bc = BioCypher()
   # given lists of nodes and edges
   bc.merge_nodes(node_set_1)
   bc.merge_edges(edge_set_1)
   bc.merge_nodes(node_set_2)
   bc.merge_edges(edge_set_2)

Details about the :mod:`biocypher.output.connect` module responsible for these
methods can be found below.

.. module:: biocypher.output.connect
.. autosummary::
   :toctree: modules

   _get_connector.get_connector
   _neo4j_driver._Neo4jDriver


.. api_get:

Download and cache functionality
================================

BioCypher provides a download and cache functionality for resources. Resources
are defined via the abstract ``Resource`` class, which have a name, a (set of)
URL(s), and a lifetime (in days, set to 0 for infinite). Two classes inherit
from the ``Resource`` class, the ``FileDownload`` class and ``APIRequest``
class. The ``Downloader`` can deal with single files, lists of files, compressed
files, and directories (which needs to be indicated using the ``is_dir``
parameter of the ``FileDownload``). It uses `Pooch
<https://www.fatiando.org/pooch/latest/>`_ under the hood to handle the
downloading of files and Python's `requests
<https://pypi.org/project/requests/>`_ library to perform API requests. Example
usage:

.. testcode:: python
   :hide:

   from biocypher import FileDownload, APIRequest

   # Check if the classes can be instantiated
   file_download_instance = FileDownload(
       name="test_file",
       url_s="https://example.com/test_file.txt",
       lifetime=1
   )
   api_request_instance = APIRequest(
       name="test_api",
       url_s="https://api.example.com/test_api",
       lifetime=1
   )

   print(isinstance(file_download_instance, FileDownload))
   print(isinstance(api_request_instance, APIRequest))

.. testoutput:: python
   :hide:

   True
   True

.. testcode:: python
   :hide:

   check_if_function_exists(bc, 'download')

.. testoutput:: python
   :hide:

   Functions exists

.. code-block:: python

   from biocypher import BioCypher, FileDownload, APIRequest
   bc = BioCypher()

   resource1 = FileDownload(
       name="file_list_resource",
       url_s=[
           "https://example.com/file_download1.txt",
           "https://example.com/file_download2.txt"
       ],
       lifetime=1
   )
   resource2 = FileDownload(
       name="zipped_resource",
       url_s="https://example.com/file_download3.zip",
       lifetime=7
   )
   resource3 = FileDownload(
       name="directory_resource",
       url_s="https://example.com/file_download4/",
       lifetime=7,
       is_dir=True,
   )
   resource4 = APIRequest(
       name="list_api_request",
       url_s=[
           "https://api.example.org/api_request1",
           "https://api.example.org/api_request2",
       ],
       life_time=7,
   )
   resource5 = APIRequest(
       name="api_request",
       url_s="https://api.example.org/api_request1",
       life_time=7,
   )
   resource_list = [resource1, resource2, resource3, resource4, resource5]
   paths = bc.download(resource_list)

The files and API requests will be stored in the cache directory, in subfolders
according to the names of the resources, and additionally determined by Pooch
(e.g., extraction of zip files can result in multiple new files). All paths of
downloaded files are returned by the ``download`` method. The ``Downloader``
class can also be used directly, without the BioCypher instance. You can set
the cache directory in the configuration file; if not set, it will use the
``TemporaryDirectory.name()`` method from the ``tempfile`` module. More details
about the ``Resource`` , ``FileDownload`` , ``APIRequest`` and ``Downloader``
classes can be found below.

.. module:: biocypher._get
.. autosummary::
   :toctree: modules

   Resource
   APIRequest
   FileDownload
   Downloader

Ontology ingestion, parsing, and manipulation
=============================================
.. module:: biocypher._ontology
.. autosummary::
   :toctree: modules

   Ontology
   OntologyAdapter

Mapping of data inputs to KG ontology
=====================================
.. module:: biocypher._mapping
.. autosummary::
   :toctree: modules

   OntologyMapping

.. _api_create:

Base classes for node and edge representations in BioCypher
===========================================================
.. module:: biocypher._create
.. autosummary::
   :toctree: modules

   BioCypherNode
   BioCypherEdge
   BioCypherRelAsNode

Translation functionality for implemented types of representation
=================================================================
.. module:: biocypher._translate
.. autosummary::
   :toctree: modules

   Translator

Logging
=======
.. module:: biocypher._logger
.. autosummary::
   :toctree: modules

   get_logger

Miscellaneous utility functions
===============================
.. module:: biocypher._misc
.. autosummary::
   :toctree: modules

   to_list
   ensure_iterable
   create_tree_visualisation
   from_pascal
   pascalcase_to_sentencecase
   snakecase_to_sentencecase
   sentencecase_to_snakecase
   sentencecase_to_pascalcase
