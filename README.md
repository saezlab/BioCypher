# BioCypher

This is the development repository for BioCypher, our proposal for a unified
framework to create knowledge graph databases for systems biology. For an 
overview, usage notes, and a tutorial, read the docs
[here](https://saezlab.github.io/BioCypher/).

![BioCypher](fig_1_draft.png)

## Usage

BioCypher is currently in prototype stage. Installation instructions can be 
found [here](https://saezlab.github.io/BioCypher/installation.html).

Exemplary usage of BioCypher to build a graph database is shown in the
[PyPath](https://github.com/saezlab/pypath) repository. The example
`PyPath->BioCypher` adapter is in the
[biocypher](https://github.com/saezlab/pypath/tree/master/pypath/biocypher)
directory, the example script for building a local database using the adapter
is located in the
[scripts](https://github.com/saezlab/pypath/blob/master/scripts/) directory.

### Note for contributors

The project uses documentation format [Napoleon](
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
) with a [Sphinx](https://www.sphinx-doc.org/en/master/) autodoc GitHub
Actions workflow. We use poetry for dependency management.
