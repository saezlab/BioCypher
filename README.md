# BioCypher

## 📖 Documentation
https://saezlab.github.io/BioCypher/

## ❓ Description
This is the development repository for BioCypher, our proposal for a unified
framework to create knowledge graph databases for systems biology. For an 
overview, usage notes, and a tutorial, read the docs
[here](https://saezlab.github.io/BioCypher/).

![BioCypher](fig_1_draft.png)

## Usage

BioCypher is currently in prototype stage. Installation instructions can be
found [here](https://saezlab.github.io/BioCypher/installation.html).

Exemplary usage of BioCypher to build a graph database is shown in various
adapters we have created. For instance, interested users can refer to the
[migration of the Clinical Knowledge Graph](
https://github.com/saezlab/CKG-BioCypher),
or the [adapters created for the CROssBAR project](
https://github.com/HUBioDataLab/CROssBAR-BioCypher-Migration/blob/main/scripts/create_crossbar.py).
As the project evolves, more of these examples will be directly included in
the documentation.

### Note for contributors
The project uses documentation format [Napoleon](
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
) with a [Sphinx](https://www.sphinx-doc.org/en/master/) autodoc GitHub
Actions workflow. We use poetry for dependency management.
