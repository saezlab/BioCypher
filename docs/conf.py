# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
# -- Path setup --------------------------------------------------------------

import pathlib

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import sys

from datetime import datetime

import biocypher

here = pathlib.Path(__file__).parent
sys.path.insert(0, str(here.parent))

# -- Project information -----------------------------------------------------

project = "BioCypher"
version = biocypher.__version__
author = ", ".join(biocypher.__author__)
copyright = f"2021-{datetime.now():%Y}, BioCypher developers"

# -- General configuration ---------------------------------------------------

# TOC only in sidebar
master_doc = "contents"
html_sidebars = {
    "**": [
        "globaltoc.html",
        "relations.html",
        "sourcelink.html",
        "searchbox.html",
    ],
}

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",  # not for output but to remove warnings
    "sphinx.ext.autosummary",
    "sphinxext.opengraph",
    "sphinx.ext.doctest",
    "myst_parser",  # markdown support
    "sphinx_rtd_theme",
    "sphinx_design",
    "nbsphinx",
]
myst_enable_extensions = ["colon_fence"]
autosummary_generate = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "biocypher-log/"]

# -- Autodoc configuration ---------------------------------------------------

autodoc_mock_imports = ["bmt", "neo4j-utils"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_title = "BioCypher"
html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "navigation_depth": 2,
    "collapse_navigation": True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']

# -- OpenGraph configuration -------------------------------------------------

ogp_site_url = "https://biocypher.org"
ogp_image = "https://biocypher.org/_images/biocypher-open-graph.png"
ogp_custom_meta_tags = [
    '<meta property="twitter:card" content="summary_large_image" />',
    '<meta property="twitter:url" content="https://biocypher.org" />',
    '<meta property="twitter:title" content="Democratising Knowledge Graphs - BioCypher" />',
    '<meta property="twitter:description" content="BioCypher is a framework to simplify the creation of robust biomedical knowledge graphs." />',  # noqa: E501
    '<meta property="twitter:image" content="https://biocypher.org/_images/biocypher-open-graph.png" />',
]
ogp_enable_meta_description = True
