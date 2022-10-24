# Installation
BioCypher is in development; if you want to use it, please clone the repository
and install the dependencies using Poetry. If you don't have Poetry installed,
you can get instructions for your system
[here](https://python-poetry.org/docs/#installation). We will provide a pip
package in the future.

```{code-block} bash
:caption: Execute in bash
git clone https://github.com/saezlab/BioCypher
cd BioCypher
poetry install
```

Poetry creates a virtual environment for you (starting with `biocypher-`;
alternatively you can name it yourself) and installs all dependencies. You can
activate the virtual environment by running `poetry shell` and then run the
tests by running `pytest` in the root directory of the repository.

```{note}
The `driver` tests require a running Neo4j instance with a DB named `test`.
```
