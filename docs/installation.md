# Installation
## Install as a dependency
BioCypher is in development; if you want to use it in your project, please use
the *install-from-github* command of your package management system. We
recommend Poetry, you can get it
[here](https://python-poetry.org/docs/#installation). We
will provide a pip package in the future. You can install BioCypher as a
dependency as follows:

```{code-block} yaml
:caption: pyproject.toml
[tool.poetry.dependencies]
python = "^3.9"
biocypher = { git = "https://github.com/saezlab/BioCypher.git", branch = "main" }
```

Alternatively, using conda/pip:

```{code-block} bash
:caption: pip install
conda create --name biocypher python=3.9
conda activate biocypher
pip install git+https://github.com/saezlab/BioCypher.git
```

## Standalone installation
If you want to directly install BioCypher, here are the steps (requires
[Poetry](https://python-poetry.org/docs/#installation)):

```{code-block} bash
:caption: Execute in bash
git clone https://github.com/saezlab/BioCypher
cd BioCypher
poetry install
```

Poetry creates a virtual environment for you (starting with `biocypher-`;
alternatively you can name it yourself) and installs all dependencies. You can
activate the virtual environment by running `poetry shell` and then run the
tests by running `pytest` in the root directory of the repository. Once this is
set up, you can go through the [tutorial](tutorial) or use it in your project as
a local dependency.

```{note}
The `driver` tests require a running Neo4j instance with a DB named `test`.
```
