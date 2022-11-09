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

# Configuration
BioCypher comes with a default set of configuration parameters. You can
overwrite them by creating a `biocypher_config.yaml` file in the root directory
of your project. You only need to specify the ones you wish to override from
default. If you want to create global user settings, you can create a
`biocypher_config.yaml` in your default BioCypher user directory (as found using
`appdirs.user_config_dir('biocypher')`). For instance, on Mac OS, this would be
`~/Library/Caches/biocypher/biocypher_config.yaml`.

Configuration files are read in the order `default -> user level -> project
level`, with the later ones overriding the preceding. The following parameters
are available:

## Neo4j settings
```{code-block} yaml
:caption: biocypher_config.yaml
# Neo4j connection credentials
neo4j_uri: neo4j://localhost:7687   # DB URI
neo4j_db: neo4j                     # DB name
neo4j_user: neo4j                   # user name
neo4j_pw: neo4j                     # password

# Neo4j admin import batch writer settings
neo4j_delimiter: '¦'
neo4j_array_delimiter: '|'
neo4j_quote_char: '"'

# MultiDB functionality
# Set to false for using community edition or older versions of Neo4j
neo4j_multi_db: true
```

## BioCypher settings
```{code-block} yaml
:caption: biocypher_config.yaml
# Offline mode: do not connect to a running Neo4j instance
# Can be used e.g. for writing CSV files for admin import
offline: true

# Logging granularity
# Set debug to true if more granular logging is desired
debug: true

# Set to change the log directory
logdir: biocypher-log

# Set to change the output directory
outdir: biocypher-out

# Clear ontology cache
# BioCypher caches the ontology scaffold for performance reasons
# Set to true to clear the cache and re-download the ontology
clear_cache: false
```
