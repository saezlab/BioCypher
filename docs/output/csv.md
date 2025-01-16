# Tabular output

When setting the `dbms` parameter in the `biocypher_config.yaml` to `tabular`,
`csv`, or `pandas`, the BioCypher Knowledge Graph is created in one of several
possible tabular formats.

## Tabular output settings

To overwrite the standard settings of the CSV writer, add a `csv` section to the
`biocypher_config.yaml` file. The following settings are possible:

```{code-block} yaml
:caption: biocypher_config.yaml

csv:
  ### CSV/Pandas configuration ###
  delimiter: ','  # The delimiter to be used in the CSV files. Default is ','.
```

## Offline mode

### Running BioCypher

After running BioCypher with the ``offline`` parameter set to ``true`` and the
``dbms`` set to ``tabular``, ``csv``, or ``pandas``, the output folder contains:

- ``*.csv``: The CSV files containing the node/edge data.

- ``import_pandas_csv.csv``: A Python script to load the created CSV files into
Pandas DataFrames.

## Online mode

After running BioCypher with the ``offline`` parameter set to ``false`` and the
``dbms`` set to ``tabular``, ``csv``, or ``pandas``, you can get the in-memory
representation of the Knowledge Graph directly from BioCypher by calling the
`get_kg()` function. This returns a dictionary with the corresponding data type
(e.g., ``Pandas`` dataframes) for every node and edge type.

```{testcode} python
:hide:
from biocypher import BioCypher
bc = BioCypher()

def check_if_function_exists(module_name, function_name):
    if hasattr(module_name, function_name):
        print("Functions exists")
    else:
        print("Function does not exist")
check_if_function_exists(bc, "get_kg")
```
```{testoutput} python
:hide:
Functions exists
```

```{code-block} python
# Initialize BioCypher
bc = BioCypher(
    biocypher_config_path="biocypher_config.yaml",
    schema_config_path="schema_config.yaml",
)

# Add nodes and edges
bc.add_nodes(nodes)
bc.add_edges(edges)

# Get the in-memory representation of the Knowledge Graph
in_memory_kg = bc.get_kg()
```
