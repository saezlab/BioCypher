import networkx as nx
import rdflib


def ontology_to_tree(ontology_path, root_label, switch_id_and_label=True):
    # Load the ontology into an rdflib Graph
    g = rdflib.Graph()
    g.parse(ontology_path, format="ttl")

    # Loop through all labels in the ontology
    for s, _, o in g.triples((None, rdflib.RDFS.label, None)):
        # If the label is the root label, set the root node to the subject of the label
        if o.eq(root_label):
            root = s
            break
    else:
        raise ValueError(f"Could not find root node with label {root_label}")

    # Create a directed graph to represent the ontology as a tree
    G = nx.DiGraph()

    # Define a recursive function to add subclasses to the graph
    def add_subclasses(node):
        # Only add nodes that have a label
        if (node, rdflib.RDFS.label, None) not in g:
            return

        nx_id, nx_label = _get_nx_id_and_label(node)

        if nx_id not in G:
            G.add_node(nx_id)
            G.nodes[nx_id]["label"] = nx_label

        # Recursively add all subclasses of the node to the graph
        for s, _, o in g.triples((None, rdflib.RDFS.subClassOf, node)):
            # Only add nodes that have a label
            if (s, rdflib.RDFS.label, None) not in g:
                continue

            s_id, s_label = _get_nx_id_and_label(s)
            G.add_node(s_id)
            G.nodes[s_id]["label"] = s_label

            G.add_edge(s_id, nx_id)
            add_subclasses(s)
            add_parents(s)

    def add_parents(node):
        # Only add nodes that have a label
        if (node, rdflib.RDFS.label, None) not in g:
            return

        nx_id, nx_label = _get_nx_id_and_label(node)

        # Recursively add all parents of the node to the graph
        for s, _, o in g.triples((node, rdflib.RDFS.subClassOf, None)):
            # Only add nodes that have a label
            if (o, rdflib.RDFS.label, None) not in g:
                continue

            o_id, o_label = _get_nx_id_and_label(o)

            # Skip nodes already in the graph
            if o_id in G:
                continue

            G.add_node(o_id)
            G.nodes[o_id]["label"] = o_label

            G.add_edge(nx_id, o_id)
            add_parents(o)

    def _get_nx_id_and_label(node):
        node_id_str = remove_prefix(str(node))
        node_label_str = str(g.value(node, rdflib.RDFS.label))

        nx_id = node_label_str if switch_id_and_label else node_id_str
        nx_label = node_id_str if switch_id_and_label else node_label_str
        return nx_id, nx_label

    # Add all subclasses of the root node to the graph
    add_subclasses(root)

    return G


def remove_prefix(uri: str) -> str:
    """
    Remove the prefix of a URI. URIs can contain either "#" or "/" as a
    separator between the prefix and the local name. The prefix is
    everything before the last separator.
    """
    return uri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


if __name__ == "__main__":
    path = "test/ontologies/so.owl"
    url = "https://raw.githubusercontent.com/biolink/biolink-model/v3.2.1/biolink-model.owl.ttl"
    root_label = "entity"
    G = ontology_to_tree(url, root_label, switch_id_and_label=True)

    # depth first search: ancestors of the "protein" node
    ancestors = nx.dfs_preorder_nodes(G, "macromolecular complex")
    print(list(ancestors))
