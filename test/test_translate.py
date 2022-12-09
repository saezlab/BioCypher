from networkx.classes.graph import Graph
from linkml_runtime.linkml_model.meta import ClassDefinition
import pytest

from biocypher._config import module_data_path
from biocypher._entity import Edge, Node
from biocypher._translate import Translator
from biocypher._biolink import BiolinkAdapter
from biocypher._meta import VersionNode


@pytest.fixture
def version_node():

    return VersionNode(
        from_config=True,
        config_file="biocypher/_config/test_schema_config.yaml",
        offline=True,
    )


@pytest.fixture
def translator(version_node):

    return Translator(version_node.schema)


@pytest.fixture
def biolink_adapter(version_node):

    return BiolinkAdapter(
        schema=version_node.schema,
        model="biocypher",  # this is the default
        # unstable, move to test yaml
        use_cache = False,
    )


def test_translate_nodes(translator):

    items = [
        ("G9205", "protein", {"taxon": 9606}),
        ("hsa-miR-132-3p", "mirna", {"taxon": 9606}),
        ("ASDB_OSBS", "complex", {"taxon": 9606}),
        ("REACT:25520", "reactome", {}),
    ]

    t = translator.translate(items = items)

    assert all(isinstance(n, Node) for n in t)

    t = translator.translate(items = items)

    assert next(t).label == "Protein"
    assert next(t).label == "Microrna"
    assert next(t).label == "Complex"


def test_specific_and_generic_ids(translator):

    items = [
        ("CHAT", "hgnc", {"taxon": 9606}),
        ("REACT:25520", "reactome", {}),
    ]
    t = list(translator.translate(items = items))

    # can't figure out how this makes sense - Denes
    assert t[0].id == "CHAT"
    assert t[0].props.get("id_type") == "id"
    assert t[0].props.get("id") == "CHAT"
    assert t[1].id == "REACT:25520"
    assert t[1].props.get("id_type") == "id" # and this?
    assert t[1].props.get("id") == "REACT:25520"


def test_translate_edges(translator):
    # edge type association (defined in `schema_config.yaml`)
    src_tar_type_edge = (
        ("G15258", "MONDO1", "gene_disease", {}),
        ("G15258", "MONDO2", "protein_disease", {}),
        ("G15258", "G15242", "phosphorylation", {}),
    )

    gen_edges = (e for e in src_tar_type_edge)

    t = translator.translate(items = gen_edges)

    assert type(next(t)) == Edge
    assert next(t).label == "PERTURBED_IN_DISEASE"
    assert next(t).label == "PHOSPHORYLATION"

    # node type association (defined in `schema_config.yaml`)
    src_tar_type_node = [
        ("G21058", "G50127", "post_translational", {"prop1": "test"}),
        (
            "G22418",
            "G50123",
            "post_translational",
            {"directed": "arbitrary_string"},
        ),
        (
            "G15258",
            "G16347",
            "post_translational",
            {"directed": True, "effect": -1},
        ),
    ]
    t = translator.translate(items = src_tar_type_node)

    n = next(t)
    n = next(t)
    assert n.source.label == "IS_PART_OF"
    n = next(t)
    assert (
        isinstance(n.node, Node) and
        isinstance(n.source, Edge) and
        isinstance(n.target, Edge)
    )
    assert n.node.id == "G15258_G16347_directed=True_effect=-1"
    assert n.source.id == None
    assert n.target.label == "IS_TARGET_OF"


def test_adapter(version_node):
    ad = BiolinkAdapter(
        schema = version_node.schema,
        model="biolink",
        use_cache = False,
    )

    assert isinstance(
        ad.biolink_schema["protein"]["class_definition"],
        ClassDefinition,
    )


def test_custom_bmt_yaml(version_node):
    ad = BiolinkAdapter(
        schema = version_node.schema,
        model = module_data_path("test-biolink-model"),
        use_cache = False,
    )
    p = ad.biolink_schema["protein"]

    assert p["class_definition"].description == "Test"


def test_biolink_yaml_extension(biolink_adapter):
    p1 = biolink_adapter.biolink_schema["post translational interaction"]
    p2 = biolink_adapter.biolink_schema["phosphorylation"]

    assert (
        p1["class_definition"].description
        == "A pairwise interaction between two proteins"
        and "PairwiseMolecularInteraction" in p1["ancestors"]
        and "Entity" in p1["ancestors"]
        and p2["class_definition"].description
        == "The action of one protein phosphorylating another protein"
        and "PostTranslationalInteraction" in p2["ancestors"]
        and "Entity" in p2["ancestors"]
    )


def test_translate_identifiers(translator):
    # representation of a different schema
    # host and guest db (which to translate)
    # TODO
    pass


def test_merge_multiple_inputs_node(version_node, translator):
    # Gene has two input labels and one preferred ID
    # no virtual leaves should be created
    # both inputs should lead to creation of the same node type

    # define nodes
    items = [
        ("CHAT", "hgnc", {"taxon": 9606}),
        ("CHRNA4", "ensg", {"taxon": 9606}),
    ]
    t = list(translator.translate(items = items))

    assert t

    # check unique node type
    assert not any([s for s in version_node.schema.keys() if ".gene" in s])
    assert any([s for s in version_node.schema.keys() if ".pathway" in s])

    # check translator.translate for unique return type
    assert all([type(n) == Node for n in t])
    assert all([n.label == "Gene" for n in t])


def test_merge_multiple_inputs_edge(version_node, translator):
    # GeneToDiseaseAssociation has two input labels and one preferred ID
    # no virtual leaves should be created
    # both inputs should lead to creation of the same edge type
    # intersects with driver/writer function

    # define nodes
    src_tar_type = [
        ("CHAT", "AD", "gene_disease", {"taxon": 9606}),
        ("CHRNA4", "AD", "protein_disease", {"taxon": 9606}),
    ]
    t = list(translator.translate(items = src_tar_type))

    # check unique edge type
    assert not any(
        [
            s
            for s in version_node.schema.keys()
            if ".gene to disease association" in s
        ]
    )
    assert any(
        [s for s in version_node.schema.keys() if ".sequence variant" in s]
    )

    # check translator.translate for unique return type
    assert all([type(e) == Edge for e in t])
    assert all([e.label == "PERTURBED_IN_DISEASE" for e in t])


def test_multiple_inputs_multiple_virtual_leaves_rel_as_node(biolink_adapter):

    vtg = biolink_adapter.biolink_schema["variant to gene association"]
    kvtg = biolink_adapter.biolink_schema[
        "known.sequence variant.variant to gene association"
    ]
    svtg = biolink_adapter.biolink_schema[
        "known.sequence variant.variant to gene association"
    ]

    assert (
        isinstance(vtg["class_definition"], ClassDefinition)
        and "VariantToGeneAssociation" in kvtg["ancestors"]
        and "VariantToGeneAssociation" in svtg["ancestors"]
    )


def test_virtual_leaves_inherit_is_a(version_node):

    snrna = version_node.schema.get("intact.snRNA sequence")

    assert "is_a" in snrna.keys()
    assert snrna["is_a"] == ["snRNA sequence", "nucleic acid entity"]

    dsdna = version_node.schema.get("intact.dsDNA sequence")

    assert dsdna["is_a"] == [
        "dsDNA sequence",
        "DNA sequence",
        "nucleic acid entity",
    ]


def test_virtual_leaves_inherit_properties(version_node):

    snrna = version_node.schema.get("intact.snRNA sequence")

    assert "properties" in snrna.keys()
    assert "exclude_properties" in snrna.keys()


def test_ad_hoc_children_node(biolink_adapter):

    se = biolink_adapter.biolink_schema["side effect"]

    assert "PhenotypicFeature" in se["ancestors"]


def test_leaves_of_ad_hoc_child(biolink_adapter):

    snrna = biolink_adapter.biolink_schema.get("intact.snRNA sequence")

    assert snrna
    assert "SnRNASequence" in snrna["ancestors"]

    dsdna = biolink_adapter.biolink_schema.get("intact.dsDNA sequence")

    assert dsdna["ancestors"][1:4] == [
        "DsDNASequence",
        "DNASequence",
        "NucleicAcidEntity",
    ]


def test_inherit_properties(version_node):

    dsdna = version_node.schema.get("intact.dsDNA sequence")

    assert "properties" in dsdna.keys()
    assert "sequence" in dsdna["properties"]


def test_multiple_inheritance(biolink_adapter):

    mta = biolink_adapter.biolink_schema.get("mutation to tissue association")

    assert "MutationToTissueAssociation" in mta["ancestors"]
    assert "GenotypeToTissueAssociation" in mta["ancestors"]
    assert "EntityToTissueAssociation" in mta["ancestors"]
    assert "Association" in mta["ancestors"]


def test_synonym(biolink_adapter):

    comp = biolink_adapter.biolink_leaves.get('complex')

    assert comp
    assert 'Complex' in comp['ancestors']
    assert 'MacromolecularComplexMixin' in comp['ancestors']


def test_properties_from_config(version_node, translator):

    items = [
        ("G49205", "protein", {"taxon": 9606, "name": "test"}),
        ("G92035", "protein", {"taxon": 9606}),
        (
            "G92205",
            "protein",
            {"taxon": 9606, "name": "test2", "test": "should_not_be_returned"},
        ),
    ]
    t = translator.translate(items = items)

    r = list(t)
    assert (
        "name" in r[0].props.keys()
        and "name" in r[1].props.keys()
        and "test" not in r[2].props.keys()
    )

    src_tar_type = [
        (
            "G49205",
            "AD",
            "gene_gene",
            {
                "directional": True,
                "score": 0.5,
            },
        ),
        (
            "G92035",
            "AD",
            "gene_gene",
            {
                "directional": False,
                "curated": True,
                "score": 0.5,
                "test": "should_not_be_returned",
            },
        ),
    ]

    t = translator.translate(items = src_tar_type)

    r = list(t)
    assert (
        "directional" in r[0].props.keys()
        and "directional" in r[1].props.keys()
        and "curated" in r[1].props.keys()
        and "score" in r[0].props.keys()
        and "score" in r[1].props.keys()
        and "test" not in r[1].props.keys()
    )


def test_exclude_properties(translator):
    items = [
        (
            "CHAT",
            "ensg",
            {"taxon": 9606, "accession": "should_not_be_returned"},
        ),
        ("ACHE", "ensg", {"taxon": 9606}),
    ]
    t = translator.translate(items = items)

    r = list(t)
    assert (
        "taxon" in r[0].props.keys()
        and "taxon" in r[1].props.keys()
        and "accession" not in r[0].props.keys()
    )

    src_tar_type = [
        (
            "G49205",
            "AD",
            "gene_disease",
            {
                "directional": True,
                "score": 0.5,
            },
        ),
        (
            "G92035",
            "AD",
            "gene_disease",
            {
                "directional": False,
                "score": 0.5,
                "accession": "should_not_be_returned",
            },
        ),
    ]

    t = translator.translate(items = src_tar_type)

    r = list(t)
    assert (
        "directional" in r[0].props.keys()
        and "directional" in r[1].props.keys()
        and "score" in r[0].props.keys()
        and "score" in r[1].props.keys()
        and "accession" not in r[1].props.keys()
    )


def test_translate_term(biolink_adapter):
    assert biolink_adapter.translate_term("hgnc") == "Gene"
    assert (
        biolink_adapter.translate_term("protein_disease")
        == "PERTURBED_IN_DISEASE"
    )


def test_reverse_translate_term(biolink_adapter):
    assert "hgnc" in biolink_adapter.reverse_translate_term("Gene")
    assert "protein_disease" in biolink_adapter.reverse_translate_term(
        "PERTURBED_IN_DISEASE"
    )


def test_translate_query(biolink_adapter):
    # we translate to PascalCase for cypher queries, not to internal
    # sentence case
    query = "MATCH (n:hgnc)-[r:gene_disease]->(d:Disease) RETURN n"
    assert (
        biolink_adapter.translate(query)
        == "MATCH (n:Gene)-[r:PERTURBED_IN_DISEASE]->(d:Disease) RETURN n"
    )


def test_reverse_translate_query(biolink_adapter):
    # TODO cannot use sentence case in this context. include sentence to
    # pascal case and back in translation?
    query = "MATCH (n:Known.SequenceVariant)-[r:Known.SequenceVariant.VariantToGeneAssociation]->(g:Gene) RETURN n"
    with pytest.raises(NotImplementedError):
        biolink_adapter.reverse_translate(query)

    query = "MATCH (n:Known.SequenceVariant)-[r:Known.SequenceVariant.VariantToGeneAssociation]->(g:Protein) RETURN n"
    assert (
        biolink_adapter.reverse_translate(query)
        == "MATCH (n:Known_variant)-[r:VARIANT_FOUND_IN_GENE_Known_variant_Gene]->(g:protein) RETURN n"
    )


def test_log_missing_nodes(translator):

    items = [
        ("G49205", "missing_protein", {"taxon": 9606}),
        ("G92035", "missing_protein", {}),
        ("REACT:25520", "missing_pathway", {}),
    ]

    tn = translator.translate(items = items)

    tn = list(tn)

    m = translator.get_missing_bl_types()

    assert m.get("missing_protein") == 2
    assert m.get("missing_pathway") == 1


def test_show_tree(biolink_adapter):

    treevis = biolink_adapter.show()

    assert treevis is not None


def test_strict_mode_error(translator):

    translator.strict_mode = True

    n1 = (
        'n2',
        'Test',
        {
            'prop': 'val',
            'source': 'test',
            'licence': 'test',
            'version': 'test'
        },
    )

    assert list(translator.translate_nodes([n1])) is not None

    # test 'license' instead of 'licence'
    n2 = (
        'n2',
        'Test',
        {
            'prop': 'val',
            'source': 'test',
            'license': 'test',
            'version': 'test'
        },
    )

    assert list(translator.translate_nodes([n2])) is not None

    n3 = ('n1', 'Test', {'prop': 'val'})

    with pytest.raises(ValueError):

        list(translator.translate_nodes([n1, n2, n3]))

    e1 = (
        'n1', 'n2', 'Test', {
            'prop': 'val',
            'source': 'test',
            'licence': 'test',
            'version': 'test',
        },
    )

    assert list(translator.translate_edges([e1])) is not None

    e2 = ('n1', 'n2', 'Test', {'prop': 'val'})

    with pytest.raises(ValueError):
        list(translator.translate_edges([e1, e2]))


def test_strict_mode_property_filter(translator):

    translator.strict_mode = True

    p1 = (
        'p1',
        'protein',
        {
            'taxon': 9606,
            'source': 'test',
            'licence': 'test',
            'version': 'test',
        },
    )

    l = list(translator.translate_nodes([p1]))

    assert 'source' in l[0].get_properties().keys()
    assert 'licence' in l[0].get_properties().keys()
    assert 'version' in l[0].get_properties().keys()


def test_networkx_from_treedict(biolink_adapter):
    graph = biolink_adapter.get_networkx_graph()

    assert isinstance(graph, Graph)
