import os
import re
import random
import string
import tempfile

import pytest

from biocypher._meta import VersionNode
from biocypher._write import BatchWriter
from biocypher._driver import Driver
from biocypher._entity import Edge, Node, RelAsNode
from biocypher._biolink import BiolinkAdapter
from biocypher._ontology import OntologyAdapter
from biocypher._translate import Translator

__all__ = ['bw', 'bw_strict', 'get_random_string', 'tab_bw', 'test_accidental_exact_batch_size', 'test_create_import_call', 'test_duplicate_edges', 'test_duplicate_id', 'test_duplicate_nodes', 'test_get_duplicate_edges', 'test_not_enough_properties', 'test_property_types', 'test_relasnode_implementation', 'test_relasnode_overwrite_behaviour', 'test_tab_delimiter', 'test_too_many_properties', 'test_write_duplicate_edges', 'test_write_edge_data_from_gen', 'test_write_edge_data_from_large_gen', 'test_write_edge_data_from_list', 'test_write_edge_data_from_list_no_props', 'test_write_edge_data_headers_import_call', 'test_write_hybrid_ontology_nodes', 'test_write_mixed_edges', 'test_write_node_data_from_gen', 'test_write_node_data_from_gen_no_props', 'test_write_node_data_from_large_gen', 'test_write_node_data_from_list', 'test_write_node_data_headers_import_call', 'test_write_none_type_property_and_order_invariance', 'test_write_offline', 'test_write_strict', 'test_write_synonym', 'test_writer_and_output_dir', 'translator', 'unformat', 'version_node']


def get_random_string(length):

    # choose from all lowercase letter
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


# temporary output paths
path = os.path.join(
    tempfile.gettempdir(),
    f'biocypher-test-{get_random_string(5)}',
)
os.makedirs(path, exist_ok=True)


def _get_nodes(l: int) -> list:

    nodes = []

    for i in range(int(l)):

        bnp = Node(
            id=f'p{i+1}',
            label='protein',
            id_type='uniprot',
            props={
                'score': 4 / (i + 1),
                'name': 'StringProperty1',
                'taxon': 9606,
                'genes': ['gene1', 'gene2'],
            },
        )
        nodes.append(bnp)
        bnm = Node(
            id=f'm{i+1}',
            label='microRNA',
            id_type='mirbase',
            props={'name': 'StringProperty1', 'taxon': 9606},
        )
        nodes.append(bnm)

    return nodes


def _get_edges(l):

    edges = []

    for i in range(int(l)):

        e1 = Edge(
            source=f'p{i}',
            target=f'p{i + 1}',
            label='PERTURBED_IN_DISEASE',
            props={
                'residue': 'T253',
                'level': 4,
                'directional': True,
                'score': .78,
            },
            # we suppose the verb-form relationship label is created by
            # translation functionality in translate.py
        )
        edges.append(e1)
        e2 = Edge(
            source=f'm{i}',
            target=f'p{i + 1}',
            label='Is_Mutated_In',
            props={'site': '3-UTR', 'confidence': 1},
            # we suppose the verb-form relationship label is created by
            # translation functionality in translate.py
        )
        edges.append(e2)

    return edges


def _get_rel_as_nodes(l):

    rels = []

    for i in range(int(l)):

        n = Node(
            id=f'i{i+1}',
            label='post translational interaction',
            props={'directed': True, 'effect': -1},
        )
        e1 = Edge(
            source=f'i{i+1}',
            target=f'p{i+1}',
            label='IS_SOURCE_OF',
        )
        e2 = Edge(
            source=f'i{i}',
            target=f'p{i + 2}',
            label='IS_TARGET_OF',
        )
        rels.append(RelAsNode(n, e1, e2))

    return rels


def _get_mixed_edges(n: int) -> list:
    """
    Creates a list of Edge and RelAsNode objects.
    """

    pid_props = {
        'directional': True,
        'residue': 'T253',
        'level': 4,
        'score': .78,
    }

    mixed = [
        Edge(
            source = f'p{i + 1}',
            target = f'p{i + 1}',  # Q: are source and target the same
            label = 'PERTURBED_IN_DISEASE',
            props = pid_props,
        )
        for i in range(n)
    ] + [
        RelAsNode(
            node = Node(f'i{i + 1}', 'post translational interaction'),
            source = Edge(f'i{i + 1}', f'p{i + 1}', 'IS_SOURCE_OF'),
            target = Edge(f'i{i + 1}', f'p{i + 2}', 'IS_TARGET_OF'),
        )
        for i in range(n)
    ]

    return mixed


def unformat(s: str) -> str:
    """
    Removes whitespace and line breaks from a string.
    """

    rewhite = re.compile(r'[\s\n\t\r]+')

    return rewhite.sub(' ', s)


@pytest.fixture
def version_node():

    return VersionNode(
        from_config=True,
        config_file='biocypher/_config/test_schema_config.yaml',
        offline=True,
    )


@pytest.fixture
def translator(version_node):

    return Translator(schema=version_node.schema)


@pytest.fixture
def bw(version_node, translator):

    biolink_adapter = BiolinkAdapter(
        schema=version_node.schema,
        translator=translator,
    )

    ontology_adapter = OntologyAdapter(
        tail_ontology_url='test/so.obo',
        head_join_node='sequence variant',
        tail_join_node='sequence_variant',
        biolink_adapter=biolink_adapter,
    )

    bw = BatchWriter(
        schema=version_node.schema,
        ontology_adapter=ontology_adapter,
        translator=translator,
        dirname=path,
        delimiter=';',
        array_delimiter='|',
        quote="'",
    )

    yield bw

    # teardown
    for f in os.listdir(path):

        os.remove(os.path.join(path, f))

    os.rmdir(path)


def bw_strict(version_node, translator):

    biolink_adapter = BiolinkAdapter(
        schema=version_node.schema,
        translator=translator,
        model=module_data_path('test-biolink-model'),
        use_cache = False,
    )

    ontology_adapter = OntologyAdapter(biolink_adapter=biolink_adapter)

    bw = BatchWriter(
        schema=version_node.schema,
        ontology_adapter=ontology_adapter,
        translator=translator,
        dirname=path_strict,
        delimiter=';',
        array_delimiter='|',
        quote="'",
        strict_mode=True,
    )

    yield bw

    # teardown
    for f in os.listdir(path_strict):
        os.remove(os.path.join(path_strict, f))
    os.rmdir(path_strict)


@pytest.fixture
def tab_bw(version_node, translator):

    tmp_biolink_adapter = BiolinkAdapter(
        schema=version_node.schema,
        translator=translator,
        model=module_data_path('test-biolink-model'),
        use_cache = False,
    )

    tmp_ontology_adapter = OntologyAdapter(
        biolink_adapter=tmp_biolink_adapter,
    )

    tab_bw = BatchWriter(
        schema=version_node.schema,
        ontology_adapter=tmp_ontology_adapter,
        translator=translator,
        dirname=path,
        delimiter='\t',
        array_delimiter='|',
        quote="'",
    )

    yield tab_bw

    # teardown
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))
    os.rmdir(path)


def test_writer_and_output_dir(bw):

    assert (
        os.path.isdir(path) and isinstance(bw, BatchWriter) and bw.delim == ';'
    )


def test_write_node_data_headers_import_call(bw):
    # four proteins, four miRNAs
    nodes = _get_nodes(8)

    passed_0 = bw.write(nodes[:4])
    passed_1 = bw.write(nodes[4:])
    bw.write_call()

    p_csv = os.path.join(path, 'Protein-header.csv')
    m_csv = os.path.join(path, 'Microrna-header.csv')
    call = os.path.join(path, 'neo4j-admin-import-call.sh')

    with open(p_csv) as f:
        p = f.read()
    with open(m_csv) as f:
        m = f.read()
    with open(call) as f:
        c = f.read()

    assert passed_0
    assert passed_1
    assert unformat(p) == (
        ':ID;id:string;id_type:string;name:string;'
        'score:double;taxon:long;:LABEL'
    )
    assert unformat(m) == (
        ':ID;id:string;id_type:string;name:string;taxon:long;:LABEL'
    )
    assert unformat(c) == unformat(
        'neo4j-admin import --database=neo4j --delimiter=";" '
        '--array-delimiter="|" --quote="\'" '
        '--skip-bad-relationships=false --skip-duplicate-nodes=false '
        f'--nodes="{path}/Protein-header.csv,{path}/Protein-part.*" '
        f'--nodes="{path}/Microrna-header.csv,{path}/Microrna-part.*"',
    )


def test_write_hybrid_ontology_nodes(bw):
    nodes = []
    for i in range(4):
        nodes.append(
            BioCypherNode(
                node_id=f'agpl:000{i}',
                node_label='altered gene product level',
                properties={},
            ),
        )

    passed = bw.write_nodes(nodes)

    h_csv = os.path.join(path, 'AlteredGeneProductLevel-header.csv')
    p_csv = os.path.join(path, 'AlteredGeneProductLevel-part000.csv')

    with open(h_csv) as f:
        header = f.read()

    with open(p_csv) as f:
        part = f.read()

    assert passed
    assert header == ':ID;id;preferred_id;:LABEL'
    assert part == (
        "agpl:0000;'agpl:0000';'id';AlteredGeneProductLevel|"
        'BiologicalEntity|Entity|FunctionalEffectVariant|GenomicEntity|'
        'Mixin|NamedThing|OntologyClass|PhysicalEssence|'
        'PhysicalEssenceOrOccurrent|SequenceVariant|ThingWithTaxon\n'
        "agpl:0001;'agpl:0001';'id';AlteredGeneProductLevel|BiologicalEntity|"
        'Entity|FunctionalEffectVariant|GenomicEntity|Mixin|NamedThing|'
        'OntologyClass|PhysicalEssence|PhysicalEssenceOrOccurrent|'
        'SequenceVariant|ThingWithTaxon\n'
        "agpl:0002;'agpl:0002';'id';AlteredGeneProductLevel|BiologicalEntity|"
        'Entity|FunctionalEffectVariant|GenomicEntity|Mixin|NamedThing|'
        'OntologyClass|PhysicalEssence|PhysicalEssenceOrOccurrent|'
        'SequenceVariant|ThingWithTaxon\n'
        "agpl:0003;'agpl:0003';'id';AlteredGeneProductLevel|BiologicalEntity|"
        'Entity|FunctionalEffectVariant|GenomicEntity|Mixin|NamedThing|'
        'OntologyClass|PhysicalEssence|PhysicalEssenceOrOccurrent|'
        'SequenceVariant|ThingWithTaxon\n'
    )


def test_tab_delimiter(tab_bw):

    nodes = _get_nodes(8)

    passed_n0 = tab_bw.write_nodes(nodes[:4])
    passed_n1 = tab_bw.write_nodes(nodes[4:])
    tab_bw.write_import_call()

    call_path = os.path.join(path, 'neo4j-admin-import-call.sh')

    with open(call_path) as f:

        the_call = f.read()

    assert passed_n0
    assert passed_n1
    assert unformat(the_call) == 'something...'


def test_property_types(bw):
    nodes = [
        Node(
            id=f'p{i+1}',
            label='protein',
            props={
                'score': 4 / (i + 1),
                'name': 'StringProperty1',
                'taxon': 9606,
                'genes': ['gene1', 'gene2'],
            },
        )
        for i in range(4)
    ]

    passed = bw.write(nodes, batch_size=1e6)

    d_csv = os.path.join(path, 'Protein-part000.csv')
    h_csv = os.path.join(path, 'Protein-header.csv')

    with open(d_csv) as f:
        data = f.read()

    with open(h_csv) as f:
        header = f.read()

    assert passed
    assert header == (
        ':ID;name;score:double;taxon:long;'
        'genes:string[];id;preferred_id;:LABEL'
    )
    assert data == (
        "p1;'StringProperty1';4.0;9606;'gene1|gene2';"
        "'p1';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p2;'StringProperty1';2.0;9606;'gene1|gene2';"
        "'p2';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p3;'StringProperty1';1.3333333333333333;9606;'gene1|gene2';"
        "'p3';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p4;'StringProperty1';1.0;9606;'gene1|gene2';"
        "'p4';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
    )


def test_write_node_data_from_list(bw):
    nodes = _get_nodes(4)

    passed = bw._write_records(nodes, batch_size=1e6)

    p_csv = os.path.join(path, 'Protein-part000.csv')
    m_csv = os.path.join(path, 'Microrna-part000.csv')

    with open(p_csv) as f:
        pr = f.read()

    with open(m_csv) as f:
        mi = f.read()

    assert passed

    assert pr == (
        "p1;'StringProperty1';4.0;9606;'gene1|gene2';"
        "'p1';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p2;'StringProperty1';2.0;9606;'gene1|gene2';"
        "'p2';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p3;'StringProperty1';1.3333333333333333;9606;'gene1|gene2';"
        "'p3';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p4;'StringProperty1';1.0;9606;'gene1|gene2';"
        "'p4';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
    )

    assert mi == (
        "m1;'StringProperty1';9606;"
        "'m1';'mirbase';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m2;'StringProperty1';9606;"
        "'m2';'mirbase';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m3;'StringProperty1';9606;"
        "'m3';'mirbase';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m4;'StringProperty1';9606;'m4';'mirbase';ChemicalEntity|"
        'ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
    )


def test_write_node_data_from_gen(bw):
    nodes = _get_nodes(4)

    node_gen = (n for n in nodes)

    passed = bw._write_records(node_gen, batch_size=1e6)

    p_csv = os.path.join(path, 'Protein-part000.csv')
    m_csv = os.path.join(path, 'Microrna-part000.csv')

    with open(p_csv) as f:
        pr = f.read()

    with open(m_csv) as f:
        mi = f.read()

    assert passed

    assert pr == (
        "p1;'StringProperty1';4.0;9606;'gene1|gene2';"
        "'p1';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p2;'StringProperty1';2.0;9606;'gene1|gene2';"
        "'p2';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p3;'StringProperty1';1.3333333333333333;9606;'gene1|gene2';"
        "'p3';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p4;'StringProperty1';1.0;9606;'gene1|gene2';"
        "'p4';'uniprot';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
    )

    assert mi == (
        "m1;'StringProperty1';9606;"
        "'m1';'mirbase';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m2;'StringProperty1';9606;"
        "'m2';'mirbase';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m3;'StringProperty1';9606;"
        "'m3';'mirbase';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m4;'StringProperty1';9606;"
        "'m4';'mirbase';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
    )


def test_write_node_data_from_gen_no_props(bw):

    nodes = [
        Node(
            id = f'{"m" if i % 2 else "p"}{i // 2 + 1}',
            label = 'microRNA' if i % 2 else 'protein',
            props = None if i % 2 else {
                'score': 4 / (i // 2 + 1),
                'name': 'StringProperty1',
                'taxon': 9606,
                'genes': ['gene1', 'gene2'],
            },
        )
        for i in range(8)
    ]

    node_gen = (n for n in nodes)

    passed = bw._write_records(node_gen, batch_size=1e6)

    p_csv = os.path.join(path, 'Protein-part000.csv')
    m_csv = os.path.join(path, 'Microrna-part000.csv')

    with open(p_csv) as f:
        pr = f.read()

    with open(m_csv) as f:
        mi = f.read()

    assert passed

    assert pr == (
        "p1;'StringProperty1';4.0;9606;'gene1|gene2';"
        "'p1';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p2;'StringProperty1';2.0;9606;'gene1|gene2';"
        "'p2';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p3;'StringProperty1';1.3333333333333333;9606;'gene1|gene2';"
        "'p3';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p4;'StringProperty1';1.0;9606;'gene1|gene2';"
        "'p4';'id';BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
    )

    assert mi == (
        "m1;'m1';'id';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m2;'m2';'id';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m3;'m3';'id';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
        "m4;'m4';'id';ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|"
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n'
    )


def test_write_node_data_from_large_gen(bw):

    nodes = _get_nodes(1e4 + 4)

    node_gen = (n for n in nodes)

    passed = bw._write_records(node_gen, batch_size=1e4)

    p0_csv = os.path.join(path, 'Protein-part000.csv')
    m0_csv = os.path.join(path, 'Microrna-part000.csv')
    p1_csv = os.path.join(path, 'Protein-part001.csv')
    m1_csv = os.path.join(path, 'Microrna-part001.csv')

    pr_lines = sum(1 for _ in open(p0_csv))
    mi_lines = sum(1 for _ in open(m0_csv))
    pr_lines1 = sum(1 for _ in open(p1_csv))
    mi_lines1 = sum(1 for _ in open(m1_csv))

    assert passed
    assert pr_lines == 1e4
    assert mi_lines == 1e4
    assert pr_lines1 == 4
    assert mi_lines1 == 4


def test_too_many_properties(bw):

    nodes = _get_nodes(1)

    bn1 = Node(
        id = 'p0',
        label = 'protein',
        props = {
            'p1': get_random_string(4),
            'p2': get_random_string(8),
            'p3': get_random_string(16),
            'p4': get_random_string(16),
        },
    )
    nodes.append(bn1)

    node_gen = (n for n in nodes)

    passed = bw._write_records(node_gen, batch_size = 1e4)

    assert not passed


def test_not_enough_properties(bw):

    nodes = _get_nodes(1)

    bn1 = Node(
        id='p0',
        label='protein',
        props={'p1': get_random_string(4)},
    )
    nodes.append(bn1)
    node_gen = (n for n in nodes)

    passed = bw._write_records(node_gen, batch_size = 1e4)

    p0_csv = os.path.join(path, 'Protein-part000.csv')

    assert not passed
    assert not os.path.exists(p0_csv)


def test_write_none_type_property_and_order_invariance(bw):

    # as introduced by translation using defined properties in
    # schema_config.yaml
    nodes = [
        Node(
            id = 'p1',
            label = 'protein',
            genes = None,
            props = {'taxon': 9606, 'score': 1, 'name': None},
        ),
        Node(
            id = 'p2',
            label='protein',
            genes = ['gene1', 'gene2'],
            props = {'name': None, 'score': 2, 'taxon': 9606},
        ),
        Node(
            id = 'm1',
            label = 'microRNA',
            props = {'name': None, 'taxon': 9606},
        ),
    ]

    node_gen = (n for n in nodes)

    passed = bw._write_records(node_gen, batch_size = 1e4)

    p0_csv = os.path.join(path, 'Protein-part000.csv')

    with open(p0_csv) as f:
        p = f.read()

    assert passed

    assert p == (
        "p1;;1;9606;;'p1';'id';BiologicalEntity|"
        'ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
        "p2;;2;9606;'gene1|gene2';'p2';'id';BiologicalEntity|"
        'ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
    )


def test_accidental_exact_batch_size(bw):

    nodes = _get_nodes(1e4)

    node_gen = (n for n in nodes)

    passed = bw.write(node_gen, batch_size = 1e4)

    p0_csv = os.path.join(path, 'Protein-part000.csv')
    m0_csv = os.path.join(path, 'Microrna-part000.csv')
    p1_csv = os.path.join(path, 'Protein-part001.csv')
    m1_csv = os.path.join(path, 'Microrna-part001.csv')

    pr_lines = sum(1 for _ in open(p0_csv))
    mi_lines = sum(1 for _ in open(m0_csv))

    ph_csv = os.path.join(path, 'Protein-header.csv')
    mh_csv = os.path.join(path, 'Microrna-header.csv')

    with open(ph_csv) as f:
        p_header = f.read()

    with open(mh_csv) as f:
        m_header = f.read()

    assert passed
    assert pr_lines == 1e4
    assert mi_lines == 1e4
    assert not os.path.exists(p1_csv)
    assert not os.path.exists(m1_csv)
    assert p_header == (
        ':ID;id:string;id_type:string;name:string;'
        'score:double;taxon:long;:LABEL'
    )
    assert m_header == (
        ':ID;id:string;id_type:string;'
        'name:string;taxon:long;:LABEL'
    )


def test_write_edge_data_from_gen(bw):

    edges = _get_edges(4)

    edge_gen = (e for e in edges)

    passed = bw._write_records(edge_gen, batch_size = 1e4)

    pid_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-part000.csv')
    imi_csv = os.path.join(path, 'IS_MUTATED_IN-part000.csv')

    with open(pid_csv) as f:
        pid_contents = f.read()

    with open(imi_csv) as f:
        imi_contents = f.read()

    assert passed

    assert pid_contents == (
        'p0;True;4;T253;0.78;p1;PERTURBED_IN_DISEASE\n'
        'p1;True;4;T253;0.78;p2;PERTURBED_IN_DISEASE\n'
        'p2;True;4;T253;0.78;p3;PERTURBED_IN_DISEASE\n'
        'p3;True;4;T253;0.78;p4;PERTURBED_IN_DISEASE'
    )
    assert imi_contents == (
        'm0;1;3-UTR;p1;IS_MUTATED_IN\n'
        'm1;1;3-UTR;p2;IS_MUTATED_IN\n'
        'm2;1;3-UTR;p3;IS_MUTATED_IN\n'
        'm3;1;3-UTR;p4;IS_MUTATED_IN'
    )


def test_write_edge_data_from_large_gen(bw):

    edges = _get_edges(1e4 + 4)

    edge_gen = (e for e in edges)

    passed = bw._write_records(edge_gen, batch_size = 1e4)

    apl0_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-part000.csv')
    ips0_csv = os.path.join(path, 'IS_MUTATED_IN-part000.csv')
    apl1_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-part001.csv')
    ips1_csv = os.path.join(path, 'IS_MUTATED_IN-part001.csv')

    pid_lines0 = sum(1 for _ in open(apl0_csv))
    imi_lines0 = sum(1 for _ in open(ips0_csv))
    pid_lines1 = sum(1 for _ in open(apl1_csv))
    imi_lines1 = sum(1 for _ in open(ips1_csv))

    assert passed
    assert pid_lines0 == 1e4
    assert imi_lines0 == 1e4
    assert pid_lines1 == 4
    assert imi_lines1 == 4


def test_write_edge_data_from_list(bw):

    edges = _get_edges(4)

    passed = bw._write_records(edges, batch_size = 1e4)
    pid_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-part000.csv')
    imi_csv = os.path.join(path, 'IS_MUTATED_IN-part000.csv')

    with open(pid_csv) as f:
        pid_contents = f.read()

    with open(imi_csv) as f:
        imi_contents = f.read()

    assert passed
    assert pid_contents == (
        'p0;True;4;T253;0.78;p1;PERTURBED_IN_DISEASE\n'
        'p1;True;4;T253;0.78;p2;PERTURBED_IN_DISEASE\n'
        'p2;True;4;T253;0.78;p3;PERTURBED_IN_DISEASE\n'
        'p3;True;4;T253;0.78;p4;PERTURBED_IN_DISEASE'
    )
    assert imi_contents == (
        'm0;1;3-UTR;p1;IS_MUTATED_IN\n'
        'm1;1;3-UTR;p2;IS_MUTATED_IN\n'
        'm2;1;3-UTR;p3;IS_MUTATED_IN\n'
        'm3;1;3-UTR;p4;IS_MUTATED_IN'
    )


def test_write_edge_data_from_list_no_props(bw):

    edges = [
        Edge(
            source = f'{"m" if i % 2 else "p"}{i // 2}',
            target = f'p{i // 2 + 1}',
            label = 'IS_MUTATED_IN' if i % 2 else 'PERTURBED_IN_DISEASE',
        )
        for i in range(8)
    ]

    passed = bw.write(edges, batch_size = 1e4)

    pid_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-part000.csv')
    imi_csv = os.path.join(path, 'IS_MUTATED_IN-part000.csv')

    assert not passed
    assert not os.path.exists(pid_csv)
    assert not os.path.exists(imi_csv)


def test_write_edge_data_headers_import_call(bw):

    edges = _get_edges(8)
    nodes = _get_nodes(8)

    edge_gen0 = (e for e in edges[:4])
    edge_gen1 = (e for e in edges[4:])

    passed_e0 = bw.write(edge_gen0)
    passed_e1 = bw.write(edge_gen1)
    passed_n = bw.write(nodes)

    bw.write_call()

    pid_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-header.csv')
    imi_csv = os.path.join(path, 'IS_MUTATED_IN-header.csv')
    call_sh = os.path.join(path, 'neo4j-admin-import-call.sh')

    with open(pid_csv) as f:
        pid_header = f.read()

    with open(imi_csv) as f:
        imi_header = f.read()

    with open(call_sh) as f:
        the_call = f.read()

    assert passed_e0
    assert passed_e1
    assert passed_n
    assert pid_header == (
        ':START_ID;directional:boolean;level:long;'
        'residue:string;score:double;:END_ID;:TYPE'
    )
    assert imi_header == ':START_ID;confidence:long;site:string;:END_ID;:TYPE'
    assert unformat(the_call) == (
        'neo4j-admin import --database=neo4j --delimiter=";" '
        '--array-delimiter="|" --quote="\'" --skip-bad-relationships=false '
        '--skip-duplicate-nodes=false '
        '--relationships="'
            f'{path}/PERTURBED_IN_DISEASE-header.csv,'
            f'{path}/PERTURBED_IN_DISEASE-part.*" '
        '--relationships="'
            f'{path}/IS_MUTATED_IN-header.csv,'
            f'{path}/IS_MUTATED_IN-part.*" '
        '--nodes="'
            f'{path}/Protein-header.csv,'
            f'{path}/Protein-part.*" '
        '--nodes="'
            f'{path}/Microrna-header.csv,'
            f'{path}/Microrna-part.*"'
    )


def test_write_duplicate_edges(bw):

    edges = _get_edges(4)
    edges.append(edges[0])

    passed = bw.write(edges)

    pid_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-part000.csv')
    imi_csv = os.path.join(path, 'IS_MUTATED_IN-part000.csv')

    lnum_pid = sum(1 for _ in open(pid_csv))
    lnum_imi = sum(1 for _ in open(imi_csv))

    assert passed
    assert lnum_pid == 4
    assert lnum_imi == 4


def test_relasnode_implementation(bw):

    trips = _get_rel_as_nodes(4) # what is trips?
    passed = bw.write(l for l in trips)

    iso_csv = os.path.join(path, 'IS_SOURCE_OF-part000.csv')
    ito_csv = os.path.join(path, 'IS_TARGET_OF-part000.csv')
    pti_csv = os.path.join(path, 'PostTranslationalInteraction-part000.csv')

    with open(iso_csv) as f:
        iso_contents = f.read()

    with open(ito_csv) as f:
        ito_contents = f.read()

    with open(pti_csv) as f:
        pti_contents = f.read()

    assert passed

    assert iso_contents == (
        'i1;p1;IS_SOURCE_OF\n'
        'i2;p2;IS_SOURCE_OF\n'
        'i3;p3;IS_SOURCE_OF\n'
        'i4;p4;IS_SOURCE_OF'
    )

    assert ito_contents == (
        'i0;p2;IS_TARGET_OF\n'
        'i1;p3;IS_TARGET_OF\n'
        'i2;p4;IS_TARGET_OF\n'
        'i3;p5;IS_TARGET_OF'
    )

    assert pti_contents == (
        "i1;True;-1;'i1';'id';Association|Entity|GeneToGeneAssociation|"
        'PairwiseGeneToGeneInteraction|PairwiseMolecularInteraction|'
        'PostTranslationalInteraction\n'
        "i2;True;-1;'i2';'id';Association|Entity|GeneToGeneAssociation|"
        'PairwiseGeneToGeneInteraction|PairwiseMolecularInteraction|'
        'PostTranslationalInteraction\n'
        "i3;True;-1;'i3';'id';Association|Entity|GeneToGeneAssociation|"
        'PairwiseGeneToGeneInteraction|PairwiseMolecularInteraction|'
        'PostTranslationalInteraction\n'
        "i4;True;-1;'i4';'id';Association|Entity|GeneToGeneAssociation|"
        'PairwiseGeneToGeneInteraction|PairwiseMolecularInteraction|'
        'PostTranslationalInteraction\n'
    )


def test_relasnode_overwrite_behaviour(bw):

    # if rel as node is called from successive write calls, SOURCE_OF,
    # TARGET_OF, and PART_OF should be continued, not overwritten
    trips = _get_rel_as_nodes(8)
    passed_n0 = bw.write(n for n in trips[:5])
    passed_n1 = bw.write(n for n in trips[5:])

    iso_csv = os.path.join(path, 'IS_SOURCE_OF-part001.csv')

    assert passed_n0
    assert passed_n1
    assert os.path.exists(iso_csv)


def test_write_mixed_edges(bw):

    mixed = _get_mixed_edges(4)
    passed = bw.write(e for e in mixed)

    pti_csv = os.path.join(path, 'PostTranslationalInteraction-header.csv')
    iso_csv = os.path.join(path, 'IS_SOURCE_OF-header.csv')
    ito_csv = os.path.join(path, 'IS_TARGET_OF-header.csv')
    pid_csv = os.path.join(path, 'PERTURBED_IN_DISEASE-header.csv')

    assert passed
    assert os.path.exists(pti_csv)
    assert os.path.exists(iso_csv)
    assert os.path.exists(ito_csv)
    assert os.path.exists(pid_csv)


def test_create_import_call(bw):

    mixed = _get_mixed_edges(4)
    passed = bw.write(e for e in mixed)
    call = bw.compile_call()

    assert passed
    assert unformat(call) == (
        'neo4j-admin import --database=neo4j --delimiter=";" '
        '--array-delimiter="|" --quote="\'" --skip-bad-relationships=false '
        '--skip-duplicate-nodes=false '
        '--nodes="'
            f'{path}/PostTranslationalInteraction-header.csv,'
            f'{path}/PostTranslationalInteraction-part.*" '
        '--relationships="'
            f'{path}/PERTURBED_IN_DISEASE-header.csv,'
            f'{path}/PERTURBED_IN_DISEASE-part.*" '
        '--relationships="'
            f'{path}/IS_SOURCE_OF-header.csv,'
            f'{path}/IS_SOURCE_OF-part.*" '
        '--relationships="'
            f'{path}/IS_TARGET_OF-header.csv,'
            f'{path}/IS_TARGET_OF-part.*"'
    )


def test_write_offline():

    d = Driver(
        offline = True,
        user_schema_config_path = 'biocypher/_config/test_schema_config.yaml',
        delimiter = ',',
        array_delimiter = '|',
        output_directory = path,
    )

    nodes = _get_nodes(4)

    passed = d.write_csv(items = nodes, dirname = path)

    p_csv = os.path.join(path, 'Protein-part000.csv')
    m_csv = os.path.join(path, 'Microrna-part000.csv')

    with open(p_csv) as f:
        pr = f.read()

    with open(m_csv) as f:
        mi = f.read()

    assert passed

    assert pr == (
        'p1,"StringProperty1",4.0,9606,"gene1|gene2",'
        '"p1","uniprot",BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n"'
        '"p2,"StringProperty1",2.0,9606,"gene1|gene2",'
        '"p2","uniprot",BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n"'
        '"p3,"StringProperty1",1.3333333333333333,9606,"gene1|gene2",'
        '"p3","uniprot",BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n"'
        '"p4,"StringProperty1",1.0,9606,"gene1|gene2",'
        '"p4","uniprot",BiologicalEntity|ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n"'
    )

    assert mi == (
        'm1,"StringProperty1",9606,'
        '"m1","mirbase",ChemicalEntity|ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n"'
        '"m2,"StringProperty1",9606,"m2","mirbase",ChemicalEntity|'
        'ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n"'
        '"m3,"StringProperty1",9606,"m3","mirbase",ChemicalEntity|'
        'ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|'
        'MolecularEntity|NamedThing|NoncodingRNAProduct|NucleicAcidEntity|'
        'OntologyClass|PhysicalEssence|PhysicalEssenceOrOccurrent|'
        'RNAProduct|ThingWithTaxon|Transcript\n"'
        '"m4,"StringProperty1",9606,"m4","mirbase",ChemicalEntity|'
        'ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|ChemicalOrDrugOrTreatment|'
        'Entity|GeneOrGeneProduct|GeneProductMixin|GenomicEntity|'
        'MacromolecularMachineMixin|MicroRNA|Mixin|MolecularEntity|'
        'NamedThing|NoncodingRNAProduct|NucleicAcidEntity|OntologyClass|'
        'PhysicalEssence|PhysicalEssenceOrOccurrent|RNAProduct|'
        'ThingWithTaxon|Transcript\n"'
    )


def test_duplicate_id(bw):

    csv_path = os.path.join(path, 'Protein-part000.csv')

    if os.path.exists(csv_path):
        os.remove(csv_path)

    nodes = [
        Node(
            id = 'p1',
            label = 'protein',
            genes = ['gene1', 'gene2'],
            props = {
                'name': 'StringProperty1',
                'score': 4.32,
                'taxon': 9606,
            },
        )
        for _ in range(2)
    ]

    passed = bw.write(nodes)

    with open(csv_path) as fp:

        numof_lines = sum(1 for _ in fp)

    assert passed
    assert numof_lines == 1


def test_write_synonym(bw):

    csv_path = os.path.join(path, 'Complex-part000.csv')

    if os.path.exists(csv_path):

        os.remove(csv)

    nodes = [
        Node(
            node_id = f'p{i + 1}',
            node_label = 'complex',
            properties = {
                'name': 'StringProperty1',
                'score': 4.32,
                'taxon': 9606,
            },
        )
        for i in range(4)
    ]

    passed = bw.write_nodes(nodes)

    with open(csv_path) as f:

        comp = f.read()

    assert passed
    assert os.path.exists(csv)

    assert comp == (
        "p1;'StringProperty1';4.32;9606;'p1';'id';"
        'Complex|Entity|MacromolecularMachineMixin|Mixin\n'
        "p2;'StringProperty1';4.32;9606;'p2';'id';"
        'Complex|Entity|MacromolecularMachineMixin|Mixin\n'
        "p3;'StringProperty1';4.32;9606;'p3';'id';"
        'Complex|Entity|MacromolecularMachineMixin|Mixin\n'
        "p4;'StringProperty1';4.32;9606;'p4';'id';"
        'Complex|Entity|MacromolecularMachineMixin|Mixin\n'
    )


def test_duplicate_nodes(bw):
    nodes = _get_nodes(4)
    nodes.append(
        BioCypherNode(
            node_id='p1',
            node_label='protein',
            properties={
                'name': 'StringProperty1',
                'score': 4.32,
                'taxon': 9606,
                'genes': ['gene1', 'gene2'],
            },
        ),
    )

    passed = bw.write_nodes(nodes)

    assert 'protein' in bw.duplicate_node_types
    assert 'p1' in bw.duplicate_node_ids


def test_duplicate_edges(bw):
    edges = _get_edges(4)
    edges.append(
        BioCypherEdge(
            source_id='p1',
            target_id='p2',
            relationship_label='PERTURBED_IN_DISEASE',
        ),
    )

    passed = bw.write_edges(edges)

    assert 'PERTURBED_IN_DISEASE' in bw.duplicate_edge_types
    assert 'p1_p2' in bw.duplicate_edge_ids



def test_get_duplicate_edges(bw):
    edges = _get_edges(4)
    edges.append(
        BioCypherEdge(
            source_id='p1',
            target_id='p2',
            relationship_label='PERTURBED_IN_DISEASE',
        ),
    )

    bw.write_edges(edges)

    d = bw.get_duplicate_edges()
    types = d[0]
    ids = d[1]

    assert 'PERTURBED_IN_DISEASE' in types
    assert 'p1_p2' in ids


def test_write_strict(bw_strict):

    n1 = BioCypherNode(
        node_id='p1',
        node_label='protein',
        properties={
            'name': 'StringProperty1',
            'score': 4.32,
            'taxon': 9606,
            'genes': ['gene1', 'gene2'],
            'source': 'source1',
            'version': 'version1',
            'licence': 'licence1',
        },
    )

    passed = bw_strict.write_nodes([n1])

    assert passed

    csv = os.path.join(path_strict, 'Protein-part000.csv')

    with open(csv) as f:
        prot = f.read()

    assert prot == (
        "p1;'StringProperty1';4.32;9606;'gene1|gene2';"
        "'p1';'id';'source1';'version1';'licence1';BiologicalEntity|"
        'ChemicalEntityOrGeneOrGeneProduct|'
        'ChemicalEntityOrProteinOrPolypeptide|Entity|GeneOrGeneProduct|'
        'GeneProductMixin|MacromolecularMachineMixin|Mixin|NamedThing|'
        'Polypeptide|Protein|ThingWithTaxon\n'
    )
