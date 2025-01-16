# TODO migrate as appropriate from test translate


def test_inheritance_loop(ontology_mapping):
    assert "gene to variant association" in ontology_mapping.schema.keys()

    assert "gene to variant association" not in ontology_mapping.extended_schema.keys()


def test_virtual_leaves_node(ontology_mapping):
    assert "wikipathways.pathway" in ontology_mapping.extended_schema


def test_getting_properties_via_config(ontology_mapping):
    assert "name" in ontology_mapping.extended_schema["protein"].get("properties").keys()


def test_preferred_id_optional(ontology_mapping):
    pti = ontology_mapping.extended_schema.get("post translational interaction")

    assert pti.get("preferred_id") == "id"
