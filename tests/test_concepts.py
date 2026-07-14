from src.concepts import CONDITION_CONCEPT_ID, CONDITION_NAMES, DRUG_CONCEPT_ID, DRUG_NAMES


def test_every_condition_id_has_a_name():
    assert set(CONDITION_CONCEPT_ID.values()) == set(CONDITION_NAMES.keys())


def test_every_drug_id_has_a_name():
    assert set(DRUG_CONCEPT_ID.values()) == set(DRUG_NAMES.keys())
