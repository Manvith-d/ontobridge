from ontobridge.profiler import infer_type
from ontobridge.text import same_word, tokens


def test_tokens_split_messy_identifiers():
    assert tokens("claimant.customerRef") == ["claimant", "customer", "ref"]
    assert tokens("PREM_AMT") == ["prem", "amt"]
    assert tokens("category_lvl1") == ["category", "lvl", "1"]
    assert tokens("isFraudFlag") == ["is", "fraud", "flag"]


def test_same_word_matches_compound_heads_only():
    assert same_word("holder", "policyholder")
    assert not same_word("claim", "claimant")  # a prefix is a different word
    assert not same_word("id", "paid")  # too short to be a compound head


def test_infer_type():
    assert infer_type(["2024-01-02", "2023-12-31"]) == "date"
    assert infer_type(["2024-07-30T11:46:08Z"]) == "dateTime"
    assert infer_type(["3", "14"]) == "integer"
    assert infer_type(["3", "14.5"]) == "decimal"
    assert infer_type(["00123", "00456"]) == "string"  # leading zeros make it a code
    assert infer_type(["1234567890"]) == "string"  # NPIs and card numbers are identifiers
    assert infer_type([True, False]) == "boolean"
    assert infer_type([]) == "string"
