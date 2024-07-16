import pytest

from hubmap_translation.addl_index_transformations.portal.add_partonomy import _get_organ_iri
from hubmap_translation.addl_index_transformations.portal.translate import TranslationException


@pytest.mark.parametrize(
    "doc, expected_organ_iri",
    [
        pytest.param(
            {}, None, id="empty doc does not throw exception"
        ),
        pytest.param(
            {"uuid": "test_uuid", "entity_type": "Donor"}, None, id="Donor is expected to have missing origin_samples"
        ),
        pytest.param(
            {"uuid": "organ_sample", "entity_type": "Sample", "sample_category": "organ"}, None, id="Organ sample is expected to have missing origin_samples"
        ),
        pytest.param(
            {"origin_samples": [{"organ": "XX"}]}, None, id="invalid organ"
        ),
        pytest.param(
            {"origin_samples": [{"fake": "XX"}]}, None, id="missing organ"
        ),
        pytest.param(
            {"uuid": "organ_sample", "entity_type": "Sample",
                "sample_category": "block", "origin_samples": [{"organ": "HT"}]},
            "http://purl.obolibrary.org/obo/UBERON_0000948", id="Block sample with valid organ"
        ),
    ]
)
def test_get_organ_iri(doc, expected_organ_iri):
    organ_iri = _get_organ_iri(doc, {'HT': {'organ_uberon': 'UBERON_0000948'}})
    assert organ_iri == expected_organ_iri


@pytest.mark.parametrize(
    "doc",
    [
        pytest.param(
            {"uuid": "test_dataset_uuid", "origin_samples": []}, id="empty origin_samples"
        ),
    ]
)
def test_get_organ_iri_invalid_doc_handling(doc):
    with pytest.raises(TranslationException) as excinfo:
        _get_organ_iri(doc, {})
    assert "Invalid document" in str(excinfo.value)
    assert "Missing or empty" in str(excinfo.value)
