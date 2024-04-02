import pytest

from hubmap_translation.addl_index_transformations.portal.add_partonomy import _get_organ_iri


@pytest.mark.parametrize(
    "doc, expected_organ_iri",
    [
        pytest.param(
            {"origin_samples": [{"organ": "UT"}]}, None, id="valid organ"
        ),
        pytest.param(
            {"origin_samples": [{"organ": "XX"}]}, None, id="invalid organ"
        ),
        pytest.param(
            {"origin_samples": [{"fake": "XX"}]}, None, id="missing organ"
        ),
    ]
)
def test_get_organ_iri(doc, expected_organ_iri):
    organ_iri = _get_organ_iri(doc)
    assert organ_iri == expected_organ_iri


@pytest.mark.parametrize(
    "doc",
    [
        pytest.param(
            {}, id="empty doc"
        ),
        pytest.param(
            {"uuid": "test_dataset_uuid", "foo": "bar"}, id="missing origin_samples"
        ),
        pytest.param(
            {"uuid": "test_dataset_uuid", "origin_samples": []}, id="empty origin_samples"
        ),
    ]
)
def test_get_organ_iri_invalid_doc_handling(doc):
    with pytest.raises(RuntimeWarning) as excinfo:
        _get_organ_iri(doc)
    assert "Missing or empty 'origin_samples'." in str(excinfo.value)
