import pytest

from hubmap_translation.addl_index_transformations.portal.add_partonomy import _get_organ_iri


@pytest.mark.parametrize(
    "doc, expected_organ_iri",
    [
        pytest.param(
            {}, None, id="empty doc"
        ),
        pytest.param(
            {"foo": "bar"}, None, id="missing origin_samples"
        ),
        pytest.param(
            {"origin_samples": []}, None, id="empty origin_samples"
        ),
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
