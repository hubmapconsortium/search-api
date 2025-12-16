from hubmap_translation.addl_index_transformations.portal import transform
from hubmap_translation.addl_index_transformations.portal.add_assay_details import (
    CreationAction,
)


input_doc = {
    "uuid": "69c70762689b20308bb049ac49653342",
    "entity_type": "Dataset",
    "status": "New",
    "group_name": "EXT - Outside HuBMAP",
    "origin_samples": [{"organ": "LY"}],
    "create_timestamp": 1575489509656,
    "creation_action": "Central Process",
    "ancestor_ids": ["1234", "5678"],
    "ancestors": [
        {
            "sample_category": "section",
            "created_by_user_displayname": "daniel Cotter",
            "entity_type": "Sample",
        }
    ],
    "data_access_level": "consortium",
    "dataset_type": "RNAseq [Salmon]",
    "descendants": [{"entity_type": "Sample or Dataset"}],
    "donor": {
        "metadata": {
            "organ_donor_data": [
                {
                    "data_type": "Nominal",
                    "grouping_concept_preferred_term": "Sex",
                    "preferred_term": "Male",
                }
            ]
        }
    },
    "files": [
        {
            "description": "OME-TIFF pyramid file",
            "edam_term": "EDAM_1.24.format_3727",
            "is_qa_qc": False,
            "rel_path": "ometiff-pyramids/stitched/expressions/reg1_stitched_expressions.ome.tif",
            "size": 123456789,
            "type": "unknown",
        }
    ],
    "ingest_metadata": {
        "dag_provenance_list": [],
    },
    "metadata": {
        "analyte_class": "RNA",
        "_random_stuff_that_should_not_be_ui": "No!",
        "collectiontype": "No!",
        "data_path": "No!",
        "metadata_path": "No!",
        "tissue_id": "No!",
        "donor_id": "No!",
        "cell_barcode_size": "123",
        "should_be_int": "123",
        "should_be_float": "123.456",
        "keep_this_field": "Yes!",
        "is_boolean": "1",
    },
    "rui_location": '{"ccf_annotations": ["http://purl.obolibrary.org/obo/UBERON_0001157"]}',
}

expected_output_doc = {
    "analyte_class": "RNA",
    "anatomy_0": ["body"],
    "anatomy_1": ["large intestine", "lymph node"],
    "anatomy_2": ["transverse colon"],
    "ancestor_counts": {"entity_type": {"Sample": 1}},
    "ancestor_ids": ["1234", "5678"],
    "ancestors": [
        {
            "created_by_user_displayname": "Daniel Cotter",
            "mapped_sample_category": "Section",
            "sample_category": "section",
            "entity_type": "Sample",
        }
    ],
    "assay_display_name": ["scRNA-seq (10x Genomics) [Salmon]"],
    "assay_modality": "single",
    "create_timestamp": 1575489509656,
    "creation_action": "Central Process",
    "data_access_level": "consortium",
    "dataset_type": "RNAseq [Salmon]",
    "descendant_counts": {"entity_type": {"Sample or Dataset": 1}},
    "descendants": [{"entity_type": "Sample or Dataset"}],
    "donor": {
        "mapped_metadata": {"sex": ["Male"]},
        "metadata": {
            "organ_donor_data": [
                {
                    "data_type": "Nominal",
                    "grouping_concept_preferred_term": "Sex",
                    "preferred_term": "Male",
                }
            ]
        },
    },
    "entity_type": "Dataset",
    "files": [
        {
            "description": "OME-TIFF pyramid file",
            "edam_term": "EDAM_1.24.format_3727",
            "is_qa_qc": False,
            "mapped_description": "OME-TIFF pyramid file (TIF file)",
            "rel_path": "ometiff-pyramids/stitched/expressions/reg1_stitched_expressions.ome.tif",
            "size": 123456789,
            "type": "unknown",
        }
    ],
    "group_name": "EXT - Outside HuBMAP",
    "ingest_metadata": {
        "dag_provenance_list": [],
    },
    "is_integrated": False,
    "mapped_consortium": "Outside HuBMAP",
    "mapped_create_timestamp": "2019-12-04 19:58:29",
    "mapped_data_access_level": "Consortium",
    "mapped_data_types": ["scRNA-seq (10x Genomics) [Salmon]"],
    "mapped_external_group_name": "Outside HuBMAP",
    "mapped_metadata": {},
    "mapped_status": "New",
    "metadata": {
        "analyte_class": "RNA",
        "cell_barcode_size": "123",
        "is_boolean": "TRUE",
        "keep_this_field": "Yes!",
        "should_be_float": 123.456,
        "should_be_int": 123,
    },
    "origin_samples": [{"mapped_organ": "Lymph Node", "organ": "LY"}],
    "origin_samples_unique_mapped_organs": ["Lymph Node"],
    "pipeline": "Salmon",
    "processing": "processed",
    "processing_type": "hubmap",
    "raw_dataset_type": "RNAseq",
    "rui_location": '{"ccf_annotations": '
    '["http://purl.obolibrary.org/obo/UBERON_0001157"]}',
    "status": "New",
    "uuid": "69c70762689b20308bb049ac49653342",
    "soft_assaytype": "salmon_rnaseq_10x",
    "vitessce-hints": ["is_sc", "rna"],
    "visualization": True,
}


input_doc_integrated_multiple_ancestors = input_doc.copy()
input_doc_integrated_multiple_ancestors.update(
    {
        "ancestors": input_doc["ancestors"]
        + [{"entity_type": "Dataset"}, {"entity_type": "Dataset"}]
    }
)

expected_output_doc_integrated_multiple_ancestors = expected_output_doc.copy()
expected_output_doc_integrated_multiple_ancestors.update(
    {
        "ancestor_counts": {"entity_type": {"Sample": 1, "Dataset": 2}},
        "ancestors": [*expected_output_doc["ancestors"], {"entity_type": "Dataset"}, {"entity_type": "Dataset"}],
        "is_integrated": True,
    }
)

input_doc_integrated_epic = input_doc.copy()
input_doc_integrated_epic.update(
    {"creation_action": CreationAction.EPIC}
)

expected_output_doc_integrated_epic = expected_output_doc.copy()
expected_output_doc_integrated_epic.update(
    {
        "is_integrated": True,
    }
)


def mock_response(response_to_mock, status_code=200, text="Logger call requires this"):
    class MockResponse:
        def __init__(self):
            self.status_code = status_code
            self.text = text

        def json(self):
            return response_to_mock

        def raise_for_status(self):
            pass

    return MockResponse()


def mock_soft_assay(uuid=None, headers=None):
    return mock_response(
        {
            "assaytype": "salmon_rnaseq_10x",
            "contains-pii": False,
            "pipeline-shorthand": "Salmon",
            "description": "scRNA-seq (10x Genomics) [Salmon]",
            "primary": False,
            "vitessce-hints": ["is_sc", "rna"],
        }
    )


# Helper function to reduce code duplication while testing above cases
def _test_transform_helper(input, expected, resources):
    actual = transform(input, resources)
    del actual["mapper_metadata"]
    assert actual == expected


def test_transform(mocker):
    mocker.patch("requests.get", side_effect=mock_soft_assay)
    transformation_resources = {
        "ingest_api_soft_assay_url": "abc123",
        "token": "def456",
        "organ_map": {
            "LY": {
                "rui_code": "LY",
                "organ_uberon": "UBERON:0000029",
                "term": "Lymph Node",
            }
        },
    }

    _test_transform_helper(input_doc, expected_output_doc, transformation_resources)
    _test_transform_helper(
        input_doc_integrated_multiple_ancestors,
        expected_output_doc_integrated_multiple_ancestors,
        transformation_resources,
    )
    _test_transform_helper(
        input_doc_integrated_epic,
        expected_output_doc_integrated_epic,
        transformation_resources,
    )
