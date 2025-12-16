from hubmap_translation.addl_index_transformations.portal.add_assay_details import CreationAction

# Determines whether to display the raw dataset-focused UI or the processed outline-focused UI
# Must be run after add_counts transformation, since it relies on ancestor_counts
def add_is_integrated(doc):
    # only datasets can be integrated
    if doc.get('entity_type') != 'Dataset':
        return

    # EPIC datasets are always considered integrated (including segmentation masks):
    # - they can be derived from multiple datasets
    # - they are always processed
    # - they always have visualization support
    # - they have their own list of contributors that may differ from the base dataset's contributors
    if (doc.get('creation_action') == CreationAction.EPIC):
        doc['is_integrated'] = True
        return

    # If a dataset is immediately descended from multiple datasets, it is considered integrated
    # This covers Snare-Seq2 experiments
    if (doc.get('ancestor_counts')):
        if (doc['ancestor_counts'].get('entity_type', {}).get('Dataset', 0) > 1):
            doc['is_integrated'] = True
            return
        
    doc['is_integrated'] = False