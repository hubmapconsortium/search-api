from hubmap_translation.addl_index_transformations.portal.add_donor_demographics import (
    add_donor_demographics,
)


def _donor(mapped_metadata):
    return {'mapped_metadata': mapped_metadata}


def test_aggregates_categorical_and_numeric_across_donors():
    doc = {
        'entity_type': 'Dataset',
        'donors': [
            _donor({
                'sex': ['Male'],
                'race': ['White'],
                'age_value': [30.0],
                'age_unit': ['years'],
                'height_value': [180.0],
                'height_unit': ['cm'],
                'body_mass_index_value': [22.0],
                'body_mass_index_unit': ['kg/m^2'],
            }),
            _donor({
                'sex': ['Female'],
                'race': ['White', 'Asian'],
                'age_value': [70.0],
                'age_unit': ['years'],
                'height_value': [160.0],
                'height_unit': ['cm'],
                'body_mass_index_value': [30.0],
                'body_mass_index_unit': ['kg/m^2'],
            }),
            _donor({
                'sex': ['Female'],
                'race': ['Black or African American'],
                'age_value': [50.0],
                'age_unit': ['years'],
                'height_value': [170.0],
                'height_unit': ['cm'],
                'body_mass_index_value': [26.0],
                'body_mass_index_unit': ['kg/m^2'],
            }),
        ],
    }

    add_donor_demographics(doc)
    demographics = doc['donor_demographics']

    # Categorical fields: deduplicated, sorted union across all donors.
    assert demographics['sex'] == ['Female', 'Male']
    assert demographics['race'] == ['Asian', 'Black or African American', 'White']

    # Numeric fields: sorted array of every value (range-filterable) ...
    assert demographics['age_value'] == [30.0, 50.0, 70.0]
    assert demographics['height_value'] == [160.0, 170.0, 180.0]
    assert demographics['body_mass_index_value'] == [22.0, 26.0, 30.0]

    # ... plus a min/max/mean stats object for display.
    assert demographics['age'] == {'min': 30.0, 'max': 70.0, 'mean': 50.0}
    assert demographics['height'] == {'min': 160.0, 'max': 180.0, 'mean': 170.0}
    assert demographics['body_mass_index'] == {'min': 22.0, 'max': 30.0, 'mean': 26.0}

    # Units collected uniquely.
    assert demographics['age_unit'] == ['years']
    assert demographics['height_unit'] == ['cm']
    assert demographics['body_mass_index_unit'] == ['kg/m^2']


def test_mean_is_rounded():
    doc = {
        'entity_type': 'Dataset',
        'donors': [
            _donor({'age_value': [40.0], 'age_unit': ['years']}),
            _donor({'age_value': [41.0], 'age_unit': ['years']}),
            _donor({'age_value': [43.0], 'age_unit': ['years']}),
        ],
    }
    add_donor_demographics(doc)
    # (40 + 41 + 43) / 3 = 41.333... -> rounded to 2 dp.
    assert doc['donor_demographics']['age']['mean'] == 41.33


def test_numeric_field_without_units_is_normalized_to_value_array_and_stats():
    # translate stores a unit-less numeric field under the bare "<field>" key (a float
    # list), not "<field>_value". It must still become a range-filterable array + stats.
    doc = {
        'entity_type': 'Dataset',
        'donors': [
            _donor({'age': [40.0]}),
            _donor({'age': [40.0]}),
            _donor({'age': [70.0]}),
        ],
    }
    add_donor_demographics(doc)
    demographics = doc['donor_demographics']
    # Duplicate values are deduped in the array but still counted in the mean.
    assert demographics['age_value'] == [40.0, 70.0]
    assert demographics['age'] == {'min': 40.0, 'max': 70.0, 'mean': 50.0}
    assert 'age_unit' not in demographics


def test_same_field_with_and_without_units_merges():
    # One donor reports units, another does not -> both values feed one numeric field.
    doc = {
        'entity_type': 'Dataset',
        'donors': [
            _donor({'age_value': [40.0], 'age_unit': ['years']}),
            _donor({'age': [70.0]}),
        ],
    }
    add_donor_demographics(doc)
    demographics = doc['donor_demographics']
    assert demographics['age_value'] == [40.0, 70.0]
    assert demographics['age'] == {'min': 40.0, 'max': 70.0, 'mean': 55.0}
    assert demographics['age_unit'] == ['years']


def test_single_donor_is_a_donors_list_of_one():
    doc = {
        'entity_type': 'Dataset',
        'donors': [_donor({'sex': ['Male'], 'race': ['White']})],
    }
    add_donor_demographics(doc)
    assert doc['donor_demographics'] == {'race': ['White'], 'sex': ['Male']}


def test_aggregates_only_from_donors_list_ignoring_single_donor_key():
    doc = {
        'entity_type': 'Dataset',
        'donor': _donor({'sex': ['Other']}),  # legacy key must be ignored
        'donors': [
            _donor({'sex': ['Male']}),
            _donor({'sex': ['Female']}),
        ],
    }
    add_donor_demographics(doc)
    assert doc['donor_demographics']['sex'] == ['Female', 'Male']


def test_applies_to_sample_and_publication():
    for entity_type in ('Sample', 'Publication'):
        doc = {
            'entity_type': entity_type,
            'donors': [_donor({'sex': ['Female']})],
        }
        add_donor_demographics(doc)
        assert doc['donor_demographics'] == {'sex': ['Female']}


def test_empty_metadata_produces_empty_demographics():
    doc = {'entity_type': 'Dataset', 'donors': [_donor({})]}
    add_donor_demographics(doc)
    assert doc['donor_demographics'] == {}


def test_skips_non_demographic_entity_types():
    doc = {'entity_type': 'Donor', 'mapped_metadata': {'sex': ['Male']}}
    add_donor_demographics(doc)
    assert 'donor_demographics' not in doc


def test_skips_when_donors_list_is_empty_or_absent():
    for doc in ({'entity_type': 'Dataset'}, {'entity_type': 'Dataset', 'donors': []}):
        add_donor_demographics(doc)
        assert 'donor_demographics' not in doc
