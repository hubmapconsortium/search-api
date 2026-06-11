from collections import defaultdict

# Entity types that carry one or more donors and should get an aggregated
# "donor_demographics" object. (Donor entities use their own top-level
# "mapped_metadata" instead, so they are intentionally excluded.)
DEMOGRAPHIC_ENTITY_TYPES = {'Dataset', 'Sample', 'Publication'}


def add_donor_demographics(doc):
    '''
    Aggregate donor demographics across *all* donors of a Dataset/Sample/Publication
    into a single top-level "donor_demographics" object, so multi-donor entities
    can be accurately filtered and displayed (rather than only reflecting donor #0).

    Each donor already has a "mapped_metadata" dict (produced by
    translate._donor_metadata_map). There a numeric field is stored as
    "<field>_value" + "<field>_unit" lists when it has units, but under the bare
    "<field>" key (a list of numbers) when it does not; categorical fields are stored
    under the bare "<field>" key as a list of terms. We therefore treat a
    "<field>_value" key -- or a bare key whose values are all numeric -- as numeric
    (producing a range-filterable "<field>_value" array plus a min/max/mean "<field>"
    stats object), and any other bare key as a categorical set.

    Categorical fields become a deduplicated, sorted set across all donors:

    >>> from pprint import pprint
    >>> doc = {
    ...     'entity_type': 'Dataset',
    ...     'donors': [
    ...         {'mapped_metadata': {'sex': ['Male'], 'race': ['White']}},
    ...         {'mapped_metadata': {'sex': ['Female'], 'race': ['White', 'Asian']}},
    ...     ],
    ... }
    >>> add_donor_demographics(doc)
    >>> pprint(doc['donor_demographics'])
    {'race': ['Asian', 'White'], 'sex': ['Female', 'Male']}

    Numeric fields keep the sorted set of all donors' values (so an ES range query
    matches if *any* donor falls in range) plus a min/max/mean stats object for
    display; units are collected uniquely:

    >>> doc = {
    ...     'entity_type': 'Dataset',
    ...     'donors': [
    ...         {'mapped_metadata': {'age_value': [30.0], 'age_unit': ['years']}},
    ...         {'mapped_metadata': {'age_value': [70.0], 'age_unit': ['years']}},
    ...         {'mapped_metadata': {'age_value': [50.0], 'age_unit': ['years']}},
    ...     ],
    ... }
    >>> add_donor_demographics(doc)
    >>> pprint(doc['donor_demographics'])
    {'age': {'max': 70.0, 'mean': 50.0, 'min': 30.0},
     'age_unit': ['years'],
     'age_value': [30.0, 50.0, 70.0]}

    Ages reported in months are normalized to years (divided by 12) so they are
    comparable across donors; the unit is relabeled accordingly:

    >>> doc = {
    ...     'entity_type': 'Dataset',
    ...     'donors': [
    ...         {'mapped_metadata': {'age_value': [30.0], 'age_unit': ['years']}},
    ...         {'mapped_metadata': {'age_value': [24.0], 'age_unit': ['months']}},
    ...     ],
    ... }
    >>> add_donor_demographics(doc)
    >>> pprint(doc['donor_demographics'])
    {'age': {'max': 30.0, 'mean': 16.0, 'min': 2.0},
     'age_unit': ['years'],
     'age_value': [2.0, 30.0]}

    A numeric field with no units is stored by translate under the bare "<field>"
    key; it is still normalized into a range-filterable array plus stats:

    >>> doc = {
    ...     'entity_type': 'Dataset',
    ...     'donors': [
    ...         {'mapped_metadata': {'age': [40.0]}},
    ...         {'mapped_metadata': {'age': [60.0]}},
    ...     ],
    ... }
    >>> add_donor_demographics(doc)
    >>> doc['donor_demographics']['age_value']
    [40.0, 60.0]
    >>> doc['donor_demographics']['age']
    {'min': 40.0, 'max': 60.0, 'mean': 50.0}

    A single-donor entity is simply a "donors" list of length one:

    >>> doc = {'entity_type': 'Dataset', 'donors': [{'mapped_metadata': {'sex': ['Male']}}]}
    >>> add_donor_demographics(doc)
    >>> doc['donor_demographics']
    {'sex': ['Male']}

    Entities of other types, or with no donor info, are left untouched:

    >>> doc = {'entity_type': 'Donor', 'mapped_metadata': {'sex': ['Male']}}
    >>> add_donor_demographics(doc)
    >>> 'donor_demographics' in doc
    False
    '''
    if doc.get('entity_type') not in DEMOGRAPHIC_ENTITY_TYPES:
        return
    donors = doc.get('donors', [])
    if not donors:
        return
    doc['donor_demographics'] = _aggregate_donor_demographics(donors)


def _aggregate_donor_demographics(donors):
    # Pool each mapped_metadata key's values across every donor.
    collected = defaultdict(list)
    for donor in donors:
        # Normalize ages to years before pooling, while each donor's parallel
        # age_value/age_unit lists are still aligned.
        mapped_metadata = _ages_in_years(donor.get('mapped_metadata') or {})
        for key, values in mapped_metadata.items():
            collected[key].extend(values if isinstance(values, list) else [values])

    # Sort the pooled keys into numeric values, units, and categorical sets. A field
    # may surface as "<field>_value"/"<field>_unit" (had units) or as a bare "<field>"
    # numeric list (no units), so both must funnel into the numeric bucket.
    numeric_values = defaultdict(list)
    units = defaultdict(set)
    categorical = {}
    for key, values in collected.items():
        if key.endswith('_unit'):
            units[key[:-len('_unit')]].update(values)
        elif key.endswith('_value'):
            numeric_values[key[:-len('_value')]].extend(_numbers(values))
        elif _all_numbers(values):
            numeric_values[key].extend(_numbers(values))
        else:
            categorical[key] = values

    demographics = {}
    for field, nums in numeric_values.items():
        if not nums:
            continue
        # Keep the array (range-filterable) and add display stats.
        demographics[f'{field}_value'] = sorted(set(nums))
        demographics[field] = {
            'min': min(nums),
            'max': max(nums),
            'mean': round(sum(nums) / len(nums), 2),
        }
        if units.get(field):
            demographics[f'{field}_unit'] = sorted(units[field])
    for field, values in categorical.items():
        demographics[field] = sorted(set(values))
    return demographics


MONTHS_PER_YEAR = 12


def _ages_in_years(mapped_metadata):
    '''Return mapped_metadata with the donor's age expressed in years.

    translate stores age as parallel "age_value"/"age_unit" lists; any value whose
    unit is "months" is divided by 12 and its unit relabeled "years" so ages are
    comparable across donors. A bare, unit-less "age" list can't be converted and is
    assumed to already be in years.
    '''
    values = mapped_metadata.get('age_value')
    units = mapped_metadata.get('age_unit')
    if not values or not units:
        return mapped_metadata

    converted_values = []
    converted_units = []
    for value, unit in zip(values, units):
        if _numbers([value]) and str(unit).lower() == 'months':
            converted_values.append(round(value / MONTHS_PER_YEAR, 2))
            converted_units.append('years')
        else:
            converted_values.append(value)
            converted_units.append(unit)
    return {**mapped_metadata, 'age_value': converted_values, 'age_unit': converted_units}


def _numbers(values):
    # bool is a subclass of int; exclude it so booleans aren't treated as numeric.
    return [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]


def _all_numbers(values):
    return bool(values) and len(_numbers(values)) == len(values)
