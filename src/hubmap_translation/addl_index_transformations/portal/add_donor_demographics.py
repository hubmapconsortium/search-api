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

    Each donor already has a "mapped_metadata" dict (produced by translate.py) that
    follows the convention: numeric fields are "<field>_value"/"<field>_unit" lists,
    and everything else is a list of categorical values.

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

    When no "donors" list is present, it falls back to the single "donor":

    >>> doc = {'entity_type': 'Dataset', 'donor': {'mapped_metadata': {'sex': ['Male']}}}
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
    donors = doc.get('donors') or ([doc['donor']] if 'donor' in doc else [])
    if not donors:
        return
    doc['donor_demographics'] = _aggregate_donor_demographics(donors)


def _aggregate_donor_demographics(donors):
    # Pool each mapped_metadata key's values across every donor.
    collected = defaultdict(list)
    for donor in donors:
        mapped_metadata = donor.get('mapped_metadata') or {}
        for key, values in mapped_metadata.items():
            collected[key].extend(values if isinstance(values, list) else [values])

    demographics = {}
    for key, values in collected.items():
        if key.endswith('_value'):
            numeric = [v for v in values if isinstance(v, (int, float))]
            if not numeric:
                continue
            # Keep the array (range-filterable) and add display stats.
            demographics[key] = sorted(set(numeric))
            demographics[key[:-len('_value')]] = {
                'min': min(numeric),
                'max': max(numeric),
                'mean': round(sum(numeric) / len(numeric), 2),
            }
        else:
            # Units and categorical fields: deduplicated, sorted set.
            demographics[key] = sorted(set(values))
    return demographics
