"""
Tests for the Schema class
"""

from dbt_schema_builder.relation import Relation
from dbt_schema_builder.schema import Schema


def test_raw_schema_filter_with_exclusion_list():
    relations = [
        Relation(
            'THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
            ['START', 'END'], [], []
        ),
        Relation(
            'NOT_THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
            ['START', 'END'], [], []
        ),
        Relation(
            'THIS_TABLE_ALSO', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
            ['START', 'END'], [], []
        ),
    ]
    exclusion_list = ['NOT_THIS_TABLE']
    raw_schema = Schema('LMS_RAW', exclusion_list, [], relations=relations)
    filtered_relations = raw_schema.filter_relations()

    assert len(filtered_relations) == 2


def test_raw_schema_filter_with_inclusion_list():
    relations = [
        Relation(
            'NOT_THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
            ['START', 'END'], [], []
        ),
        Relation(
            'ONLY_THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
            ['START', 'END'], [], []
        ),
        Relation(
            'NOT_THIS_TABLE_EITHER', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
            ['START', 'END'], [], []
        ),
    ]
    inclusion_list = ['ONLY_THIS_TABLE']
    raw_schema = Schema('LMS_RAW', [], inclusion_list, relations=relations)
    filtered_relations = raw_schema.filter_relations()

    assert len(filtered_relations) == 1
