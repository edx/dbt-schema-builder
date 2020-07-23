"""
Tests for the Schema class
"""

from dbt_schema_builder.relation import Relation
from dbt_schema_builder.schema import Schema


def test_raw_schema_invalid_key():
    raw_schema = Schema(
        'LMS_RAW',
        []
        [],
        None,
        None,
        relations=[]
    )


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
    raw_schema = Schema(
        'LMS_RAW',
        exclusion_list,
        [],
        None,
        None,
        relations=relations
    )
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
    raw_schema = Schema(
        'LMS_RAW',
        [],
        inclusion_list,
        None,
        None,
        relations=relations
    )
    filtered_relations = raw_schema.filter_relations()

    assert len(filtered_relations) == 1


def test_raw_schema_sql_clause_null():
    raw_schema = Schema(
        'LMS_RAW',
        [],
        [],
        'SOFT_DELETE',
        None,
        relations=[]
    )
    raw_schema.soft_delete_sql_clause() == "SOFT_DELETE IS NULL"


def test_raw_schema_sql_clause_boolean():
    raw_schema = Schema(
        'LMS_RAW',
        [],
        [],
        'SOFT_DELETE',
        False,
        relations=[]
    )
    raw_schema.soft_delete_sql_clause() == "NOT SOFT_DELETE"


def test_raw_schema_sql_clause_string():
    raw_schema = Schema(
        'LMS_RAW',
        [],
        [],
        'SOFT_DELETE',
        'SOMETHING',
        relations=[]
    )
    raw_schema.soft_delete_sql_clause() == "SOFT_DELETE == 'SOMETHING'"
