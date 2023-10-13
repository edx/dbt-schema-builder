"""
Tests for the Schema class
"""

import pytest

from dbt_schema_builder.relation import Relation
from dbt_schema_builder.schema import InvalidConfigurationException, Schema


def test_raw_schema_filter_with_exclusion_list():
    relations = [
        Relation(
            'THIS_TABLE',
            ['COLUMN_1', 'COLUMN_2'],
            'LMS',
            'models/PROD/LMS',
            ['START', 'END'],
            [],
            [],
            []
        ),
        Relation(
            'NOT_THIS_TABLE',
            ['COLUMN_1', 'COLUMN_2'],
            'LMS',
            'models/PROD/LMS',
            ['START', 'END'],
            [],
            [],
            []
        ),
        Relation(
            'THIS_TABLE_ALSO',
            ['COLUMN_1', 'COLUMN_2'],
            'LMS',
            'models/PROD/LMS',
            ['START', 'END'],
            [],
            [],
            []
        ),
    ]

    exclusion_list = ['NOT_THIS_TABLE']

    raw_schema = Schema(
        'PROD',
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
            'NOT_THIS_TABLE',
            ['COLUMN_1', 'COLUMN_2'],
            'LMS',
            'models/PROD/LMS',
            ['START', 'END'],
            [],
            [],
            []
        ),
        Relation(
            'ONLY_THIS_TABLE',
            ['COLUMN_1', 'COLUMN_2'],
            'LMS',
            'models/PROD/LMS',
            ['START', 'END'],
            [],
            [],
            []
        ),
        Relation(
            'NOT_THIS_TABLE_EITHER',
            ['COLUMN_1', 'COLUMN_2'],
            'LMS',
            'models/PROD/LMS',
            ['START', 'END'],
            [],
            [],
            []
        ),
    ]
    inclusion_list = ['ONLY_THIS_TABLE']
    raw_schema = Schema(
        'PROD',
        'LMS_RAW',
        [],
        inclusion_list,
        None,
        None,
        relations=relations
    )
    filtered_relations = raw_schema.filter_relations()

    assert len(filtered_relations) == 1


def test_raw_schema_sql_clause_success():
    raw_schema = Schema(
        'PROD',
        'LMS_RAW',
        [],
        [],
        'SOFT_DELETE_COLUMN',
        'IS NOT NULL',
        relations=[]
    )

    assert raw_schema.soft_delete_sql_clause() == "SOFT_DELETE_COLUMN IS NOT NULL"


def test_raw_schema_sql_predicate_null():
    with pytest.raises(InvalidConfigurationException) as excinfo:
        Schema(
            'PROD',
            'LMS_RAW',
            [],
            [],
            'SOFT_DELETE_COLUMN',
            None,
            relations=[]
        )

    assert "has an invalid SOFT_DELETE configuration" in str(excinfo.value)


def test_raw_schema_sql_predicate_empty():
    with pytest.raises(InvalidConfigurationException) as excinfo:
        Schema(
            'PROD',
            'LMS_RAW',
            [],
            [],
            'SOFT_DELETE_COLUMN',
            '',
            relations=[]
        )

    assert "has an invalid SOFT_DELETE configuration" in str(excinfo.value)


def test_raw_schema_sql_clause_non_string():
    with pytest.raises(InvalidConfigurationException) as excinfo:
        Schema(
            'PROD',
            'LMS_RAW',
            [],
            [],
            'SOFT_DELETE_COLUMN',
            False,
            relations=[]
        )

    assert "has an invalid SOFT_DELETE configuration" in str(excinfo.value)
