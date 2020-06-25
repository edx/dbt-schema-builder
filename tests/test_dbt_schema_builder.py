"""
Tests for dbt_schema_builder.py.
"""
import logging

import pytest
from dbt_schema_builder.schema import Relation, Schema


def test_prep_meta_data():
    relation = Relation(
        'START', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS', ['START', 'END'], [], []
    )
    model = relation.prep_meta_data()

    expected_model = {
        "name": "START",
        "alias": "_START",
        "description": "TODO: Replace me",
        "columns": [
            {"name": "COLUMN_1"},
            {"name": "COLUMN_2"}
        ],
    }

    assert model == expected_model

def test_manual_model_not_exist():
    relation = Relation(
        'TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'non/existent/path', ['START', 'END'], [], []
    )
    assert not relation.manual_safe_model_exists

def test_manual_model_not_flat(tmpdir):
    app_path_base = tmpdir.mkdir('test_app_path')
    db_path = app_path_base.mkdir('PROD')
    app_path = db_path.mkdir('LMS')
    manual_model_path = app_path.mkdir('LMS_MANUAL')
    manual_model_path.mkdir('subdirectory')

    relation = Relation(
        'TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', app_path, ['START', 'END'], [], []
    )
    with pytest.raises(RuntimeError) as excinfo:
        _ = relation.manual_safe_model_exists
    assert 'MANUAL directory is not "flat"' in str(excinfo.value)

def test_manual_model_exists(tmpdir):
    app_path_base = tmpdir.mkdir('test_app_path')
    db_path = app_path_base.mkdir('PROD')
    app_path = db_path.mkdir('LMS')
    manual_model_path = app_path.mkdir('LMS_MANUAL')
    manual_model_file = manual_model_path.join("LMS_TABLE.sql")
    manual_model_file.write('data')

    relation = Relation(
        'TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', app_path, ['START', 'END'], [], []
    )
    assert relation.manual_safe_model_exists

def test_in_current_sources():

    relation = Relation(
        'THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'app_path', ['START', 'END'], [], []
    )

    current_raw_sources = {
        "version": "2",
        "sources": [
            {
                "name": "LMS_RAW",
                "tables": [
                    {
                      "name": "NOT_THIS_TABLE"
                    },
                    {
                      "name": "THIS_TABLE"
                    },
                    {
                      "name": "NOT_THIS_TABLE_EITHER"
                    },
                ],
            }
        ],
        "models": []
    }

    current_downstream_sources = {
        "version": "2",
        "sources": [
            {
                "name": "LMS",
                "database": "PROD",
                "tables": [
                    {
                        "name": "NOT_THIS_TABLE",
                        "description": "Do not expect this in the assertion",
                    },
                    {
                        "name": "THIS_TABLE",
                        "description": "Make sure all of the aspects of this are preserved",
                        "freshness": {
                            "warn_after": {
                                "count": "24",
                                "period": "hour"
                            },
                            "error_after": {
                                "count": "36",
                                "period": "hour"
                            }
                        },
                        "loaded_at_field": "last_login"
                    },
                    {
                        "name": "NOT_THIS_TABLE_EITHER",
                        "description": "Do not expect this in the assertion",
                    },
                ]
            },
            {
                "name": "LMS_PII",
                "database": "PROD",
                "tables": [
                    {
                        "name": "THIS_TABLE",
                        "description": "Expect this",
                    },
                    {
                        "name": "NOT_THIS_TABLE",
                        "description": "Do not expect this in the assertion",
                    },
                    {
                        "name": "NOT_THIS_TABLE_EITHER:",
                        "description": "Do not expect this in the assertion",
                    },
                ]
            }
        ],
        "models": []
    }

    (
        current_raw_source,
        current_safe_downstream_source,
        current_pii_downstream_source
    ) = relation.find_in_current_sources(
        current_raw_sources,
        current_downstream_sources
    )

    expected_current_raw_source = {'name': 'THIS_TABLE'}
    expected_current_safe_downstream_source = {
        'name': 'THIS_TABLE',
        'description': 'Make sure all of the aspects of this are preserved',
        "freshness": {
            "warn_after": {
                "count": "24",
                "period": "hour"
            },
            "error_after": {
                "count": "36",
                "period": "hour"
            }
        },
        "loaded_at_field": "last_login"
    }
    expected_current_pii_downstream_source = {'name': 'THIS_TABLE', 'description': 'Expect this'}

    assert current_raw_source == expected_current_raw_source
    assert current_safe_downstream_source == expected_current_safe_downstream_source
    assert current_pii_downstream_source == expected_current_pii_downstream_source


# schema tests:

def test_add_table_to_new_schema():
    schema = Schema(
        'LMS_RAW', 'LMS', 'models/PROD/LMS', 'models/PROD/LMS/LMS.yml', {}, {}, 'PROD'
    )

    current_raw_source = None
    relation = Relation(
        'THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
        ['START', 'END'], [], []
    )
    schema.add_table_to_new_schema(current_raw_source, relation)

    current_raw_source = {"name": "THAT_TABLE", "description": "some special description"}
    relation = Relation(
        'THAT_TABLE', ['COLUMN_3', 'COLUMN_4'], 'LMS', 'models/PROD/LMS',
        ['START', 'END'], [], []
    )
    schema.add_table_to_new_schema(current_raw_source, relation)

    expected_schema = {
        "version": 2,
        "sources": [
            {
                "name": 'LMS_RAW', "tables": [
                    {"name": 'THIS_TABLE'},
                    {"name": 'THAT_TABLE', "description": "some special description"},
                ]
            }
        ],
        "models": [],
    }

    assert schema.new_schema == expected_schema


def test_update_trifecta_models():
    schema = Schema(
        'LMS_RAW', 'LMS', 'models/PROD/LMS', 'models/PROD/LMS/LMS.yml', {}, {}, 'PROD'
    )

    relation = Relation(
        'THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
        ['START', 'END'], [], []
    )

    schema.update_trifecta_models(relation)
    expected_schema = {
        "version": 2,
        "sources": [
            {
                "name": "LMS_RAW",
                "tables": []
            }
        ],
        "models": [
            {
                "name": "LMS_PII_THIS_TABLE",
                "columns": [
                    {"name": "COLUMN_1"},
                    {"name": "COLUMN_2"},
                ]
            },
            {
                "name": "LMS_THIS_TABLE",
                "columns": [
                    {"name": "COLUMN_1"},
                    {"name": "COLUMN_2"},
                ]
            }
        ],
    }


def test_add_table_to_downstream_sources(tmpdir):
    app_path_base = tmpdir.mkdir('models')
    db_path = app_path_base.mkdir('PROD')
    app_path = db_path.mkdir('LMS')
    manual_model_path = app_path.mkdir('LMS_MANUAL')
    manual_model_file = manual_model_path.join("LMS_TABLE.sql")
    manual_model_file.write('data')

    schema = Schema(
        'LMS_RAW', 'LMS', 'models/PROD/LMS', 'models/PROD/LMS/LMS.yml', {}, {}, 'PROD'
    )

    relation = Relation(
        'THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
        ['START', 'END'], [], []
    )

    schema.add_table_to_downstream_sources(relation, None, None)

    relation = Relation(
        'THIS_TABLE', ['COLUMN_1', 'COLUMN_2'], 'LMS', 'models/PROD/LMS',
        ['START', 'END'], [], []
    )
    current_safe_downstream_source = {
        'name': 'THAT_TABLE',
        'description': 'Make sure all of the aspects of this are preserved',
        "freshness": {
            "warn_after": {
                "count": "24",
                "period": "hour"
            },
            "error_after": {
                "count": "36",
                "period": "hour"
            }
        },
        "loaded_at_field": "last_login"
    }
    current_pii_downstream_source = {'name': 'THAT_TABLE', 'description': 'Expect this'}

    schema.add_table_to_downstream_sources(
        relation, current_safe_downstream_source, current_pii_downstream_source
    )

    expected_downstream_sources = {
        "version": 2,
        "sources": [
            {
                "name": 'LMS',
                "database": 'PROD',
                "tables": [
                    {'name': 'THIS_TABLE', 'description': 'Expect this'},
                    {
                        'name': 'THAT_TABLE',
                        'description': 'Make sure all of the aspects of this are preserved',
                        "freshness": {
                            "warn_after": {
                                "count": "24",
                                "period": "hour"
                            },
                            "error_after": {
                                "count": "36",
                                "period": "hour"
                            }
                        },
                        "loaded_at_field": "last_login"
                    }
                ]
            },
            {
                "name": 'LMS_PII',
                "database": 'PROD',
                "tables": [
                    {'name': 'THIS_TABLE', 'description': 'Expect this'},
                    {'name': 'THAT_TABLE', 'description': 'Expect this'}
                ],
            },
        ],
        "models": [],
    }
