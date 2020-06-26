"""
Tests for the Schema class
"""

from dbt_schema_builder.relation import Relation
from dbt_schema_builder.schema import Schema


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

    assert schema.new_schema == expected_schema


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
                    {'name': 'THIS_TABLE', 'description': 'TODO: Replace me'},
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
                    {'name': 'THIS_TABLE', 'description': 'TODO: Replace me'},
                    {'name': 'THAT_TABLE', 'description': 'Expect this'}
                ],
            },
        ],
        "models": [],
    }

    assert schema.new_downstream_sources == expected_downstream_sources
