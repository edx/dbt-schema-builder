"""
Tests for the App class
"""

from dbt_schema_builder.app import App
from dbt_schema_builder.relation import Relation
from dbt_schema_builder.schema import Schema


def test_add_source_to_new_schema():
    schema_1 = Schema('LMS_TEST_RAW', [], [], None, None)
    schema_2 = Schema('LMS_RAW', [], [], None, None)
    schema_3 = Schema('LMS_STITCH_RAW', [], [], None, None)
    raw_schemas = [schema_1, schema_2, schema_3]
    app = App(
        raw_schemas,
        'LMS',
        'models/PROD/LMS',
        'models/PROD/LMS/LMS.yml',
        {},
        {},
        'PROD'
    )

    current_raw_source = None
    relation = Relation(
        'THIS_TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        ['START', 'END'],
        [],
        [],
        []
    )
    app.add_source_to_new_schema(current_raw_source, relation, schema_2)

    current_raw_source = {"name": "THAT_TABLE", "description": "some special description"}
    relation = Relation(
        'THAT_TABLE',
        ['COLUMN_3', 'COLUMN_4'],
        'LMS',
        'models/PROD/LMS',
        ['START', 'END'],
        [],
        [],
        []
    )
    app.add_source_to_new_schema(current_raw_source, relation, schema_2)

    expected_schema = {
        "version": 2,
        "sources": [
            {
                "name": 'LMS_TEST_RAW', "tables": []
            },
            {
                "name": 'LMS_RAW', "tables": [
                    {"name": 'THIS_TABLE'},
                    {"name": 'THAT_TABLE', "description": "some special description"},
                ]
            },
            {
                "name": 'LMS_STITCH_RAW', "tables": []
            },
        ],
        "models": [],
    }

    assert app.new_schema == expected_schema


def test_update_trifecta_models():
    raw_schemas = [
        Schema('LMS_RAW', [], [], None, None)
    ]
    app = App(
        raw_schemas,
        'LMS',
        'models/PROD/LMS',
        'models/PROD/LMS/LMS.yml',
        {},
        {},
        'PROD'
    )

    relation = Relation(
        'THIS_TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        ['START', 'END'],
        [],
        [],
        []
    )

    app.update_trifecta_models(relation)
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

    assert app.new_schema == expected_schema


def test_add_table_to_downstream_sources(tmpdir):
    app_path_base = tmpdir.mkdir('models')
    db_path = app_path_base.mkdir('PROD')
    app_path = db_path.mkdir('LMS')
    manual_model_path = app_path.mkdir('LMS_MANUAL')
    manual_model_file = manual_model_path.join("LMS_TABLE.sql")
    manual_model_file.write('data')

    raw_schemas = [
        Schema('LMS_RAW', [], [], None, None)
    ]
    app = App(
        raw_schemas,
        'LMS',
        'models/PROD/LMS',
        'models/PROD/LMS/LMS.yml',
        {},
        {},
        'PROD'
    )

    relation = Relation(
        'THIS_TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        ['START', 'END'],
        [],
        [],
        []
    )

    app.add_table_to_downstream_sources(relation, None, None)

    relation = Relation(
        'THIS_TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        ['START', 'END'],
        [],
        [],
        []
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

    app.add_table_to_downstream_sources(
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

    assert app.new_downstream_sources == expected_downstream_sources
