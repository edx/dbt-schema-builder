"""
Tests for the Relation class
"""

import pytest

from dbt_schema_builder.relation import Relation


def test_prep_meta_data():
    relation = Relation(
        'START',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        ['START', 'END'],
        [],
        [],
        []
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


def test_excluded_from_downstream_sources():
    relation = Relation(
        'NOT_THIS_TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'non/existent/path',
        ['START', 'END'],
        [],
        [],
        ['LMS.THIS_TABLE', 'LMS.THAT_TABLE'],
    )
    assert relation.excluded_from_downstream_sources


def test_manual_model_not_exist():
    relation = Relation(
        'TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'non/existent/path',
        ['START', 'END'],
        [],
        [],
        []
    )
    assert not relation.manual_safe_model_exists


def test_manual_model_not_flat(tmpdir):
    app_path_base = tmpdir.mkdir('test_app_path')
    db_path = app_path_base.mkdir('PROD')
    app_path = db_path.mkdir('LMS')
    manual_model_path = app_path.mkdir('LMS_MANUAL')
    manual_model_path.mkdir('subdirectory')

    relation = Relation(
        'TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        app_path,
        ['START', 'END'],
        [],
        [],
        []
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
        'TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        app_path,
        ['START', 'END'],
        [],
        [],
        []
    )
    assert relation.manual_safe_model_exists


def test_in_current_sources():

    relation = Relation(
        'THIS_TABLE',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'app_path',
        ['START', 'END'],
        [],
        [],
        []
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


def _get_fake_relation_dict():
    """
    Returns the bare usable relation dit we can use for testing SQL templates
    """
    return {
        "name": "RELATION_NAME",
        "alias": "RELATION_ALIAS",
        "description": "RELATION_DESCRIPTION",
        "columns": [
            {"name": "COLUMN_NAME"},
            {"name": "SOFT_DELETE_COLUMN"}
        ],
    }


def _get_fake_raw_schema(soft_delete_column_name=None, soft_delete_sql_clause=None):
    """
    Returns the bare usable object we can use to fake a Schema for testing SQL templates

    This only works because class and dict syntax is the same in Jinja.
    """
    return {
        "schema_name": "SCHEMA_NAME",
        "soft_delete_column_name": soft_delete_column_name,
        "soft_delete_sql_clause": lambda: "{} {}".format(soft_delete_column_name, soft_delete_sql_clause)
    }


def test_sql_no_soft_delete_no_pii_no_redactions():
    relation_dict = _get_fake_relation_dict()
    raw_schema = _get_fake_raw_schema()
    sql = Relation.render_sql(
        'APP_NAME',
        'SAFE',
        relation_dict,
        raw_schema,
        []
    )

    assert 'APP_NAME' in sql
    assert 'PII' not in sql
    assert 'SCHEMA_NAME' in sql
    assert 'WHERE' not in sql


def test_sql_soft_delete_no_pii_no_redactions():
    relation_dict = _get_fake_relation_dict()
    raw_schema = _get_fake_raw_schema(
        soft_delete_column_name='SOFT_DELETE_COLUMN',
        soft_delete_sql_clause='IS NULL'
    )
    sql = Relation.render_sql(
        'APP_NAME',
        'SAFE',
        relation_dict,
        raw_schema,
        []
    )

    assert 'APP_NAME' in sql
    assert 'PII' not in sql
    assert 'SCHEMA_NAME' in sql
    assert 'WHERE SOFT_DELETE_COLUMN IS NULL' in sql


def test_add_prefix_to_model_alias_with_snowflake_keyword_collision():
    relation = Relation(
        'START',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        ['START'],
        [],
        [],
        [],
        prefix="TESTPREFIX"
    )
    test_dict = relation.prep_meta_data()
    assert test_dict["alias"] == "TESTPREFIX_START"


def test_add_prefix_to_model_alias_with_no_snowflake_keyword_collision():
    relation = Relation(
        'START',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        [],
        [],
        [],
        [],
        prefix="TESTPREFIX"
    )
    test_dict = relation.prep_meta_data()
    assert test_dict["alias"] == "TESTPREFIX_START"


def test_snowflake_keyword_collision():
    relation = Relation(
        'START',
        ['COLUMN_1', 'COLUMN_2'],
        'LMS',
        'models/PROD/LMS',
        ['START'],
        [],
        [],
        [],
    )
    test_dict = relation.prep_meta_data()
    assert test_dict["alias"] == "_START"


def test_snowflake_keyword_quoting():
    relation = Relation(
        'START',
        ['TABLE', 'SCHEMA'],
        'LMS',
        'models/PROD/LMS',
        ['START', 'TABLE', 'SCHEMA'],
        [],
        [],
        [],
    )
    test_dict = relation.prep_meta_data()
    assert test_dict['columns'][0]["name"].startswith('"')
    assert test_dict['columns'][1]["name"].startswith('"')
