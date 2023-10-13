"""
Tests for various things in builder.py
"""

import os
from tempfile import mkdtemp
from unittest.mock import MagicMock, patch

import pytest
import yaml

from dbt_schema_builder.builder import GetCatalogTask, SchemaBuilder
from dbt_schema_builder.schema import InvalidConfigurationException


def get_valid_test_config():
    return {
        'DB_1.APP_1': {
            'DB_1.RAW_SCHEMA_1': {
                'INCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ]
            },
            'DB_1.RAW_SCHEMA_2': {
                'EXCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ],
                'SOFT_DELETE': {
                    'DELETED_AT': 'IS NOT NULL'
                }
            }
        },
        'DB_1.APP_2': {
            'DB_1.RAW_SCHEMA_1': {},
        },
    }


def test_valid_config():
    config = get_valid_test_config()
    assert SchemaBuilder.validate_schema_config(config)


def test_invalid_config_keys():
    config = {
        'DB_1.APP_1': {
            'DB_1.RAW_SCHEMA_1': {
                'INCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ],
                'EXCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ]
            },
        },
    }
    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_schema_config(config)
    assert "has both an EXCLUDE and INCUDE section" in str(excinfo.value)


def test_invalid_soft_delete_keys():
    config = {
        'DB_1.APP_1': {
            'DB_1.RAW_SCHEMA_1': {
                'INCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ],
                'SOFT_DELETE': [
                    'SOFT_DELETE_COLUMN'
                ]
            }
        }
    }
    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_schema_config(config)

    assert "The SOFT_DELETE key in DB_1.RAW_SCHEMA_1 must map" in str(excinfo.value)

    config = {
        'DB_1.APP_1': {
            'DB_1.RAW_SCHEMA_2': {
                'EXCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ],
                'SOFT_DELETE': {
                    'SOFT_DELETE_COLUMN_NAME_1': 'SOFT_DELETE_VALUE',
                    'SOFT_DELETE_COLUMN_NAME_2': 'SOFT_DELETE_VALUE'
                }
            }
        }
    }

    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_schema_config(config)

    err_msg = "SOFT_DELETE key in DB_1.RAW_SCHEMA_2 must only have one key/value"

    assert err_msg in str(excinfo.value)


def test_bad_destination_config_format():
    config = {
        'DB_1': {
            'DB_1.RAW_SCHEMA_1': {},
        }
    }
    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_schema_config(config)

    err_msg = "Invalid destination schema path in schema_config.yml"
    assert err_msg in str(excinfo.value)


def test_bad_source_config_format():
    config = {
        'DB_1.APP_1': {
            'RAW_SCHEMA_1': {},
        }
    }

    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_schema_config(config)

    err_msg = "Invalid source schema path in schema_config.yml"
    assert err_msg in str(excinfo.value)


def test_valid_unmanaged_tables_file():
    unmanaged_tables = []
    assert SchemaBuilder.validate_unmanaged_tables(unmanaged_tables)
    unmanaged_tables = [
        'SCHEMA_1.TABLE_1',
        'SCHEMA_1.TABLE_.*',
        'SCHEMA_2.TABLE_[0-9]',
    ]
    assert SchemaBuilder.validate_unmanaged_tables(unmanaged_tables)


def test_invalid_unmanaged_tables_file():
    unmanaged_tables = [
        'SCHEMA_1.TABLE_1',
        'BAD_SCHEMA',
        'SCHEMA_2.TABLE_1',
    ]
    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_unmanaged_tables(unmanaged_tables)

    err_msg = (
        'Entry "BAD_SCHEMA" in unmanaged_files.yml is not formatted '
        'correctly.'
    )
    assert err_msg in str(excinfo.value)


def test_bad_regex_unmanaged_tables_file():
    unmanaged_tables = [
        'SCHEMA_1.TABLE_1',
        'SCHEMA_1.BAD_REGEX[',
        'SCHEMA_2.TABLE_1',
    ]
    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_unmanaged_tables(unmanaged_tables)

    err_msg = (
        'Entry "SCHEMA_1.BAD_REGEX[" in unmanaged_files.yml contains '
        'an invalid regular expression'
    )
    assert err_msg in str(excinfo.value)


@patch.object(SchemaBuilder, 'get_redactions', lambda x: {})
@patch.object(SchemaBuilder, 'get_snowflake_keywords', lambda x: {})
@patch.object(SchemaBuilder, 'get_banned_columns', lambda x: {})
@patch.object(SchemaBuilder, 'get_unmanaged_tables', lambda x: {})
@patch.object(SchemaBuilder, 'get_downstream_sources_allow_list', lambda x: {})
def test_build_app():
    app_name = 'DB_1.APP'
    app_config = {
        app_name: {
            'DB_2.RAW_SCHEMA_1': {},
            'DB_3.RAW_SCHEMA_2': {},
        }
    }

    temp_dir = mkdtemp()
    mock_get_catalog_task = MagicMock(GetCatalogTask)
    mock_get_catalog_task.run.return_value = [
        {"TABLE_NAME": "TABLE_A", "COLUMN_NAME": "COLUMN_A"},
        {"TABLE_NAME": "TABLE_B", "COLUMN_NAME": "COLUMN_D"},
    ]
    with patch.object(SchemaBuilder, 'build_app_path', lambda x, y, z: temp_dir):
        with patch.object(SchemaBuilder, 'get_app_schema_configs', lambda x: app_config):
            builder = SchemaBuilder(temp_dir, temp_dir, temp_dir, mock_get_catalog_task)
            builder.build_app(app_name, app_config[app_name])
    with open(os.path.join(temp_dir, 'models/automatically_generated_sources/APP.yml')) as fp:
        downstream_sources = yaml.safe_load(fp)
    assert downstream_sources['sources'][0]['name'] == 'APP'
    assert downstream_sources['sources'][1]['name'] == 'APP_PII'
    with open(os.path.join(temp_dir, 'APP.yml')) as fp:
        raw_source = yaml.safe_load(fp)
    assert raw_source['sources'][0]['database'] == 'DB_2'
    assert raw_source['sources'][0]['name'] == 'RAW_SCHEMA_1'
    assert raw_source['sources'][1]['database'] == 'DB_3'
    assert raw_source['sources'][1]['name'] == 'RAW_SCHEMA_2'
