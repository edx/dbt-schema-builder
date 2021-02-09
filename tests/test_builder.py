"""
Tests for various things in builder.py
"""

import pytest

from dbt_schema_builder.builder import SchemaBuilder
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
