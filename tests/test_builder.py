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

def test_empty_redactions_file():
    redactions = {}
    assert SchemaBuilder.validate_redactions(redactions)

def test_valid_redactions_file():
    redactions = {
        'SCHEMA1.TABLE1': {
            'COLUMN1': '''redacted''',
            'COLUMN2': '''redacted@email.com''',
            'COLUMN3': {
                'hashed': True,
                'input': 'COLUMN3',
            },
            'COLUMN4': {
                'hashed': True,
                'input': 'LOWER(COLUMN4)',
            },
        },
    }
    assert SchemaBuilder.validate_redactions(redactions)

def test_invalid_redactions_file_missing_keys():
    invalid_redactions = {
        'SCHEMA1.TABLE1': {
            'COLUMN1': '''redacted''',
            'COLUMN2': '''redacted@email.com''',
            'COLUMN3': {
                'hashed': True,
            },
            'COLUMN4': {
                'hashed': True,
                'input': 'LOWER(COLUMN4)',
            },
        },
    }
    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_redactions(invalid_redactions)

    err_msg = (
        'missing key'
    )
    assert err_msg in str(excinfo.value)

def test_invalid_redactions_file_invalid_sql():
    invalid_redactions = {
        'SCHEMA1.TABLE1': {
            'COLUMN1': '''redacted''',
            'COLUMN2': '''redacted@email.com''',
            'COLUMN3': {
                'hashed': True,
                'input': 'COLUMN3',
            },
            'COLUMN4': {
                'hashed': True,
                'input': 'LOWER(COLUMN4',
            },
        },
    }
    with pytest.raises(InvalidConfigurationException) as excinfo:
        SchemaBuilder.validate_redactions(invalid_redactions)

    err_msg = (
        'invalid sql'
    )
    assert err_msg in str(excinfo.value)
