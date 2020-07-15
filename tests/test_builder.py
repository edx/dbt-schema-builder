"""
Tests for various things in builder.py
"""

import pytest

from dbt_schema_builder.builder import validate_schema_config
from dbt_schema_builder.schema import InvalidConfigurationException


def test_valid_config():
    config = {
        'APP_1': {
            'RAW_SCHEMA_1': {
                'INCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ]
            },
            'RAW_SCHEMA_2': {
                'INCLUDE': [
                    'TABLE_1',
                    'TABLE_2',
                ]
            }
        },
        'APP_2': {
            'RAW_SCHEMA_1': {},
        },
    }
    assert validate_schema_config(config)


def test_invalid_config_keys():
    config = {
        'APP_1': {
            'RAW_SCHEMA_1': {
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
        validate_schema_config(config)
    assert "has both an EXCLUDE and INCUDE section" in str(excinfo.value)
