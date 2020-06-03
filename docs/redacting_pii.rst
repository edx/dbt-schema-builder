.. _redacting_pii:

Redacting PII
=============

Schemas managed by the schema builder are designed to allow specific columns to
be redacted by adding them to a
``redactions.yml`` file at the top of the dbt project. Currently this file is
hand-managed. An example file::

    # Redactions are identified by SCHEMA.TABLE and the literal value of the key value pair will be used in the SQL to
    # generate the SAFE schema SQL models. Strings currently must be quoted with single quotes inside of double quotes.
    #
    # Example:
    #
    # SCHEMA.USER:                              # LMS schema, D_USER table
    #   USER_YEAR_OF_BIRTH: 1900                 # Replace the USER_YEAR_OF_BIRTH column with the number 1900
    #   USER_EMAIL: "'redacted@edx.invalid'"     # Replace the USER_EMAIL column with the string 'redacted@edx.invalid'
    #   USER_USERNAME': "'<redacted>'"
    #
    # Generates a USER.sql in <SCHEMA> similar to:
    # SELECT id, foo, bar, 1900 as USER_YEAR_OF_BIRTH, 'redacted@edx.invalid' as USER_EMAIL, '<redacted>' as USER_USERNAME
    # FROM SCHEMA.USER
    #
    # Also a USER.sql in <SCHEMA_PII> similar to:
    # SELECT id, foo, bar, USER_YEAR_OF_BIRTH, USER_EMAIL, USER_USERNAME
    # FROM SCHEMA.USER


Unmanaged tables
----------------

If there are tables which need custom handling for whatever reason, they can
avoid having models generated for them by the schema builder script by adding
the model names to the `unmanaged_tables.yml` file at the top of the dbt
project. The most frequent reason so far for manual management is when a table
has a column that is a Snowflake reserved keyword. The table's view SQL is
manually managed in this case to rename the column for successful creation in
Snowflake. However in some cases we've also wanted to filter out certain
sensitive rows from being included in downstream queries.

These tables will not have any views built for them. If you wish to have them
present in the <SCHEMA_PII> or <SCHEMA> schemas you will need to manually add
models for them to the <SCHEMA>_MANUAL directory by hand.

This YML file is managed by hand. An example file::

    # This file contains list tables that should have their models managed by
    # the schema builder tool in that form of SCHEMA.TABLE.
    #
    # Example:
    # - LMS.COURSE_STRUCTURE
    # - LMS.AUTH_USER

    - FOO_SCHEMA.BAR_TABLE
    - BAZ_SCHAME.BING_TABLE

