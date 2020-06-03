dbt-schema-builder
=============================

|pypi-badge| |travis-badge| |codecov-badge| |doc-badge| |pyversions-badge|
|license-badge|

The Schema Builder tool is used to create dbt schema files, sql models, and
default PII / non-PII views for tables in the given Snowflake schemas. The
script will generate models for new `<SCHEMA>` and `<SCHEMA>_PII` schemas.
These will be created when you run dbt if they don't exist, presuming you have
permissions to do so.

Once the script is successfully run, you can execute a `dbt run` to create the
actual new schemas and views.

Setup
-----

In order to use this script you will need a dbt profile pointing to your
personal Snowflake database:
https://docs.getdbt.com/docs/configure-your-profile

To run the script that generates the sources yaml file and the model sql files
you should create a Python 3.5+ virtualenv and
``pip install -r requirements.txt``.

In order for this tool to work you will need the following Snowflake
permissions on the role you use in profiles.yml:

- USE and SELECT on the source schema (ex. LMS)
- CREATE VIEW on the target schemas (ex. LMS_SAFE, LMS_PII)
- If the target schemas don't exist, CREATE SCHEMA them as well

Here is an example working profiles.yml::

    default:
       target: dev
       outputs:
         dev:
           type: snowflake
           account: edx.us-east-1
           user: bmesick@edx.org
           password: <password>
           role: BMESICK_EVAL_ROLE
           database: BMESICK_TESTDB
           warehouse: DEMO_WH
           schema: LMS
           threads: 2
           client_session_keep_alive: False


Running
-------

Since it piggy-backs on dbt's configuration, Schema Builder must be run from
inside an existing dbt project.

``$ python ../../tools/dbt_schema_builder/schema_builder.py build --raw-schemas <source schemas> [--profiles-dir <path to your dbt profiles.yml>] [--profile
<profile name>] [--target <a target from profiles.yml>]``

Required Parameters

``--raw-schemas`` - a space separated list of source schemas to work on. For
every table in this schema two views will be created in the associated schemas.

``--destination-project`` - the dbt project that will use the generated
sources. Schema Builder will create or overwrite the source file(s) associated
with the schemas passed in to ``--raw-schemas`` for this project.

Options

``--profiles-dir`` - the path to your dbt profiles.yml, defaults to
~/.dbt/profiles.yml

``--profile`` -  the profile name to use from your profiles.yml, defaults to
the "profile" value in ``dbt_project.yml``

``--target`` -  a valid target from your profiles.yml, defaults to the default
target in your chosen profile


Redacting PII
-------------

Schemas managed by the schema builder are designed to allow specific columns to
be redacted by adding them to a
``redactions.yml`` file at the top of the dbt project. Currently this file is
hand-managed. Directions are included in the top of the file::

    # Redactions are identified by SCHEMA.TABLE and the literal value of the key value pair will be used in the SQL to
    # generate the SAFE schema SQL models. Strings currently must be quoted with single quotes inside of double quotes.
    #
    # Example:
    #
    # 'LMS.D_USER':                              # LMS schema, D_USER table
    # - USER_YEAR_OF_BIRTH: 1900                 # Replace the USER_YEAR_OF_BIRTH column with the number 1900
    #   USER_EMAIL: "'redacted@edx.invalid'"     # Replace the USER_EMAIL column with the string 'redacted@edx.invalid'
    #   USER_USERNAME': "'<redacted>'"
    #
    # Generates a D_USER_SAFE.sql similar to:
    # SELECT id, foo, bar, 1900 as USER_YEAR_OF_BIRTH, 'redacted@edx.invalid' as USER_EMAIL, '<redacted>' as USER_USERNAME
    # FROM LMS.D_USER

Unmanaged tables
----------------

If there are tables which need custom handling for whatever reason, they can
avoid having models generated for them by the schema builder script by adding
the model names to the `unmanaged_tables.yml` file at the top of the dbt
project. (The most frequent reason so far for manual management is when a table
has a column that is a Snowflake reserved keyword. The table's view SQL is
manually managed in this case to rename the column for successful creation in
Snowflake.)

This YML file is managed by hand. Directions are included at the top of the file::

    # This file contains list tables that should have their models managed by the schema builder tool in that form of
    # SCHEMA.TABLE. These tables will not have any _SAFE or _PII views build for them, but they *will* show up in the
    # schema.yml file. You will need to add models for them to the _MANUAL directory by hand.
    #
    # Example:
    # - LMS.COURSE_STRUCTURE
    # - LMS.AUTH_USER

Adding new schemas
------------------

dbt will not find any schema that it does not already know about via a .yml
file for performance reasons. So to add a new schema to be used in this tool
you will first need to create a stub .yml file for it. Luckily it's very easy!

If the new source schema that you are adding is named "LMS", create a folder
named "LMS" in the top level "models" directory. Inside this folder create
"LMS.yml" with the following contents::

    version: 2

    sources:
    - name: LMS

    tables:
      - name: A_REAL_TABLE_NAME_IN_THIS_SCHEMA

You will also need a stub model file in, or under, the same directory.


In a file named after a real table in the source schema (ex.
``auth_group.sql``), create a bogus model that uses the real database name,
schema name, and table name in this format::

    select * from DATABASE_NAME.SCHEMA_NAME.TABLE_NAME;

These files will be overwritten on the first run with the real version of
schema, which will include all of the real tables and sources. This should be
enough to let dbt do what we need it to. We're hoping to work around this in
the near future.

License
-------

The code in this repository is licensed under the AGPL 3.0 unless
otherwise noted.

Please see `LICENSE.txt <LICENSE.txt>`_ for details.

How To Contribute
-----------------

Contributions are very welcome.
Please read `How To Contribute <https://github.com/edx/edx-platform/blob/master/CONTRIBUTING.rst>`_ for details.
Even though they were written with ``edx-platform`` in mind, the guidelines
should be followed for all Open edX projects.

The pull request description template should be automatically applied if you are creating a pull request from GitHub. Otherwise you
can find it at `PULL_REQUEST_TEMPLATE.md <.github/PULL_REQUEST_TEMPLATE.md>`_.

The issue report template should be automatically applied if you are creating an issue on GitHub as well. Otherwise you
can find it at `ISSUE_TEMPLATE.md <.github/ISSUE_TEMPLATE.md>`_.

Reporting Security Issues
-------------------------

Please do not report security issues in public. Please email security@edx.org.

Getting Help
------------

If you're having trouble, we have discussion forums at
https://discuss.openedx.org where you can connect with others in the community.

Our real-time conversations are on Slack. You can request a `Slack
invitation`_, then join our `community Slack team`_.

For more information about these options, see the `Getting Help`_ page.

.. _Slack invitation: https://openedx-slack-invite.herokuapp.com/
.. _community Slack team: https://openedx.slack.com/
.. _Getting Help: https://openedx.org/getting-help

.. |pypi-badge| image:: https://img.shields.io/pypi/v/dbt-schema-builder.svg
    :target: https://pypi.python.org/pypi/dbt-schema-builder/
    :alt: PyPI

.. |travis-badge| image:: https://travis-ci.org/edx/dbt-schema-builder.svg?branch=master
    :target: https://travis-ci.org/edx/dbt-schema-builder
    :alt: Travis

.. |codecov-badge| image:: https://codecov.io/github/edx/dbt-schema-builder/coverage.svg?branch=master
    :target: https://codecov.io/github/edx/dbt-schema-builder?branch=master
    :alt: Codecov

.. |doc-badge| image:: https://readthedocs.org/projects/dbt-schema-builder/badge/?version=latest
    :target: https://dbt-schema-builder.readthedocs.io/en/latest/
    :alt: Documentation

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/dbt-schema-builder.svg
    :target: https://pypi.python.org/pypi/dbt-schema-builder/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/edx/dbt-schema-builder.svg
    :target: https://github.com/edx/dbt-schema-builder/blob/master/LICENSE.txt
    :alt: License
