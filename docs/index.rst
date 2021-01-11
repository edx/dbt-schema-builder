.. dbt-schema-builder documentation top level file, created by
   sphinx-quickstart on Tue Jun 02 16:11:12 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

dbt-schema-builder
==================

Automate management of PII redacted schemas for dbt projects on Snowflake.

Setup
-----

See :ref:`getting_started`


Running
-------

Since it piggy-backs on dbt's configuration, Schema Builder must be run from
inside an existing dbt project, see :ref:`getting_started` for more information.

``$ schema_builder build --raw-schemas <source schemas> [--profiles-dir <path
to your dbt profiles.yml>] [--profile <profile name>] [--target <a target from
profiles.yml>]``

Config file

In order to run, you must have a schema config file (``schema_config.yml``)
in the directory in which you are running schema builder. This file must
be in the following format::

    <DESTINATION_DATABASE>.<APPLICATION SCHEMA_1>:
        <SOURCE_DATABASE>.<RAW SCHEMA 1>:
            INCLUDE:
                - TABLE_1
        <SOURCE_DATABASE>.<RAW SCHEMA 2>:
            EXCLUDE:
                - TABLE_1
    <DESTINATION_DATABASE>.<APPLICATION SCHEMA_2>:
        <SOURCE_DATABASE>.<RAW SCHEMA 3>:

At a high level, this file is describing how Schema Builder will use one or more
source schemas to build out the dbt models for a destination schema.

The top level keys in this file, i.e. `<DESTINATION_DATABASE>.<APPLICATION SCHEMA_1>`
define the database and schema name that will be built by Schema Builder. There
will be no change applied to the destinations. These are simply used as
paths when building out a directory of dbt model files.

The next level of keys in this file, i.e. `<SOURCE_DATABASE>.<RAW SCHEMA 3>`,
define the database and schema name in Snowflake from which the current state
of tables and columns will be queried. This structure data, along with any
additional configurations, will be used to generate the dbt models in the
destination schema described above.

NOTE: source schemas and destination schemas can use different databases. This
is particularly useful if you are trying to use a raw schema from a data-share
to create downstream models in a database that your account has ownership of.

In the above example, the ``APPLICATION_SCHEMA_1`` will be built by combining
*ONLY* ``TABLE_1`` from ``RAW_SCHEMA_1`` and all tables *BUT* ``TABLE_1`` from
``RAW_SCHEMA_2``. ``APPLICATION_SCHEMA_2`` will be built from every table in
``RAW_SCHEMA_3``.

NOTE: the order of the ``RAW`` schemas above does not matter.

Another configuration section optionally allows for source tables with soft
deletes (not actually deleted from the source, usually just having a column
that denotes that they have been deleted) to have those soft deleted rows be
excluded from the downstream views.

This feature is turned on and off per-schema and requires you to add the SQL
you would like to *exclude* those rows::

    <DATABASE>.<APPLICATION SCHEMA_1>:
        <DATABASE>.<RAW SCHEMA 1>:
            SOFT_DELETE:
                DELETED_AT: IS NOT NULL

This configuration will add the following SQL to the downstream views:

``WHERE DELETED_AT IS NOT NULL``

Required Parameters
-------------------

``--destination-project`` - the dbt project that will use the generated
sources. Schema Builder will create or overwrite the source file(s) associated
with the schemas passed in to ``--raw-schemas`` for this project. This path
is relative to the source project path (the path that Schema Builder is being
run from).

Options

``--profiles-dir`` - the path to your dbt profiles.yml, defaults to
~/.dbt/profiles.yml

``--profile`` -  the profile name to use from your profiles.yml, defaults to
the "profile" value in ``dbt_project.yml``

``--target`` -  a valid target from your profiles.yml, defaults to the default
target in your chosen profile

If you have you views you do not want to include in your downstream models, add
those that you want to include to a file entitled
``downstream_sources_allow_list.yml``. This file should be placed in the
directory that you run dbt-schema-builder from. Entries should be in the
following format:

``"<SCHEMA>.<TABLE>"``

All others will be omitted when the dbt-schema-builder is run. If you want to
permit all downstream views to be created, do not add this file.

Redacting PII
-------------
See :ref:`redacting_pii`

Contents:

.. toctree::
   :maxdepth: 2

   readme
   getting_started
   redacting_pii
   testing
   internationalization
   modules


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
