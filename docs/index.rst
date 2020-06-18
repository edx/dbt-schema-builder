.. dbt-schema-builder documentation master file, created by
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

Required Parameters

``--raw-schemas`` - a space separated list of source schemas to work on. For
every table in this schema a view will be created in the ``<SCHEMA>`` and
``<SCHEMA_PII>`` schema based on your settings.

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
