dbt-schema-builder
=============================

|pypi-badge| |ci-badge| |codecov-badge| |pyversions-badge|
|license-badge|

The Schema Builder tool is used to create dbt schema files, sql models, and
default PII / non-PII views for tables in the given Snowflake schemas.

For each specified application schema, the script will generate dbt models for
a ``<SCHEMA>`` and ``<SCHEMA>_PII`` schema. We refer to these schemas as a
"trifecta".

* ``<SCHEMA>_<RAW_SUFFIX>`` contains the original source tables.
* ``<SCHEMA>_PII`` contains views on the _RAW tables that have un-redacted PII.
* ``<SCHEMA>`` contains views on the _RAW tables sensitive data redacted.

Application schemas can be sourced from multiple raw schemas. This allows you
to specify which tables should be pulled from which raw schema to construct the
"trifecta".

Schema Builder ensures that all three schemas provide the same interface to the
data (number and order of columns match what is present in the _RAW schema).

Once the script is successfully run, you can execute a `dbt run` to create or
update the views in ``<SCHEMA>`` and ``<SCHEMA>_PII``. If your source data in
the ``<SCHEMA>_<RAW_SUFFIX>`` schema changes you should run Schema Builder frequently
to keep up with changes in the tables and columns stored there.

Schema Builder will also automatically create sources in one or more other dbt
projects so that they can use the results of these models as sources.

See `the docs <https://dbt-schema-builder.readthedocs.io/en/latest/>`_ for more
information.


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

.. |ci-badge| image:: https://github.com/edx/dbt-schema-builder/workflows/Python%20CI/badge.svg?branch=master
        :target: https://github.com/edx/dbt-schema-builder/actions?query=workflow%3A%22Python+CI%22
    :alt: GitHub CI

.. |codecov-badge| image:: https://codecov.io/github/edx/dbt-schema-builder/coverage.svg?branch=main
    :target: https://codecov.io/github/edx/dbt-schema-builder?branch=main
    :alt: Codecov

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/dbt-schema-builder.svg
    :target: https://pypi.python.org/pypi/dbt-schema-builder/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/edx/dbt-schema-builder.svg
    :target: https://github.com/edx/dbt-schema-builder/blob/main/LICENSE.txt
    :alt: License
