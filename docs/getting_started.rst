.. _getting_started:

Getting Started
===============

If you have not already done so, create/activate a `virtualenv`_. Unless
otherwise stated, assume all terminal code below is executed within the
virtualenv. The virtualenv will need Python 3.6+ as it uses dbt under the hood.

.. _virtualenv: https://virtualenvwrapper.readthedocs.org/en/latest/


Install dependencies
--------------------
Dependencies can be installed via the command below.

.. code-block:: bash

    $ make requirements


dbt profile
-----------

In order to use this script you will need a dbt profile pointing to your
personal Snowflake database:
https://docs.getdbt.com/docs/configure-your-profile

In order for this tool to work you will need the following Snowflake
permissions on the role you use in profiles.yml:

- USE and SELECT on the source schema (ex. PRODUCT_RAW)
- CREATE VIEW on the target schemas (ex. PRODUCT, PRODUCT_PII)

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
           schema: PRODUCT_RAW
           threads: 2
           client_session_keep_alive: False



dbt project
-----------

Your Schema Builder project should be separate from that used by end users
since it will create numerous objects and clutter the namespace, as well as
slow down your dbt runs. You can create an empty dbt project and on the first
run Schema Builder should create all of the necessary model directories and
files for you.
