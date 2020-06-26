"""
The schema builder tool
"""
import glob
import os
import re
import string

import jinja2
import yaml
from dbt.config import RuntimeConfig
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.logger import log_manager
from dbt.task.compile import CompileTask
from dbt.task.generate import _coerce_decimal, get_adapter

from .queries import COLUMN_NAME_FILTER, GET_RELATIONS_BY_SCHEMA_AND_START_LETTER_SQL, GET_RELATIONS_BY_SCHEMA_SQL
from .relation import Relation
from .schema import Schema

# Set up the dbt logger
log_manager.set_path(None)
# log_manager.set_debug()  # Uncomment for dbt's debug level logging

# Set up our SQL templates
LOCAL_PATH = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_LOADER = jinja2.PackageLoader("dbt_schema_builder", "templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)
SQL_TEMPLATE_PII = TEMPLATE_ENV.get_template("model_sql_pii.tpl")
SQL_TEMPLATE_SAFE = TEMPLATE_ENV.get_template("model_sql_safe.tpl")
SQL_ESCAPE_CHAR = "^"

DEFAULT_DESCRIPTION = "TODO: Replace me"


class GetCatalogTask(CompileTask):
    """
    A dbt task to load the information schema to dict in the form of:
    {
        'SCHEMA_NAME': {
            'TABLE_NAME_1': [
                    'COLUMN_1_NAME',
                    'COLUMN_2_NAME',
                    ...
                ],
            'TABLE_NAME_2': [
                    'COLUMN_3_NAME',
                    'COLUMN_3_NAME',
                    ...
                ],
        }
    }
    """

    def _get_column_name_filter(self, banned_column_names):
        """
        Create the SQL string to omit banned_column_names from the Snowflake metadata queries.
        """
        if not banned_column_names:
            return ""

        return COLUMN_NAME_FILTER.format(
            database=self.config.credentials.database,
            banned_column_names=",".join(
                ["'{}'".format(x) for x in banned_column_names]
            ),
        )

    def fetch_full_catalog(self, adapter, schema, banned_column_names):
        """
        Query Snowflake for all columns in the given schema in one query.
        """
        with adapter.connection_named("generate_catalog"):
            sql = GET_RELATIONS_BY_SCHEMA_SQL.format(
                database=self.config.credentials.database,
                schema=schema,
                column_name_filter=self._get_column_name_filter(banned_column_names),
            )
            _, catalog_table = adapter.execute(sql, fetch=True)

        catalog_data = [
            dict(zip(catalog_table.column_names, map(_coerce_decimal, row)))
            for row in catalog_table
        ]

        return catalog_data

    def fetch_catalog_by_letter(self, adapter, schema, banned_column_names):
        """
        Query Snowflake for all columns in the given schema over several queries.

        Snowflake has an issue when too much data is returned from these kinds of queries that requires us to break
        up the queries into smaller chunks sometimes. We fall back on this method when fetch_full_catalog fails.
        """
        with adapter.connection_named("generate_catalog"):
            all_letters = []

            for start_letter in "_{}".format(string.ascii_uppercase):
                # Need to escape underscores in LIKE. The ^ is less syntax confusing than backslash.
                if start_letter == "_":
                    start_letter = SQL_ESCAPE_CHAR + start_letter

                # Get the list of table names for this schema
                sql = GET_RELATIONS_BY_SCHEMA_AND_START_LETTER_SQL.format(
                    database=self.config.credentials.database,
                    schema=schema,
                    start_letter=start_letter,
                    column_name_filter=self._get_column_name_filter(
                        banned_column_names
                    ),
                    escape_char=SQL_ESCAPE_CHAR,
                )
                _, catalog_tables = adapter.execute(sql, fetch=True)
                all_letters.append(catalog_tables)

        catalog_data = []

        for letter in all_letters:
            catalog_data.extend(
                [
                    dict(zip(letter.column_names, map(_coerce_decimal, row)))
                    for row in letter
                ]
            )

        return catalog_data

    def run(self, schema, banned_column_names):  # pylint: disable=arguments-differ
        """
        Run the task.

        TODO: Explain why parameters must differ from overridden 'run' method, forcing us to ignore a pylint warning.
        """
        # Check for any non-word characters that might indicate a SQL injection attack
        if re.search("[^a-zA-Z0-9_]", schema):
            raise Exception(
                "Non-word character in schema name '{}'! Possible SQL injection?".format(
                    schema
                )
            )

        adapter = get_adapter(self.config)

        try:
            catalog = self.fetch_full_catalog(adapter, schema, banned_column_names)
        except Exception as e:  # pylint: disable=broad-except
            # TODO: Catch a less-broad exception than Exception.
            if "Information schema query returned too much data" not in str(e):
                raise
            logger.info(
                "Schema too large to fetch at once, fetching by first letter instead."
            )
            catalog = self.fetch_catalog_by_letter(adapter, schema, banned_column_names)

        return catalog


class SchemaBuilderTask:
    """
    This class handles the actual heavy lifting of the schema builder.
    """

    def __init__(self, args):
        self.args = args
        self.config = RuntimeConfig.from_args(args)

        (
            self.source_project_path,
            self.destination_project_path,
        ) = self.get_project_dirs()
        self.redactions = self.get_redactions()
        self.banned_column_names = self.get_banned_columns()
        self.unmanaged_tables = self.get_unmanaged_tables()
        self.downstream_sources_allow_list = self.get_downstream_sources_allow_list()

    def get_project_dirs(self):
        """
        Find the dbt project directory based on the command line inputs.
        """
        source_project_path = os.getcwd()
        destination_project_path = os.path.join(
            source_project_path, self.args.destination_project
        )

        for project_path in [source_project_path, destination_project_path]:
            if not os.path.exists(os.path.join(project_path, "dbt_project.yml")):
                raise Exception(
                    "fatal: {} is not a dbt project. Does not exist or is missing a "
                    "dbt_project.yml file.".format(project_path)
                )

        return source_project_path, destination_project_path

    def get_banned_columns(self):
        """
        Loads the banned columns file into a local list.

        This file contains any column names we never wish to propagate up.
        """
        banned_column_file_path = os.path.join(
            self.source_project_path, "banned_column_names.yml"
        )
        with open(banned_column_file_path, "r") as f:
            banned_columns = yaml.safe_load(f)

        return banned_columns if banned_columns else []

    def get_redactions(self):
        """
        Loads the redactions file into a local dict.

        This file configures our PII replacement on a field-by-field basis.
        """
        redaction_file_path = os.path.join(self.source_project_path, "redactions.yml")
        with open(redaction_file_path, "r") as f:
            redactions = yaml.safe_load(f)

        return redactions if redactions else {}

    def get_downstream_sources_allow_list(self):
        """
        Loads the downstream_sources_allow_list.yml file into a local list.

        This file allows us to include certain views as downstream sources.

        Returns:
          List of tables in the "<SCHEMA>.<TABLE>" format or None if the file
          does not exist.

        Raises:
          ValueError: When the file exists but is empty, contains an empty
          list, or contains something other than a list.
        """
        yml_file_path = os.path.join(
            self.source_project_path, "downstream_sources_allow_list.yml"
        )
        tables = None
        if os.path.exists(yml_file_path):
            with open(yml_file_path, "r") as f:
                tables = yaml.safe_load(f)
            if not tables or not isinstance(tables, list):
                raise ValueError(
                    "downstream_sources_allow_list.yml must contain a non-empty list."
                )
        return tables

    def get_unmanaged_tables(self):
        """
        Loads the unmanaged tables file into a local list.

        This file allows us to skip certain tables from being managed by this process.
        """
        unmanaged_tables_file_path = os.path.join(
            self.source_project_path, "unmanaged_tables.yml"
        )
        with open(unmanaged_tables_file_path, "r") as f:
            tables = yaml.safe_load(f)

        # The file may be empty, in which case safe_load() will return None.
        # We simply treat that as an empty list.
        return tables if tables else []

    def render_sql(self, app, view_type, relation_dict, raw_schema):
        """
        Renders the appropriate SQL file template for the source and returns the rendered string.
        """
        if view_type == "SAFE":
            tpl = SQL_TEMPLATE_SAFE
        else:
            tpl = SQL_TEMPLATE_PII

        return tpl.render(
            app=app,
            raw_schema=raw_schema,
            relation=relation_dict,
            redactions=self.redactions,
        )

    def write_relation(self, design_file_path, yml):
        """
        Writes out the given schema file with the given string.
        """
        logger.info("Creating schema file: {}".format(design_file_path))
        with open(design_file_path, "w") as f:
            f.write(yml)

    def write_sql(self, sql_file_path, sql):
        """
        Writes out the given SQL file with the given string.
        """
        with open(sql_file_path, "w") as f:
            f.write(sql)

    def clean_sql_files(self, app):
        """
        Delete existing SQL models to make sure that we don't have orphaned models for deleted tables.
        """
        app_path = os.path.join(
            self.config.source_paths[0], self.config.credentials.database, app
        )

        # Only delete from these paths so we leave the manual files intact
        for managed_path in ("_PII", ""):
            schema_sql_glob = os.path.join(app_path, app + managed_path, "*.sql")

            for f in glob.glob(schema_sql_glob):
                os.remove(f)

    def get_relations(self, schema):
        """
        Look up all of the relations in Snowflake using dbt's get_catalog macro.

        If you see an error like "Warning: No relations found in selected schemas", please see the section on adding new
        schemas in the README for more info.
        """
        task = GetCatalogTask((), self.config)
        all_relations = task.run(schema, self.banned_column_names)

        selected_relations = {}
        selected_relations[schema] = {}
        curr_table_name = None
        curr_table_cols = None

        for r in all_relations:
            if r["TABLE_NAME"] != curr_table_name:
                if curr_table_name:
                    selected_relations[schema][curr_table_name] = curr_table_cols
                curr_table_name = r["TABLE_NAME"]
                curr_table_cols = []
            curr_table_cols.append(r["COLUMN_NAME"])

        if curr_table_name:
            selected_relations[schema][curr_table_name] = curr_table_cols

        return selected_relations

    def get_current_raw_schema_attrs(self, app_path, design_file_path):
        """
        Make sure the path exists for this schema, and check if there's an existing file that we need to preserve.
        """
        if not os.path.isdir(app_path):
            os.mkdir(app_path)

        if os.path.exists(design_file_path):
            with open(design_file_path, "r") as f:
                current_schema = yaml.safe_load(f)

            logger.info("Found existing schema file: {}".format(design_file_path))
        else:
            current_schema = None

        return current_schema

    def get_current_downstream_sources_attrs(
        self, downstream_sources_dir_path, downstream_sources_file_path
    ):
        """
        Make sure the path exists for holding downstream sources files, and check if there's an existing file that we
        need to preserve.
        """
        if not os.path.isdir(downstream_sources_dir_path):
            os.mkdir(downstream_sources_dir_path)

        if os.path.exists(downstream_sources_file_path):
            with open(downstream_sources_file_path, "r") as f:
                current_downstream_sources = yaml.safe_load(f)

            logger.info(
                "Found existing downstream sources file: {}".format(
                    downstream_sources_file_path
                )
            )
        else:
            current_downstream_sources = None

        return current_downstream_sources

    def write_sources_for_downstream_project(self, sources_file_path, yml):
        """
        Writes out the given schema file with the given string.
        """
        logger.info("Creating sources file: {}".format(sources_file_path))
        with open(sources_file_path, "w") as f:
            f.write(yml)

    def remove_suffix(self, source_name, raw_suffixes):
        """
        Remove suffix/es from raw source name.
        """
        for suffix in raw_suffixes:
            if source_name.endswith(suffix):
                return source_name[: -len(suffix)]
        raise ValueError("No suffix found in source :{}".format(source_name))

    def run(self):
        """
        Build the requested schemas.
        """
        # pylint: disable=too-many-nested-blocks,too-many-statements
        # TODO: simplify this function, it doesn't fit on one page and goes too deep.

        with log_manager.applicationbound():
            os.chdir(self.source_project_path)

            with open(os.path.join(LOCAL_PATH, "snowflake_keywords.yml"), "r") as f:
                snowflake_keywords = yaml.safe_load(f)

            raw_schemas = self.args.raw_schemas
            raw_suffixes = self.args.raw_suffixes
            # We are sorting on length since we want to remove suffix from raw_schemas in this order.
            raw_suffixes.sort(key=len, reverse=True)

            if not all(
                "_" in raw_schema and raw_schema.rsplit("_", 1)[-1] == "RAW"
                for raw_schema in raw_schemas
            ):
                raise Exception('Expecting all input schemas to end with "_RAW".')

            for schema in raw_schemas:
                app = self.remove_suffix(schema, raw_suffixes)
                logger.info("Building schema for the {} app".format(app))
                relations_to_build = self.get_relations(schema)

                found_relations = [
                    len(relations_to_build[key]) for key in relations_to_build
                ]

                if not sum(found_relations):
                    logger.error(
                        "No relations found in selected schemas: {}."
                        "\nYou may need to create a stub schema.yml file for this source to make this work. "
                        "Check README.".format(raw_schemas)
                    )
                    logger.error("Aborting.")
                    return

                # Don't clean these files until after we've gotten our catalog, for some reason it causes dbt to fail to
                # connect to Snowflake.
                self.clean_sql_files(app)

                for raw_schema, relations in relations_to_build.items():

                    app = self.remove_suffix(raw_schema, raw_suffixes)
                    app_path = os.path.join(
                        self.config.source_paths[0],
                        self.config.credentials.database,
                        app,
                    )
                    design_file_name = "{}.yml".format(app)
                    design_file_path = os.path.join(app_path, design_file_name)
                    downstream_sources_dir_path = os.path.join(
                        self.destination_project_path,
                        "models",
                        "automatically_generated_sources",
                    )
                    downstream_sources_file_name = "{}.yml".format(app)
                    downstream_sources_file_path = os.path.join(
                        downstream_sources_dir_path, downstream_sources_file_name,
                    )

                    current_raw_sources = self.get_current_raw_schema_attrs(
                        app_path, design_file_path
                    )

                    current_downstream_sources = self.get_current_downstream_sources_attrs(
                        downstream_sources_dir_path, downstream_sources_file_path,
                    )

                    schema_object = Schema(
                        raw_schema, app, app_path, design_file_path, current_raw_sources,
                        current_downstream_sources, self.config.credentials.database
                    )

                    # Sort by table names here, so that the output is deterministic and can be diff'd
                    for source_relation_name in relations:

                        meta_data = relations[source_relation_name]
                        relation = Relation(
                            source_relation_name, meta_data, app, app_path, snowflake_keywords,
                            self.unmanaged_tables, self.downstream_sources_allow_list
                        )

                        (
                            current_raw_source,
                            current_safe_source,
                            current_pii_source,
                        ) = relation.find_in_current_sources(
                            current_raw_sources,
                            current_downstream_sources,
                        )

                        schema_object.add_table_to_new_schema(current_raw_source, relation)

                        schema_object.add_table_to_downstream_sources(relation, current_safe_source, current_pii_source)

                        schema_object.update_trifecta_models(relation)

                        ##############################
                        # Write out dbt models which are responsible for generating the views
                        ##############################

                        relation_dict = relation.prep_meta_data()

                        if relation.is_unmanaged:
                            logger.info(
                                "{}.{} is an unmanaged table, skipping SQL generation.".format(
                                    relation.app, relation.relation
                                )
                            )
                        else:
                            for view_type in ("SAFE", "PII"):
                                if view_type == "SAFE":
                                    sql_path = os.path.join(app_path, app)
                                else:
                                    sql_path = os.path.join(
                                        app_path, "{}_{}".format(app, view_type)
                                    )

                                if not os.path.isdir(sql_path):
                                    os.mkdir(sql_path)
                                model_name = relation._get_model_name(view_type)  # pylint: disable=protected-access
                                sql_file_name = "{}.sql".format(model_name)
                                sql_file_path = os.path.join(sql_path, sql_file_name)
                                sql = self.render_sql(
                                    app, view_type, relation_dict, raw_schema
                                )
                                self.write_sql(sql_file_path, sql)

                    self.write_relation(
                        design_file_path, yaml.safe_dump(schema_object.new_schema, sort_keys=False)
                    )

                    # Create source definitions pertaining to app database views in the downstream dbt
                    # project, i.e. reporting.
                    self.write_sources_for_downstream_project(
                        downstream_sources_file_path,
                        yaml.safe_dump(schema_object.new_downstream_sources, sort_keys=False),
                    )
