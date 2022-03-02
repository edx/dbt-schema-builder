"""
The schema builder tool
"""
import glob
import os
import re
import string

import dbt.utils
import yaml
from dbt.config import RuntimeConfig
from dbt.events import AdapterLogger
from dbt.exceptions import DatabaseException
from dbt.logger import log_manager
from dbt.task.compile import CompileTask
from dbt.task.generate import get_adapter

from .app import App
from .queries import COLUMN_NAME_FILTER, GET_RELATIONS_BY_SCHEMA_AND_START_LETTER_SQL, GET_RELATIONS_BY_SCHEMA_SQL
from .relation import Relation
from .schema import InvalidConfigurationException, Schema

# Set up the dbt logger
log_manager.set_path(None)
# log_manager.set_debug()  # Uncomment for dbt's debug level logging

logger = AdapterLogger("Snowflake")

DEFAULT_DESCRIPTION = "TODO: Replace me"
SQL_ESCAPE_CHAR = "^"
LOCAL_PATH = os.path.abspath(os.path.dirname(__file__))


class InvalidDatabaseException(Exception):
    pass


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
    def _get_column_name_filter(self, source_database, banned_column_names):
        """
        Create the SQL string to omit banned_column_names from the Snowflake metadata queries.
        """
        if not banned_column_names:
            return ""

        return COLUMN_NAME_FILTER.format(
            database=source_database,
            banned_column_names=",".join(
                ["'{}'".format(x) for x in banned_column_names]
            ),
        )

    def fetch_full_catalog(self, adapter, source_database, schema, banned_column_names):
        """
        Query Snowflake for all columns in the given schema in one query.
        """
        with adapter.connection_named("generate_catalog"):
            sql = GET_RELATIONS_BY_SCHEMA_SQL.format(
                database=source_database,
                schema=schema,
                column_name_filter=self._get_column_name_filter(source_database, banned_column_names),
            )
            try:
                _, catalog_table = adapter.execute(sql, fetch=True)
            except DatabaseException as e:
                raise InvalidDatabaseException(
                    "The database {} was not found in Snowflake. Make sure schema_config.yml file is "
                    "valid and that the Snowflake user has access to the database in question".format(
                        source_database
                    )
                ) from e

        catalog_data = [
            dict(
                zip(catalog_table.column_names, map(dbt.utils._coerce_decimal, row))  # pylint: disable=protected-access
            )
            for row in catalog_table
        ]

        return catalog_data

    def fetch_catalog_by_letter(self, adapter, source_database, schema, banned_column_names):
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
                    database=source_database,
                    schema=schema,
                    start_letter=start_letter,
                    column_name_filter=self._get_column_name_filter(
                        source_database, banned_column_names
                    ),
                    escape_char=SQL_ESCAPE_CHAR,
                )
                try:
                    _, catalog_tables = adapter.execute(sql, fetch=True)
                except DatabaseException as e:
                    raise InvalidDatabaseException(
                        "The database {} was not found in Snowflake. Make sure schema_config.yml file is "
                        "valid and that the Snowflake user has access to the database in question".format(
                            source_database
                        )
                    ) from e
                all_letters.append(catalog_tables)

        catalog_data = []

        for letter in all_letters:
            catalog_data.extend(
                [
                    dict(zip(letter.column_names, map(_coerce_decimal, row)))  # pylint: disable=undefined-variable
                    for row in letter
                ]
            )

        return catalog_data

    def run(self, source_database, schema, banned_column_names):  # pylint: disable=arguments-differ
        """
        Run the task.
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
            catalog = self.fetch_full_catalog(adapter, source_database, schema, banned_column_names)
        except Exception as e:  # pylint: disable=broad-except
            # TODO: Catch a less-broad exception than Exception.
            if "Information schema query returned too much data" not in str(e):
                raise
            logger.info(
                "Schema too large to fetch at once, fetching by first letter instead."
            )
            catalog = self.fetch_catalog_by_letter(adapter, source_database, schema, banned_column_names)

        return catalog


class SchemaBuilder:
    """
    This class handles the actual heavy lifting of the schema builder.
    """
    def __init__(self,
                 source_path,
                 source_project_path,
                 destination_project_path,
                 get_catalog_task
                 ):
        self.source_path = source_path
        self.source_project_path = source_project_path
        self.destination_project_path = destination_project_path
        self.get_catalog_task = get_catalog_task
        self.redactions = self.get_redactions()
        self.snowflake_keywords = self.get_snowflake_keywords()
        self.banned_column_names = self.get_banned_columns()
        self.unmanaged_tables = self.get_unmanaged_tables()
        self.downstream_sources_allow_list = self.get_downstream_sources_allow_list()

        self.app_schema_configs = self.get_app_schema_configs()

    def get_app_schema_configs(self):
        """
        Load the configuration file "schema_config.yml" into a dictionary for use
        in this task
        """
        schema_config_file_path = os.path.join(self.source_project_path, "schema_config.yml")
        with open(schema_config_file_path, "r") as f:
            config = yaml.safe_load(f)

        self.validate_schema_config(config)

        return config

    @staticmethod
    def validate_schema_config(config):
        """
        Read through an app-schema config dict, making sure that it meets certain
        expectations before proceeding. If not, raise an exception to prevent
        invalid schemas from being built
        """
        valid_keys = ['EXCLUDE', 'INCLUDE', 'SOFT_DELETE']
        database_schema_pattern = re.compile(r'^[A-Za-z0-9_$]+\.[A-Za-z0-9_$]+$')
        for destination_schema, destination_schema_config in config.items():
            if not re.search(database_schema_pattern, destination_schema):
                raise InvalidConfigurationException(
                    "Invalid destination schema path in schema_config.yml. "
                    "These must be in the format <DATABASE_NAME>.<SCHEMA_NAME>. "
                    "Found {}".format(destination_schema)
                )
            for source_schema, source_schema_config in destination_schema_config.items():

                if not re.search(database_schema_pattern, source_schema):
                    raise InvalidConfigurationException(
                        "Invalid source schema path in schema_config.yml. "
                        "These must be in the format <DATABASE_NAME>.<SCHEMA_NAME>. "
                        "Found {}".format(source_schema)
                    )

                # This represents the case in which an application schema does
                # not have any special logic concerning which tables to include
                # or exclude
                if not source_schema_config:
                    continue
                keys = source_schema_config.keys()
                if 'EXCLUDE' in keys and 'INCLUDE' in keys:
                    raise InvalidConfigurationException(
                        "{} has both an EXCLUDE and INCUDE section".format(
                            source_schema
                        )
                    )
                if 'SOFT_DELETE' in keys:
                    soft_delete_key_value = source_schema_config['SOFT_DELETE']
                    if not isinstance(soft_delete_key_value, dict):
                        raise InvalidConfigurationException(
                            "The SOFT_DELETE key in {} must map to the following "
                            "format 'SOFT_DELETE_COLUMN_NAME': 'SOFT_DELETE_VALUE'".format(
                                source_schema
                            )
                        )
                    if len(soft_delete_key_value) != 1:
                        raise InvalidConfigurationException(
                            "The SOFT_DELETE key in {} must only have one key/value pair".format(
                                source_schema
                            )
                        )
                for key in keys:
                    if key not in valid_keys:
                        raise InvalidConfigurationException(
                            "{} is not a valid key".format(key)
                        )
        return True

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
        tables = []
        unmanaged_tables_file_path = os.path.join(
            self.source_project_path, "unmanaged_tables.yml"
        )
        with open(unmanaged_tables_file_path, "r") as f:
            tables = yaml.safe_load(f)

        self.validate_unmanaged_tables(tables)

        return tables

    @staticmethod
    def validate_unmanaged_tables(table_identifiers):
        """
        Ensure that each entry in the unmanaged_tables list is in one of the following
        formats:
        - SCHEMA_NAME.TABLE_NAME
        - SCHEMA_NAME.VALID_REGEX
        Otherwise, raise an InvalidConfigurationException
        """
        if not table_identifiers:
            return True

        table_identifier_regex = re.compile(
            r'^(?P<schema>[A-Za-z0-9_$]+)\.(?P<table>.*)'
        )

        for table_identifier in table_identifiers:
            if not re.search(table_identifier_regex, table_identifier):
                raise InvalidConfigurationException(
                    'Entry "{}" in unmanaged_files.yml is not formatted correctly.'
                    'It must be in one of the following formats: '
                    'SCHEMA_NAME.TABLE_NAME or SCHEMA_NAME.VALID_REGEX'.format(
                        table_identifier
                    )
                )
            table = re.search(
                table_identifier_regex, table_identifier
            ).group('table')
            try:
                re.compile(table)
            except re.error as e:
                raise InvalidConfigurationException(
                    'Entry "{}" in unmanaged_files.yml contains an invalid '
                    'regular expression'.format(table_identifier)
                ) from e
        return True

    def clean_sql_files(self, app, app_path):
        """
        Delete existing SQL models to make sure that we don't have orphaned models for deleted tables.
        """
        # Only delete from these paths so we leave the manual files intact
        for managed_path in ("_PII", ""):
            schema_sql_glob = os.path.join(app_path, app + managed_path, "*.sql")

            for f in glob.glob(schema_sql_glob):
                os.remove(f)

    @staticmethod
    def get_snowflake_keywords():
        with open(os.path.join(LOCAL_PATH, "snowflake_keywords.yml"), "r") as f:
            return yaml.safe_load(f)

    def build_app_path(self, app_destination_database, app_destination_schema):
        """
        Create a path to the directory into which schema files will be built
        """
        db_path = os.path.join(self.source_path, app_destination_database)

        if not os.path.isdir(db_path):
            os.mkdir(db_path)

        app_path = os.path.join(db_path, app_destination_schema)

        if not os.path.isdir(app_path):
            os.mkdir(app_path)

        return app_path

    @staticmethod
    def get_current_raw_schema_attrs(design_file_path):
        """
        Make sure the path exists for this schema, and check if there's an existing file that we need to preserve.
        """
        if os.path.exists(design_file_path):
            with open(design_file_path, "r") as f:
                current_schema = yaml.safe_load(f)

            logger.info("Found existing schema file: {}".format(design_file_path))
        else:
            current_schema = None

        return current_schema

    @staticmethod
    def get_current_downstream_sources_attrs(downstream_sources_dir_path, downstream_sources_file_path):
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

    @staticmethod
    def write_sources_for_downstream_project(sources_file_path, yml):
        """
        Writes out the given schema file with the given string.
        """
        logger.info("Creating sources file: {}".format(sources_file_path))
        with open(sources_file_path, "w") as f:
            f.write(yml)

    def get_relations(self, app_source_database, schema):
        """
        Look up all of the relations in Snowflake using dbt's get_catalog macro.
        """
        all_relations = self.get_catalog_task.run(app_source_database, schema, self.banned_column_names)

        selected_relations = {schema: {}}
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

    def build_app(self, app_name, app_config):
        """
        Build the requested application schema from the raw schemas.
        """
        # Create an App object to represent the current Application
        # that we will be building schemas for
        app_destination_database = app_name.split('.')[0]
        app_destination_schema = app_name.split('.')[1]

        app_path = self.build_app_path(app_destination_database, app_destination_schema)

        design_file_name = "{}.yml".format(app_destination_schema)
        design_file_path = os.path.join(app_path, design_file_name)
        downstream_sources_dir_path = os.path.join(
            self.destination_project_path,
            "models",
            "automatically_generated_sources",
        )
        downstream_sources_file_name = "{}.yml".format(app_destination_schema)
        downstream_sources_file_path = os.path.join(
            downstream_sources_dir_path, downstream_sources_file_name,
        )

        current_raw_sources = self.get_current_raw_schema_attrs(design_file_path)

        current_downstream_sources = self.get_current_downstream_sources_attrs(
            downstream_sources_dir_path, downstream_sources_file_path,
        )

        # Construct the raw schemas that act as sources for this application
        # and gather their relations
        app_raw_schemas = []
        for raw_schema_name, raw_schema_config in app_config.items():
            app_source_database = raw_schema_name.split('.')[0]
            app_source_schema = raw_schema_name.split('.')[1]
            raw_schema = Schema.from_config(
                app_source_schema, raw_schema_config
            )
            raw_schema_relations = self.get_relations(app_source_database, app_source_schema)
            for source_relation_name, meta_data in raw_schema_relations[app_source_schema].items():
                relation = Relation(
                    source_relation_name, meta_data, app_destination_schema,
                    app_path, self.snowflake_keywords,
                    self.unmanaged_tables, self.redactions,
                    self.downstream_sources_allow_list
                )
                raw_schema.relations.append(relation)
            app_raw_schemas.append(raw_schema)

        app_object = App(
            app_raw_schemas, app_destination_schema, app_path, design_file_path, current_raw_sources,
            current_downstream_sources, app_destination_database
        )

        logger.info("Building schema for the {} app".format(app_object.app))

        self.clean_sql_files(app_object.app, app_path)

        # Go through each raw schema that backs this Application, building out
        # the model files for each relation
        for raw_schema in app_object.raw_schemas:
            logger.info("Using raw schema {}".format(raw_schema.schema_name))
            filtered_relations = raw_schema.filter_relations()
            logger.info(
                "Using {} out of {} relations in this schema".format(
                    len(filtered_relations), len(raw_schema.relations)
                )
            )
            for relation in filtered_relations:
                (
                    current_raw_source,
                    current_safe_source,
                    current_pii_source,
                ) = relation.find_in_current_sources(
                    current_raw_sources,
                    current_downstream_sources,
                )

                app_object.add_source_to_new_schema(current_raw_source, relation, app_source_database, raw_schema)
                app_object.add_table_to_downstream_sources(relation, current_safe_source, current_pii_source)
                app_object.update_trifecta_models(relation)

                ##############################
                # Write out dbt models which are responsible for generating the views
                ##############################
                relation.write_sql(raw_schema)

        app_object.write_app_schema(design_file_path)

        # Create source definitions pertaining to app database views in the downstream dbt
        # project, i.e. reporting.
        self.write_sources_for_downstream_project(
            downstream_sources_file_path,
            yaml.safe_dump(app_object.new_downstream_sources, sort_keys=False),
        )


class SchemaBuilderTask:
    """
    This class handles the dbt configuration and wraps the SchemaBuilder steps.
    """

    def __init__(self, args):
        self.args = args
        self.config = RuntimeConfig.from_args(args)
        self.source_project_path, self.destination_project_path = self.get_project_dirs()
        self.builder = SchemaBuilder(
            self.config.model_paths[0],
            self.source_project_path,
            self.destination_project_path,
            GetCatalogTask(self.args, self.config)
        )

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

    def run(self):
        """
        Wraps the SchemaBuilder steps
        """
        with log_manager.applicationbound():
            os.chdir(self.builder.source_project_path)

            for app_name, app_config in self.builder.app_schema_configs.items():
                logger.info('\n')
                logger.info('------- {} -------'.format(app_name))
                self.builder.build_app(app_name, app_config)
