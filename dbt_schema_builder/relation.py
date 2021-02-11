"""
Class and helpers for dealing with DBT relations
"""

import os
import re

import jinja2
from dbt.logger import GLOBAL_LOGGER as logger

DEFAULT_DESCRIPTION = "TODO: Replace me"

# Set up our SQL templates
TEMPLATE_LOADER = jinja2.PackageLoader("dbt_schema_builder", "templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)
SQL_TEMPLATE_PII = TEMPLATE_ENV.get_template("model_sql_pii.tpl")
SQL_TEMPLATE_SAFE = TEMPLATE_ENV.get_template("model_sql_safe.tpl")


class Relation:
    """
    Class to represent a DBT relation (a table/view)
    """

    def __init__(
        self, source_relation_name, meta_data, app, app_path,
        snowflake_keywords, unmanaged_tables, redactions, downstream_sources_allow_list
    ):
        self.snowflake_keywords = snowflake_keywords
        self.redactions = redactions

        self.source_relation_name = source_relation_name
        self.relation = self._get_model_name_alias()
        self.new_safe_relation_name = "{}_{}".format(app, self.relation)
        self.new_pii_relation_name = "{}_PII_{}".format(app, self.relation)

        self.meta_data = meta_data

        self.app = app
        self.app_path = app_path

        self.unmanaged_tables = unmanaged_tables
        self.downstream_sources_allow_list = downstream_sources_allow_list

    def __repr__(self):
        return self.source_relation_name

    def _get_model_name_alias(self):
        if self.source_relation_name in self.snowflake_keywords:
            return "_{}".format(self.source_relation_name)
        else:
            return self.source_relation_name

    def prep_meta_data(self):
        """
        Transforms the data we receive back from Snowflake / dbt to a more usable form.
        """
        columns = []

        for colname in self.meta_data:
            column = {"name": colname.upper()}
            columns.append(column)

        model = {
            "name": self.source_relation_name,
            "alias": self.relation,
            "description": DEFAULT_DESCRIPTION,
            "columns": columns,
        }

        return model

    def find_in_current_sources(
        self, current_raw_sources, current_downstream_sources
    ):
        """
        Find source data in an existing loaded schema yml file.

        If a file already exists for this schema, find the values for the current relation
        so we can preserve any manual modifications (tests, description, etc.).
        """
        if not current_raw_sources and not current_downstream_sources:
            return None, None, None

        current_raw_source = None
        current_safe_downstream_source = None
        current_pii_downstream_source = None

        if current_raw_sources and "sources" in current_raw_sources:
            for source in current_raw_sources["sources"]:
                for table in source["tables"]:
                    if table["name"] == self.source_relation_name:
                        current_raw_source = table
                        break

        if current_downstream_sources and "sources" in current_downstream_sources:
            for source in current_downstream_sources["sources"]:
                if source["name"] == self.app:
                    for table in source["tables"]:
                        if table["name"] == self.source_relation_name:
                            current_safe_downstream_source = table
                elif source["name"] == "{}_PII".format(self.app):
                    for table in source["tables"]:
                        if table["name"] == self.source_relation_name:
                            current_pii_downstream_source = table

                if current_safe_downstream_source and current_pii_downstream_source:
                    break

        return (
            current_raw_source,
            current_safe_downstream_source,
            current_pii_downstream_source,
        )

    @property
    def is_unmanaged(self):
        """
        Is it "unmanaged" (i.e. it has been added to the list of unmanaged tables in
        unmanaged_tables.yml, indicating that we do not want schema builder to manage this table's
        view-generating models)
        """
        for unmanaged_table in self.unmanaged_tables:
            # make sure to include the EOL character in the regex, to prevent
            # matching a substring in a larger string.
            unmanaged_table_regex = re.compile(r'{}$'.format(unmanaged_table))
            relation_name = "{}.{}".format(self.app, self.relation)
            if re.search(unmanaged_table_regex, relation_name):
                return True
        return False

    @property
    def manual_safe_model_exists(self):
        """
        Manual models exist for it (i.e. someone has gone into the {APP}_MANUAL directory for this app
        and manually written view-generating models for this relation).
        """
        return self._manual_model_exists(view_type="SAFE")

    @property
    def excluded_from_downstream_sources(self):
        """
        Views generated for this relation are to be excluded in downstream sources since the relation
        was not listed in an allow_list or has otherwise been flagged for exclusion.  An empty allow_list
        signifies that all relations are to be included.
        """
        return (
            self.downstream_sources_allow_list
            and "{}.{}".format(self.app, self.relation)
            not in self.downstream_sources_allow_list
        )

    def _manual_model_exists(self, view_type):
        """
        Return true if a manual model exists for the given relation.

        Args:
            view_type (str): either "SAFE" or "PII".

        Raises:
            RuntimeError: When the manual models directory is not flat.
        """
        manual_models_directory = os.path.join(self.app_path, "{}_MANUAL".format(self.app))
        if os.path.isdir(manual_models_directory):

            # Ensure that the directory contents are flat.
            if not self._dir_is_flat(manual_models_directory):
                raise RuntimeError(
                    'MANUAL directory is not "flat", i.e. it contains subdirectories: {}'.format(
                        manual_models_directory,
                    )
                )

            manual_model_name = self.get_model_name(view_type)
            manual_model_path = os.path.join(
                manual_models_directory, "{}.sql".format(manual_model_name)
            )
            if os.path.exists(manual_model_path):
                return True
        return False

    def get_model_name(self, view_type):
        """
        Get the model name for a given relation in the UPSTREAM project.  This
        will be the model filename without the .sql extension, and also the
        name of the model in `ref()`s in the upstream dbt project.
        """
        if view_type == "SAFE":
            return "{}_{}".format(self.app, self.relation)
        else:
            return "{}_{}_{}".format(self.app, view_type, self.relation)

    def _dir_is_flat(self, path):
        """
        Returns True iff the given directory is "flat", i.e. contains no subdirectories.
        """
        for entry in os.scandir(path):
            if entry.is_dir():
                return False
        return True

    @staticmethod
    def render_sql(app, view_type, relation_dict, raw_schema, redactions):
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
            redactions=redactions,
        )

    @staticmethod
    def write_sql_file(sql_file_path, sql):
        """
        Writes out the given SQL file with the given string.
        """
        with open(sql_file_path, "w") as f:
            f.write(sql)

    def write_sql(self, raw_schema):
        """
        Renders the SQL for this relation and writes out.
        """
        relation_dict = self.prep_meta_data()

        if self.is_unmanaged:
            logger.info(
                "{}.{} is an unmanaged table, skipping SQL generation.".format(
                    self.app, self.relation
                )
            )
        else:
            for view_type in ("SAFE", "PII"):
                if view_type == "SAFE":
                    sql_path = os.path.join(self.app_path, self.app)
                else:
                    sql_path = os.path.join(
                        self.app_path, "{}_{}".format(self.app, view_type)
                    )

                if not os.path.isdir(sql_path):
                    os.mkdir(sql_path)
                model_name = self.get_model_name(view_type)
                sql_file_name = "{}.sql".format(model_name)
                sql_file_path = os.path.join(sql_path, sql_file_name)
                sql = self.render_sql(
                    self.app, view_type, relation_dict, raw_schema, self.redactions
                )
                self.write_sql_file(sql_file_path, sql)
