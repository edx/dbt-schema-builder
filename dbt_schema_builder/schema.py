import os
from dbt.config import RuntimeConfig

DEFAULT_DESCRIPTION = "TODO: Replace me"



class Relation(object):

    def __init__(self, source_relation_name, metadata, app, app_path, snowflake_keywords):
        self.snowflake_keywords = snowflake_keywords

        self.source_relation_name = source_relation_name
        self.relation = self._get_model_name_alias()
        self.new_safe_relation_name = "{}_{}".format(app, self.relation)
        self.new_pii_relation_name = "{}_PII_{}".format(app, self.relation)

        self.metadata = metadata

        self.app = app
        self.app_path = app_path

    def _get_model_name_alias(self):
        if self.source_relation_name in self.snowflake_keywords:
            return "_{}".format(self.source_relation_name)
        else:
            return self.source_relation_name 

    def prep_metadata(self):
        """
        Transforms the data we receive back from Snowflake / dbt to a more usable form.
        """
        columns = []

        for colname in self.metadata:
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

    def is_unmanaged(self, unmanaged_tables):
        """
        Is it "unmanaged" (i.e. it has been added to the list of unmanaged tables in
        unmanaged_tables.yml, indicating that we do not want schema builder to manage this table's
        view-generating models), and
        """
        return (
            "{}.{}".format(app, relation) in unmanaged_tables
        )


    # make a property
    def manual_safe_model_exists(self):
        """
        Manual models exist for it (i.e. someone has gone into the {APP}_MANUAL directory for this app
        and manually written view-generating models for this relation).
        """
        return self._manual_model_exists(view_type="SAFE")

    def excluded_from_downstream_sources(self):
        """
        Views generated for this relation are to be excluded in downstream sources since the relation
        was not listed in a whitelist or has otherwise been flagged for exclusion.  A None whitelist
        signifies that all relations are to be included.
        """
        return (
            self.downstream_sources_whitelist
            and "{}.{}".format(app, relation)
            not in self.downstream_sources_whitelist
        )

    def _manual_model_exists(self, view_type):
        """
        Return true if a manual model exists for the given relation.

        Args:
            app (str): name of app, e.g. "LMS".
            app_path (str): path to app directory, e.g. "models/PROD/LMS".
            relation (str): name of relation, e.g. "AUTH_USER".
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

            manual_model_name = self._get_model_name(view_type)
            manual_model_path = os.path.join(
                manual_models_directory, "{}.sql".format(manual_model_name)
            )
            if os.path.exists(manual_model_path):
                return True
        return False

    def _get_model_name(self, view_type):
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
