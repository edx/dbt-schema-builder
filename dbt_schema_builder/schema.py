import os

DEFAULT_DESCRIPTION = "TODO: Replace me"


class Relation():

    def __init__(
        self, source_relation_name, meta_data, app, app_path,
        snowflake_keywords, unmanaged_tables
    ):
        self.snowflake_keywords = snowflake_keywords

        self.source_relation_name = source_relation_name
        self.relation = self._get_model_name_alias()
        self.new_safe_relation_name = "{}_{}".format(app, self.relation)
        self.new_pii_relation_name = "{}_PII_{}".format(app, self.relation)

        self.meta_data = meta_data

        self.app = app
        self.app_path = app_path

        self.unmanaged_tables = unmanaged_tables

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
        view-generating models), and
        """
        return (
            "{}.{}".format(self.app, self.relation) in self.unmanaged_tables
        )


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
        was not listed in a whitelist or has otherwise been flagged for exclusion.  A None whitelist
        signifies that all relations are to be included.
        """
        return (
            self.downstream_sources_whitelist
            and "{}.{}".format(self.app, self.relation)
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


class Schema():

    def __init__(
        self, raw_schema, app, app_path, design_file_path, current_raw_sources,
        current_downstream_sources, database
    ):

        # raw scheam: CREDENTIALS_RAW
        # app: CREDENTIALS
        # app_path: 'models/PROD/CREDENTIALS'
        # design file path: models/PROD/CREDENTIALS/CREDENTIALS.yml

        # destination_project_path: /home/stu/Sandbox/oct/warehouse-transforms/projects/reporting
        # database: PROD

        self.raw_schema = raw_schema
        self.app = app
        self.app_path = app_path
        self.design_file_path = design_file_path
        self.current_raw_sources = current_raw_sources
        self.current_downstream_sources = current_downstream_sources
        self.safe_downstream_source_name = app
        self.pii_downstream_source_name = "{}_PII".format(app)

        # Create a new, empty object to store a new version of our schema so we
        # don't get any tables/models that may have been deleted since the last run.
        self.new_schema = {
            "version": 2,
            "sources": [{"name": self.raw_schema, "tables": []}],
            "models": [],
        }

        # Create a new, empty object to store a new version of our downstream
        # sources so we don't get any tables / models that may have been
        # deleted since the last run.
        self.new_downstream_sources = {
            "version": 2,
            "sources": [
                {
                    "name": self.safe_downstream_source_name,
                    "database": database,
                    "tables": [],
                },
                {
                    "name": self.pii_downstream_source_name,
                    "database": database,
                    "tables": [],
                },
            ],
            "models": [],
        }

    def add_table_to_new_schema(self, current_raw_source, relation):
        # Add our table to the "sources" list in the new schema.
        if current_raw_source:
            self.new_schema["sources"][0]["tables"].append(
                current_raw_source
            )
        else:
            self.new_schema["sources"][0]["tables"].append(
                {"name": relation.source_relation_name}
            )

    def add_table_to_downstream_sources():
        # Whenever there is no view generated for a relation, we should not add it to sources in the
        # downstream project.  If we did, the source would be non-functional since it would not be
        # backed by any real data!  No view is generated under the following condition: when the
        # relation is unmanaged AND no manual models exist.
        if relation.is_unmanaged and not relation.manual_safe_model_exists:
            logger.info(
                (
                    "{}.{} is an unmanaged table WITHOUT a manual model, "
                    "skipping inclusion as a source in downstream project."
                ).format(relation.app, relation.relation)
            )
        elif relation.excluded_from_downstream_sources:
            logger.info(
                (
                    "{}.{} is absent from the downstream sources whitelist, "
                    "skipping inclusion as a source in downstream project."
                ).format(relation.app, relation.relation)
            )
        elif current_safe_source:
            for source in new_downstream_sources["sources"]:
                if source["name"] == safe_downstream_source_name:
                    source["tables"].append(current_safe_source)
                elif source["name"] == pii_downstream_source_name:
                    source["tables"].append(current_pii_source)
        else:
            for source in new_downstream_sources["sources"]:
                if source["name"] == safe_downstream_source_name:
                    source["tables"].append(
                        {
                            "name": relation.relation,
                            "description": DEFAULT_DESCRIPTION,
                        }
                    )
                elif source["name"] == pii_downstream_source_name:
                    source["tables"].append(
                        {
                            "name": relation.relation,
                            "description": DEFAULT_DESCRIPTION,
                        }
                    )

    def update_trifecta_models(self, relation):
        for relation_name in [
            relation.new_pii_relation_name,
            relation.new_safe_relation_name,
        ]:
            self.add_model_to_new_schema(
                relation_name, relation.meta_data
            )

    def add_model_to_new_schema(self, new_relation_name, model_meta_data):
        """
        Add models and their columns to a schema that is currently being generated.
        """
        # Add our table to the "models" list in the new schema
        self.new_schema["models"].append({"name": new_relation_name})

        # Add columns to our model
        new_cols = [{"name": c} for c in model_meta_data]

        self.new_schema["models"][-1]["columns"] = new_cols
