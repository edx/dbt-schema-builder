"""
Class and helpers for dealing with Application schemas
"""
import yaml
from dbt.logger import GLOBAL_LOGGER as logger

from .relation import DEFAULT_DESCRIPTION


class App:
    """
    Class to represent an application whose data is managed by DBT and provides
    functionality for updating its downstreams. An app can be backed by multiple
    raw schemas.
    """

    def __init__(
        self, raw_schemas, app, app_path, design_file_path, current_raw_sources,
        current_downstream_sources, database
    ):

        self.raw_schemas = raw_schemas
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
            "sources": [
                {"name": rs.schema_name, "database": database, "tables": []}
                for rs in self.raw_schemas
            ],
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

    def __repr__(self):
        """
        Makes debugging less of a pain
        """
        return self.app

    def add_source_to_new_schema(self, current_raw_source, relation, source_database, raw_schema):
        """
        Add our table to the appropriate raw schema entry in our "sources" list
        in the new schema.
        """
        for index, item in enumerate(self.new_schema["sources"]):
            if item['name'] == raw_schema.schema_name:
                source_index = index
                break

        self.new_schema["sources"][source_index]["database"] = source_database

        if current_raw_source:
            self.new_schema["sources"][source_index]["tables"].append(
                current_raw_source
            )
        else:
            self.new_schema["sources"][source_index]["tables"].append(
                {"name": relation.source_relation_name}
            )

    def add_table_to_downstream_sources(self, relation, current_safe_source, current_pii_source):
        """
        Whenever there is no view generated for a relation, we should not add it to sources in the
        downstream project.  If we did, the source would be non-functional since it would not be
        backed by any real data!  No view is generated under the following condition: when the
        relation is unmanaged AND no manual models exist.
        """
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
                    "{}.{} is absent from the downstream sources allow_list, "
                    "skipping inclusion as a source in downstream project."
                ).format(relation.app, relation.relation)
            )
        elif current_safe_source:
            for source in self.new_downstream_sources["sources"]:
                if source["name"] == self.safe_downstream_source_name:
                    source["tables"].append(current_safe_source)
                elif source["name"] == self.pii_downstream_source_name:
                    source["tables"].append(current_pii_source)
        else:
            for source in self.new_downstream_sources["sources"]:
                if source["name"] == self.safe_downstream_source_name:
                    source["tables"].append(
                        {
                            "name": relation.relation,
                            "description": DEFAULT_DESCRIPTION,
                        }
                    )
                elif source["name"] == self.pii_downstream_source_name:
                    source["tables"].append(
                        {
                            "name": relation.relation,
                            "description": DEFAULT_DESCRIPTION,
                        }
                    )

    def update_trifecta_models(self, relation):
        """
        Given a relation, add it to the 'trifecta'. These are the PII and safe views
        constructed from the raw data.
        """
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

    def write_app_schema(self, design_file_path):
        """
        Writes out the given schema file with the given string.
        """
        logger.info("Creating schema file: {}".format(design_file_path))

        with open(design_file_path, "w") as f:
            f.write(yaml.safe_dump(self.new_schema, sort_keys=False))
