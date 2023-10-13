"""
Class and helpers for dealing with Application schemas
"""
from copy import deepcopy

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
        current_downstream_sources, database, no_pii=False, pii_only=False
    ):

        self.raw_schemas = raw_schemas
        self.app = app
        self.app_path = app_path
        self.design_file_path = design_file_path
        self.current_raw_sources = current_raw_sources
        self.current_downstream_sources = current_downstream_sources or {}
        self.safe_downstream_source_name = app
        self.pii_downstream_source_name = "{}_PII".format(app)
        if no_pii and pii_only:
            raise ValueError('Cannot specify both no_pii and pii_only flags as true')
        if no_pii:
            self.add_pii = False
            self.add_safe = True
        elif pii_only:
            self.add_pii = True
            self.add_safe = False
        else:
            self.add_pii = True
            self.add_safe = True

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
        self.new_downstream_sources = self._generate_downstream_sources(database, no_pii, pii_only)

    def _generate_downstream_sources(self, database, no_pii=False, pii_only=False):
        """
        Generates the object to store the version of the downstream
        sources so we don't get any tables / models that may have been
        deleted since the last run. If no_pii flag is set we will exclude
        the downstream source name for that.
        """
        ret_val = {
            "version": 2,
            "sources": deepcopy(self.current_downstream_sources.get('sources', [])),
            "models": [],
        }
        current_sources = {s['name']: i for i, s in enumerate(ret_val['sources'])}
        if not pii_only:
            if self.safe_downstream_source_name in current_sources:
                ret_val['sources'][current_sources[self.safe_downstream_source_name]]['tables'] = []
            else:
                ret_val['sources'].append(
                    {
                        "name": self.safe_downstream_source_name,
                        "database": database,
                        "tables": [],
                    }
                )
        if not no_pii:
            if self.pii_downstream_source_name in current_sources:
                ret_val['sources'][current_sources[self.pii_downstream_source_name]]['tables'] = []
            else:
                ret_val['sources'].append(
                    {
                        "name": self.pii_downstream_source_name,
                        "database": database,
                        "tables": [],
                    }
                )
        return ret_val

    def __repr__(self):
        """
        Makes debugging less of a pain
        """
        return self.app

    def check_downstream_sources_for_dupes(self):
        """
        Checks downstream sources for duplicate tables within the same schema.
        """
        table_dict = {}
        for source in self.new_downstream_sources["sources"]:
            table_dict[source["name"]] = []
            for table in source["tables"]:
                table_dict[source["name"]].append(table["name"])

        seen = set()
        dupes = []
        for schema, table_list in table_dict.items():
            for table_ in table_list:
                qualified_table_name = schema + '.' + table_
                if qualified_table_name in seen:
                    dupes.append(qualified_table_name)
                else:
                    seen.add(qualified_table_name)

        return dupes

    def add_source_to_new_schema(self, current_raw_source, relation, raw_schema):
        """
        Add our table to the appropriate raw schema entry in our "sources" list
        in the new schema.
        """
        for index, item in enumerate(self.new_schema["sources"]):
            if item['name'] == raw_schema.schema_name:
                source_index = index
                break

        self.new_schema["sources"][source_index]["database"] = raw_schema.database

        if current_raw_source:
            self.new_schema["sources"][source_index]["tables"].append(
                current_raw_source
            )
        else:
            self.new_schema["sources"][source_index]["tables"].append(
                {"name": relation.source_relation_name}
            )

    def add_table_to_downstream_sources(
            self,
            relation,
            current_safe_source,
            current_pii_source,
            ):
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
            return
        if relation.excluded_from_downstream_sources:
            logger.info(
                (
                    "{}.{} is absent from the downstream sources allow_list, "
                    "skipping inclusion as a source in downstream project."
                ).format(relation.app, relation.relation)
            )
            return
        for source in self.new_downstream_sources["sources"]:
            if self.add_safe and source["name"] == self.safe_downstream_source_name:
                if current_safe_source:
                    source["tables"].append(current_safe_source)
                else:
                    source["tables"].append(
                        {
                            "name": relation.relation,
                            "description": DEFAULT_DESCRIPTION,
                        }
                    )
            elif self.add_pii and source["name"] == self.pii_downstream_source_name:
                if current_pii_source:
                    source["tables"].append(current_pii_source)
                else:
                    source["tables"].append(
                        {
                            "name": relation.relation,
                            "description": DEFAULT_DESCRIPTION,
                        }
                    )

    def update_trifecta_models(self, relation, no_pii=False, pii_only=False):
        """
        Given a relation, add it to the 'trifecta'. These are the PII and safe views
        constructed from the raw data.
        """
        relations = [relation.new_safe_relation_name] if no_pii else [relation.new_pii_relation_name] if pii_only else [
                relation.new_pii_relation_name,
                relation.new_safe_relation_name,
            ]
        for relation_name in relations:
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
