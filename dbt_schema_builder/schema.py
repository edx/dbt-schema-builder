"""
Class and helpers for dealing with raw schemas
"""


class Schema:
    """
    Class to represent a raw Schema used to back an application schema
    """

    def __init__(self, schema_name, exclusion_list, inclusion_list, soft_delete_column_name,
                 soft_delete_sql_predicate, relations=None):
        self.schema_name = schema_name
        self.exclusion_list = exclusion_list
        self.inclusion_list = inclusion_list
        self.soft_delete_column_name = soft_delete_column_name
        self.soft_delete_sql_predicate = soft_delete_sql_predicate
        self.relations = relations

        self.validate()

    def __repr__(self):
        return self.schema_name

    def validate(self):
        """
        Confirm that the class is instantiated correctly.
        """
        if self.soft_delete_column_name is not None:
            if self.soft_delete_sql_predicate in (None, '') or not isinstance(self.soft_delete_sql_predicate, str):
                raise InvalidConfigurationException(
                    'Schema {} has an invalid SOFT_DELETE configuration. '
                    'SOFT_DELETE must be a single dict with the column name to look for and '
                    'the SQL needed to exclude the soft deleted rows. '.format(self.schema_name)
                )

        if self.exclusion_list and self.inclusion_list:
            raise InvalidConfigurationException(
                'Schema {} has both INCLUDE and EXCLUDE sections in its'
                'sections in its configuration file'.format(self.schema_name)
            )

    @classmethod
    def from_config(cls, schema_name, config):
        """
        Construct a Schema object from a config dictionary. This is encapuslated
        into it's own function to help declutter the `run` function in
        builder.py.
        TODO: add code to parse out un-managed table configs here
        """
        exclusion_list = []
        inclusion_list = []
        soft_delete_column_name = None
        soft_delete_sql_predicate = None

        if config:
            exclusion_list = config.get('EXCLUDE', [])
            inclusion_list = config.get('INCLUDE', [])

            if 'SOFT_DELETE' in config:
                for k, v in config['SOFT_DELETE'].items():
                    soft_delete_column_name = k
                    soft_delete_sql_predicate = v

        schema = Schema(
            schema_name,
            exclusion_list,
            inclusion_list,
            soft_delete_column_name,
            soft_delete_sql_predicate,
            relations=[]
        )
        return schema

    def filter_relations(self):
        """
        Filter the relations in this Schema, based upon the exclusion and
        inclusion lists.
        """
        filtered_relations = []
        for relation in self.relations:

            if self.exclusion_list and not self.inclusion_list:
                if relation.source_relation_name not in self.exclusion_list:
                    filtered_relations.append(relation)
            elif not self.exclusion_list and self.inclusion_list:
                if relation.source_relation_name in self.inclusion_list:
                    filtered_relations.append(relation)
            elif not self.exclusion_list and not self.inclusion_list:
                filtered_relations.append(relation)
            else:
                raise InvalidConfigurationException(
                    "This schema has both an INCLUDE and EXCLUDE list."
                )
        return filtered_relations

    def soft_delete_sql_clause(self):
        """
        Return the SQL to exclude soft deleted rows based on configuration.
        """
        if self.soft_delete_column_name is None:
            return ""

        return "{} {}".format(self.soft_delete_column_name, self.soft_delete_sql_predicate)


class InvalidConfigurationException(Exception):
    pass
