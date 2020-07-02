"""
Class and helpers for dealing with raw schemas
"""


class Schema():
    """
    Class to represent a raw Schema used to back an application schema
    """

    def __init__(self, schema_name, exclusion_list, inclusion_list, relations=None):
        self.schema_name = schema_name
        self.exclusion_list = exclusion_list
        self.inclusion_list = inclusion_list
        self.relations = relations

    def __repr__(self):
        return self.schema_name

    @classmethod
    def from_config(cls, schema_name, config):
        """
        Construct a Schema object from a config dictionary. This is encapuslated
        into it's own function to help declutter the `run` function in
        builder.py.
        TODO: add code to parse out soft delete and unmanaged table configs here
        """
        if config:
            exclusion_list = config['EXCLUDE'] if config.get('EXCLUDE') else []
            inclusion_list = config['INCLUDE'] if config.get('INCLUDE') else []
        else:
            exclusion_list = []
            inclusion_list = []

        if exclusion_list and inclusion_list:
            raise InvalidConfigurationException(
                'Schema {} has both INCLUDE and EXCLUDE sections in its'
                'sections in its configuration file'.format(schema_name)
            )

        schema = Schema(
            schema_name,
            exclusion_list,
            inclusion_list,
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


class InvalidConfigurationException(Exception):
    pass
