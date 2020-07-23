"""
Class and helpers for dealing with raw schemas
"""


class Schema():
    """
    Class to represent a raw Schema used to back an application schema
    """

    def __init__(self, schema_name, exclusion_list, inclusion_list, soft_delete_column_name,
                 soft_delete_column_value, relations=None
    ):
        self.schema_name = schema_name
        self.exclusion_list = exclusion_list
        self.inclusion_list = inclusion_list
        self.soft_delete_column_name = soft_delete_column_name
        self.soft_delete_column_value = soft_delete_column_value
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
            if config.get('SOFT_DELETE'):
                for k, v in config['SOFT_DELETE'].items():
                    soft_delete_column_name = k
                    soft_delete_column_value = v
            else:
                soft_delete_column_name = None
                soft_delete_column_value = None
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
            soft_delete_column_name,
            soft_delete_column_value,
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
        """
        if self.soft_delete_column_value is None:
            return "{} IS NULL".format(self.soft_delete_column_name)
        elif isinstance(self.soft_delete_column_value, bool):
            if self.soft_delete_column_value:
                return "{}".format(self.soft_delete_column_name)
            else:
                return "NOT {}".format(self.soft_delete_column_name)
        elif isinstance(self.soft_delete_column_value, str):
            return "{} = '{}'".format(
                self.soft_delete_column_name, self.soft_delete_column_value
            )


class InvalidConfigurationException(Exception):
    pass



def convert_to_sql(value):
    """
    """
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        if value.lower() in ['null', 'none']:
            return None
        elif value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        else:
            return value
    else:
        raise InvalidConfigurationException()
