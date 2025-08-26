import os
import json


class GraphQLQueries:

    def __init__(self):
        self._query_cache = {}

    def get_query(self, name):
        if name not in self._query_cache:
            self._query_cache[name] = GraphQLQuery(name)

        return self._query_cache[name]


class GraphQLQuery:

    def __init__(self, name, extension='.graphql'):
        dirname = os.path.dirname(__file__)
        with open(os.path.join(dirname, 'graphql_queries', name) + extension) as f:
            self._query = json.loads(f.read())

    def get_query(self):
        return self._query

    def get_query_with_inputs(self, input, drop_keys=['test_name']):
        query = dict(self.get_query())
        new_dict = dict(self._get_default_input())
        new_dict.update(input)
        for drop_key in drop_keys:
            del new_dict[drop_key]
        query["variables"]["input"] = new_dict
        return query

    def get_query_with_variables(self, variables, drop_keys=['test_name']):
        query = dict(self.get_query())
        new_dict = dict(self._get_default_variables())
        new_dict.update(variables)
        for drop_key in drop_keys:
            del new_dict[drop_key]
        query["variables"] = new_dict
        return query

    def _get_default_input(self):
        return self._get_default_variables()["input"]

    def _get_default_variables(self):
        return self._query["variables"]


if __name__ == "__main__":
    print(GraphQLQuery("searchAcrossEntities").get_query_with_inputs({"query": "test query"}))
